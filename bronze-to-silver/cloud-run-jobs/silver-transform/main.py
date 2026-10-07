"""Entry point of the ``silver-transform`` Cloud Run Job.

Env contract (set by the orchestrator, see docs/CONVENTIONS.md):

    TRANSFORM       module name under transforms/ (required)
    GCP_PROJECT     project id (required)
    BRONZE_DATASET  bronze dataset (default elementl_bronze)
    SILVER_DATASET  silver dataset (default elementl_silver)
    OUTPUT_TABLE    project.dataset.table (default <project>.<silver>.<transform DEFAULT_TABLE>)
    EXECUTION_DATE  YYYY-MM-DD logical date (default today UTC)
    RUN_ID          Airflow run_id / job id (default cloud-run execution name)
    PARAMS          JSON object string (default {})

On success a single-line JSON summary
``{"transform", "output_table", "rows_written", "duration_s", "status"}`` is logged (the
orchestrator greps for ``SILVER_SUMMARY``). Any failure exits non-zero.
"""
from __future__ import annotations

import datetime as dt
import importlib
import json
import logging
import os
import re
import sys
import time
import traceback

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                    stream=sys.stdout)
log = logging.getLogger("silver_transform")


def _env(name: str, default: str | None = None, required: bool = False) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        if required and default is None:
            raise SystemExit(f"missing required env var {name}")
        return default or ""
    return v


def build_context():
    from google.cloud import bigquery

    from common import Context

    transform = _env("TRANSFORM", required=True)
    if not re.match(r"^[a-z][a-z0-9_]*$", transform):
        raise SystemExit(f"invalid TRANSFORM name {transform!r}")
    project = _env("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT"), required=True)
    bronze = _env("BRONZE_DATASET", "elementl_bronze")
    silver = _env("SILVER_DATASET", "elementl_silver")
    ds = _env("EXECUTION_DATE", dt.datetime.now(dt.timezone.utc).date().isoformat())
    dt.date.fromisoformat(ds)  # validate
    run_id = _env("RUN_ID", os.environ.get("CLOUD_RUN_EXECUTION", f"manual-{int(time.time())}"))
    params_raw = _env("PARAMS", "{}")
    try:
        params = json.loads(params_raw) if params_raw.strip() else {}
    except json.JSONDecodeError as e:
        raise SystemExit(f"PARAMS is not valid JSON: {e}: {params_raw!r}")
    if not isinstance(params, dict):
        raise SystemExit("PARAMS must be a JSON object")

    module = importlib.import_module(f"transforms.{transform}")
    default_table = getattr(module, "DEFAULT_TABLE", transform)
    output_table = _env("OUTPUT_TABLE", f"{project}.{silver}.{default_table}")
    if output_table.count(".") != 2:
        raise SystemExit(f"OUTPUT_TABLE must be project.dataset.table, got {output_table!r}")

    client = bigquery.Client(project=project)
    ctx = Context(project=project, bronze_dataset=bronze, silver_dataset=silver, output_table=output_table,
                  ds=ds, run_id=run_id, params=params, client=client, transform=transform)
    return module, ctx


def main() -> int:
    started = time.time()
    summary = {"transform": os.environ.get("TRANSFORM"), "output_table": os.environ.get("OUTPUT_TABLE"),
               "rows_written": None, "duration_s": None, "status": "failed"}
    try:
        module, ctx = build_context()
        summary["output_table"] = ctx.output_table
        log.info("starting transform=%s output=%s ds=%s run_id=%s params=%s bronze=%s silver=%s",
                 ctx.transform, ctx.output_table, ctx.ds, ctx.run_id, ctx.params, ctx.bronze_dataset, ctx.silver_dataset)
        rows = module.run(ctx)
        summary.update(rows_written=int(rows or 0), status="success")
        return 0
    except SystemExit as e:
        summary["error"] = str(e)
        log.error("configuration error: %s", e)
        return 2
    except Exception as e:  # noqa: BLE001
        summary["error"] = f"{type(e).__name__}: {e}"
        log.error("transform failed: %s\n%s", e, traceback.format_exc())
        return 1
    finally:
        summary["duration_s"] = round(time.time() - started, 1)
        log.info("SILVER_SUMMARY %s", json.dumps(summary, default=str))
        print("SILVER_SUMMARY " + json.dumps(summary, default=str), flush=True)


if __name__ == "__main__":
    sys.exit(main())

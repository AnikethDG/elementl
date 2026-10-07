#!/usr/bin/env python3
"""Render a bronze-to-silver BigQuery SQL script by substituting the engine contract placeholders.

The same substitution logic is used by the Composer factory (udp_silver_engines.render_sql) so a
script that renders here renders identically in Airflow.  Placeholders (see docs/CONVENTIONS.md):

    ${project} ${bronze_dataset} ${silver_dataset} ${ds} ${run_id} ${output} ${params.<key>}

Usage (print rendered SQL to stdout):

    python3 bigquery/render.py bigquery/sql/stg_netsuite_customer.sql \
        --project ani-labs --bronze-dataset elementl_bronze --silver-dataset elementl_silver \
        --output ani-labs.elementl_silver.stg_netsuite_customer --ds 2026-10-07 --run-id manual \
        --param lookback_days=2

Pipe into `bq query --use_legacy_sql=false --location=us-central1` to execute.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_PLACEHOLDER = re.compile(r"\$\{([a-zA-Z_][a-zA-Z0-9_.]*)\}")


def render_sql(template: str, context: dict, params: dict | None = None) -> str:
    """Substitute `${name}` and `${params.key}` placeholders.

    Raises KeyError listing every unresolved placeholder so a typo in YAML fails before BigQuery.
    """
    params = params or {}
    missing: list[str] = []

    def _sub(match: re.Match) -> str:
        name = match.group(1)
        if name.startswith("params."):
            key = name[len("params."):]
            if key in params:
                return str(params[key])
        elif name in context:
            return str(context[name])
        missing.append(name)
        return match.group(0)

    rendered = _PLACEHOLDER.sub(_sub, template)
    if missing:
        raise KeyError(f"unresolved placeholders: {sorted(set(missing))}")
    return rendered


def _parse_param(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(f"--param expects key=value, got {raw!r}")
    key, value = raw.split("=", 1)
    return key.strip(), value.strip()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sql_file", type=Path)
    ap.add_argument("--project", required=True)
    ap.add_argument("--bronze-dataset", required=True)
    ap.add_argument("--silver-dataset", required=True)
    ap.add_argument("--output", required=True, help="fully qualified output table project.dataset.table")
    ap.add_argument("--ds", required=True, help="logical date YYYY-MM-DD")
    ap.add_argument("--run-id", default="manual")
    ap.add_argument("--param", action="append", default=[], type=_parse_param, metavar="key=value")
    args = ap.parse_args(argv)

    context = {
        "project": args.project,
        "bronze_dataset": args.bronze_dataset,
        "silver_dataset": args.silver_dataset,
        "output": args.output,
        "ds": args.ds,
        "run_id": args.run_id,
    }
    sys.stdout.write(render_sql(args.sql_file.read_text(), context, dict(args.param)))
    return 0


if __name__ == "__main__":
    sys.exit(main())

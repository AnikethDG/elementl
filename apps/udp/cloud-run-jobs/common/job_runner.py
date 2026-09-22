# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Shared entrypoint logic for the Elementl Cloud Run ingestion jobs.

A Cloud Run Job cannot return a response to its caller, so each execution
publishes its outcome in two places:

  1. A result JSON object in GCS at ``_job_results/<task_id>/<run_id>.json``,
     which the Airflow DAG reads back to populate XCom.
  2. A row in the BigQuery ingestion audit table.

Both successes and failures are recorded, which is why the audit row is written
from inside the job rather than from a downstream Airflow task that would be
skipped on failure.
"""

import json
import logging
import datetime
import traceback
from typing import Any, Dict

from google.cloud import storage

from .base_extractor import BaseExtractor
from .job_config import JobConfig

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("job_runner")

RESULT_PREFIX = "_job_results"


def _write_result_to_gcs(config: JobConfig, result: Dict[str, Any]) -> str:
    """Publishes the execution result so the DAG can read it back."""
    client = storage.Client()
    run_id = (config.run_id or "manual").replace("/", "_").replace(":", "_")
    blob_name = f"{RESULT_PREFIX}/{config.task_id}/{run_id}.json"
    bucket = client.bucket(config.gcs_bucket)
    bucket.blob(blob_name).upload_from_string(
        json.dumps(result, indent=2, default=str), content_type="application/json"
    )
    uri = f"gs://{config.gcs_bucket}/{blob_name}"
    logger.info("Wrote job result to %s", uri)
    return uri


def _write_audit_row(
    config: JobConfig, result: Dict[str, Any], started: datetime.datetime
) -> None:
    """Appends a row to the BigQuery ingestion audit table.

    The deployed audit table carries six REQUIRED columns from an earlier
    revision of the framework (task_id, execution_id, source_type, status,
    execution_date, log_timestamp). Those must be populated or the insert is
    rejected, so this writes the union of the legacy columns and the newer
    framework columns.
    """
    if not config.audit_enabled:
        logger.info("Audit table not configured; skipping audit row.")
        return

    from google.cloud import bigquery

    now = datetime.datetime.now(datetime.timezone.utc)
    execution_date = config.execution_date or now.date().isoformat()
    run_id = config.run_id or "manual"

    client = bigquery.Client(project=config.audit_project)
    row = {
        # Legacy REQUIRED columns.
        "task_id": config.task_id,
        "execution_id": run_id,
        "source_type": config.source_type,
        "status": result.get("status", "FAILED"),
        "execution_date": execution_date,
        "log_timestamp": now.isoformat(),
        # Legacy optional columns.
        "records_extracted": result.get("records_ingested", 0),
        "bytes_processed": result.get("bytes_written", 0),
        "start_time": started.isoformat(),
        "end_time": now.isoformat(),
        "duration_seconds": result.get("duration_seconds"),
        "error_message": result.get("error"),
        # Framework columns.
        "dag_id": config.dag_id or f"dag_ingest_{config.task_id}",
        "run_id": run_id,
        "engine_type": "cloud_run_job",
        "target_dataset": config.target_dataset,
        "target_table": config.target_table,
        "gcs_output_path": f"gs://{config.gcs_bucket}/{config.gcs_prefix}",
        "bytes_written": result.get("bytes_written", 0),
        "created_at": now.isoformat(),
    }
    errors = client.insert_rows_json(config.audit_full_table, [row])
    if errors:
        # Surfaced loudly: a silent audit failure is what hid the previous
        # generation of bugs.
        logger.error("Audit insert returned errors: %s", errors)
    else:
        logger.info("Recorded audit row in %s", config.audit_full_table)


def run_job(extractor: BaseExtractor) -> int:
    """Runs one extraction. Returns a process exit code."""
    started = datetime.datetime.now(datetime.timezone.utc)
    config = None
    result: Dict[str, Any]

    try:
        config = JobConfig.from_env()
        logger.info("Starting job with config: %s", json.dumps(config.redacted(), default=str))

        records, bytes_written, gcs_files = extractor.extract(config)

        result = {
            "status": "SUCCESS",
            "task_id": config.task_id,
            "source_type": config.source_type,
            "records_ingested": records,
            "bytes_written": bytes_written,
            "gcs_files": gcs_files,
            "duration_seconds": round(
                (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds(), 2
            ),
        }
        logger.info("Extraction succeeded: %s", result)

    except Exception as exc:  # noqa: BLE001 - top-level job boundary
        logger.error("Extraction failed: %s\n%s", exc, traceback.format_exc())
        result = {
            "status": "FAILED",
            "task_id": getattr(config, "task_id", "unknown_task"),
            "source_type": getattr(config, "source_type", ""),
            "records_ingested": 0,
            "bytes_written": 0,
            "gcs_files": [],
            "error": str(exc),
            "duration_seconds": round(
                (datetime.datetime.now(datetime.timezone.utc) - started).total_seconds(), 2
            ),
        }

    # Best-effort publication of the outcome; never mask the original result.
    if config is not None:
        try:
            _write_result_to_gcs(config, result)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish result: %s", exc)
        try:
            _write_audit_row(config, result, started)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to publish audit row: %s", exc)

    return 0 if result.get("status") == "SUCCESS" else 1

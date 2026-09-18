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
Job configuration for Elementl Cloud Run ingestion jobs.

Cloud Run Jobs are run-to-completion workloads: unlike an HTTP function they
cannot receive a request body and cannot return a response. All per-execution
configuration therefore arrives as environment variables, supplied by the
Airflow ``CloudRunExecuteJobOperator`` via container overrides.
"""

import os
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger("job_config")


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Env %s=%r is not an int; using default %s", name, raw, default)
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = _env(name).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


@dataclass
class JobConfig:
    """Everything a job execution needs, resolved from the environment."""

    task_id: str
    source_type: str

    connection_secret_id: str = ""
    load_type: str = "full_refresh"
    watermark_column: str = ""
    watermark_value: str = ""
    execution_date: str = ""
    run_id: str = ""
    dag_id: str = ""

    # Source-specific
    query: str = ""
    endpoint: str = ""
    data_category: str = ""
    query_params: Dict[str, Any] = field(default_factory=dict)
    page_size: int = 1000
    fetch_size: int = 5000
    timeout_seconds: int = 300

    # Destination
    gcs_bucket: str = ""
    gcs_prefix: str = ""
    destination_format: str = "parquet"

    # Audit
    audit_project: str = ""
    audit_dataset: str = ""
    audit_table: str = ""
    target_dataset: str = ""
    target_table: str = ""

    @classmethod
    def from_env(cls) -> "JobConfig":
        raw_params = _env("QUERY_PARAMS")
        try:
            query_params = json.loads(raw_params) if raw_params else {}
        except json.JSONDecodeError:
            logger.warning("QUERY_PARAMS is not valid JSON; ignoring: %r", raw_params)
            query_params = {}

        cfg = cls(
            task_id=_env("TASK_ID", "unknown_task"),
            source_type=_env("SOURCE_TYPE").lower(),
            connection_secret_id=_env("CONNECTION_SECRET_ID"),
            load_type=_env("LOAD_TYPE", "full_refresh").lower(),
            watermark_column=_env("WATERMARK_COLUMN"),
            watermark_value=_env("WATERMARK_VALUE"),
            execution_date=_env("EXECUTION_DATE"),
            run_id=_env("RUN_ID"),
            dag_id=_env("DAG_ID"),
            query=_env("QUERY"),
            endpoint=_env("ENDPOINT"),
            data_category=_env("DATA_CATEGORY"),
            query_params=query_params,
            page_size=_env_int("PAGE_SIZE", 1000),
            fetch_size=_env_int("FETCH_SIZE", 5000),
            timeout_seconds=_env_int("TIMEOUT_SECONDS", 300),
            gcs_bucket=_env("GCS_BUCKET"),
            gcs_prefix=_env("GCS_PREFIX"),
            destination_format=_env("DESTINATION_FORMAT", "parquet").lower(),
            audit_project=_env("AUDIT_PROJECT"),
            audit_dataset=_env("AUDIT_DATASET"),
            audit_table=_env("AUDIT_TABLE"),
            target_dataset=_env("TARGET_DATASET"),
            target_table=_env("TARGET_TABLE"),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        """Fail fast and loudly rather than producing a half-configured run.

        This is deliberately strict: the previous HTTP-function design silently
        shipped unrendered Jinja placeholders, so we also reject any value that
        still looks like an unrendered template.
        """
        missing = [
            name
            for name in ("task_id", "source_type", "gcs_bucket", "gcs_prefix")
            if not getattr(self, name)
        ]
        if missing:
            raise ValueError(f"Missing required environment config: {sorted(missing)}")

        unrendered = [
            name
            for name, value in self.__dict__.items()
            if isinstance(value, str) and ("{{" in value or "{%" in value)
        ]
        if unrendered:
            raise ValueError(
                "Unrendered Jinja template detected in config fields "
                f"{sorted(unrendered)} - the DAG did not render its overrides."
            )

    @property
    def audit_enabled(self) -> bool:
        return bool(self.audit_project and self.audit_dataset and self.audit_table)

    @property
    def audit_full_table(self) -> str:
        return f"{self.audit_project}.{self.audit_dataset}.{self.audit_table}"

    def redacted(self) -> Dict[str, Any]:
        """Loggable view of the config."""
        out = dict(self.__dict__)
        if out.get("query"):
            out["query"] = out["query"][:200] + ("..." if len(out["query"]) > 200 else "")
        return out

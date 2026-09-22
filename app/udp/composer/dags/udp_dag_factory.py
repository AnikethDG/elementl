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

"""Dynamic Airflow DAG Factory for Elementl UDP.

Generates 1 DAG per declarative YAML config across NetSuite (11 tables),
P6 (15 reporting tables), and Atlas (4 GIS/tabular layers).
Configurations can be sourced from:
  1. GCS location: gs://bkt-elementl-509009-udp-configs/sources/
  2. Local directory: configs/sources/

Each DAG executes:
  1. ingestion_group:
     - Cloud Run Ingestion Job execution with automated auth & telemetry
     - GCS-to-BigQuery Bronze table verification & audit logging
  2. dataform_group:
     - Dataform Compilation & Workflow Invocation for Silver Transformations
"""

import os
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import yaml

try:
    from airflow import DAG
    from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
    from airflow.providers.google.cloud.operators.dataform import (
        DataformCreateCompilationResultOperator,
        DataformCreateWorkflowInvocationOperator,
    )
    from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
    from airflow.utils.task_group import TaskGroup
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Graceful fallback for environments validating DAG factory outside Airflow container
    AIRFLOW_AVAILABLE = False
    DAG = object
    TaskGroup = object

logger = logging.getLogger("udp_dag_factory")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT") or "elementl-509009"
REGION = os.environ.get("GCP_REGION", "us-central1")
RAW_BUCKET = os.environ.get("RAW_BUCKET_NAME", f"bkt-{PROJECT_ID}-udp-bronze-raw")
GCS_CONFIG_BUCKET = os.environ.get("GCS_CONFIG_BUCKET", f"bkt-{PROJECT_ID}-udp-configs")
GCS_CONFIG_PREFIX = os.environ.get("GCS_CONFIG_PREFIX", "sources/")
DATAFORM_REPO = os.environ.get("DATAFORM_REPOSITORY", "gcp-dataform-transformations")
DATAFORM_SA = os.environ.get(
    "DATAFORM_SERVICE_ACCOUNT",
    f"sa-data-transform@{PROJECT_ID}.iam.gserviceaccount.com",
)


def _normalize_config(data: dict) -> dict:
    """Normalizes configuration dictionary so both structured and legacy keys are present."""
    if not isinstance(data, dict):
        return data

    source_cfg = data.get("source", {})
    engine_cfg = data.get("engine", {})
    dest_cfg = data.get("destination", {})
    orch_cfg = data.get("orchestration", {})

    source_type = source_cfg.get("type") or data.get("source_system", "unknown")
    data["source_system"] = source_type
    data["ingestion_job"] = (
        engine_cfg.get("job_name")
        or data.get("ingestion_job")
        or f"{source_type}-ingestion"
    )
    data["target_dataset"] = (
        dest_cfg.get("bq_dataset")
        or data.get("target_dataset")
        or f"ds_bronze_{source_type}"
    )
    data["target_table"] = (
        dest_cfg.get("bq_table")
        or data.get("target_table")
        or f"{source_type}_{data.get('task_id', '')}"
    )
    data["source_table"] = str(
        source_cfg.get("source_table")
        or data.get("source_table")
        or source_cfg.get("connection", {}).get("layer_id")
        or data.get("layer_id", "")
    )
    data["where_clause"] = str(
        source_cfg.get("where_clause")
        or data.get("where_clause", "1=1")
    )
    data["max_records"] = str(
        source_cfg.get("max_records")
        or data.get("max_records", 10)
    )
    data["schedule"] = (
        orch_cfg.get("schedule")
        or orch_cfg.get("schedule_interval")
        or data.get("schedule", "@daily")
    )
    data["dataform_tags"] = (
        orch_cfg.get("dataform", {}).get("included_tags")
        or data.get("dataform_tags", [data.get("task_id", "")])
    )

    raw_format = str(
        dest_cfg.get("format")
        or data.get("destination_format")
        or "PARQUET"
    ).strip()
    norm_upper = raw_format.upper().replace(" ", "_")
    if norm_upper in ("SAME_AS_ORIGIN", "ORIGIN"):
        if source_type.lower() != "atlas":
            raise ValueError(
                f"Destination format 'SAME_AS_ORIGIN' is only allowed for Atlas sources, "
                f"but found on {source_type} task '{data.get('task_id')}'. "
                f"For {source_type}, please use PARQUET, CSV, or JSON."
            )
        dest_format = "SAME_AS_ORIGIN"
    else:
        dest_format = raw_format.upper()
    data["destination_format"] = dest_format
    if "format" in dest_cfg:
        dest_cfg["format"] = dest_format
    return data


def create_udp_dag(cfg: dict) -> DAG:
    """Creates an Airflow DAG for a specific ingestion YAML specification."""
    if not AIRFLOW_AVAILABLE:
        return None

    cfg = _normalize_config(cfg)
    task_id = cfg["task_id"]
    dag_id = f"dag_udp_{task_id}"
    orch_cfg = cfg.get("orchestration", {})
    source_cfg = cfg.get("source", {})
    dest_cfg = cfg.get("destination", {})

    default_args = {
        "owner": "elementl-udp",
        "depends_on_past": False,
        "retries": int(orch_cfg.get("retries", 1)),
        "retry_delay": timedelta(minutes=int(orch_cfg.get("retry_delay_minutes", 5))),
    }

    # By default, do not automatically schedule/backfill all 30 DAGs simultaneously
    # to avoid worker CPU/memory starvation. Individual DAGs are triggered on-demand
    # or scheduled when ENABLE_DAG_SCHEDULE=true is explicitly set.
    enable_schedule = os.environ.get("ENABLE_DAG_SCHEDULE", "false").lower() in ("true", "1")
    dag_schedule = cfg.get("schedule", "@daily") if enable_schedule else None

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=cfg.get("description") or f"UDP E2E Pipeline ({cfg['source_system'].upper()} -> Bronze -> Silver): {task_id}",
        schedule_interval=dag_schedule,
        start_date=datetime(2026, 9, 1),
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        tags=["udp", cfg["source_system"], task_id],
    )

    with dag:
        with TaskGroup(group_id="ingestion_group", tooltip="Cloud Run Job Extraction + Bronze BQ Load") as ingestion_group:
            job_name = cfg.get("ingestion_job", f"{cfg['source_system']}-ingestion")
            target_dataset = cfg.get("target_dataset", f"ds_bronze_{cfg['source_system']}")
            target_table = cfg.get("target_table", f"{cfg['source_system']}_{cfg['task_id']}")
            source_table = str(cfg.get("source_table", ""))
            where_clause = str(cfg.get("where_clause", "1=1"))
            max_records = str(cfg.get("max_records", 10))

            dest_format = dest_cfg.get("format") or cfg.get("destination_format", "PARQUET")
            gcs_bucket = dest_cfg.get("gcs_bucket") or RAW_BUCKET
            gcs_prefix = f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}"

            env_overrides = [
                {"name": "TASK_ID", "value": task_id},
                {"name": "SOURCE_TYPE", "value": cfg["source_system"]},
                {"name": "SOURCE_SYSTEM", "value": cfg["source_system"]},
                {"name": "SOURCE_TABLE", "value": source_table},
                {"name": "TARGET_DATASET", "value": target_dataset},
                {"name": "TARGET_TABLE", "value": target_table},
                {"name": "WHERE_CLAUSE", "value": where_clause},
                {"name": "MAX_RECORDS", "value": max_records},
                {"name": "GCS_BUCKET", "value": gcs_bucket},
                {"name": "GCS_PREFIX", "value": gcs_prefix},
                {"name": "EXECUTION_DATE", "value": "{{ ds }}"},
                {"name": "RUN_ID", "value": "{{ run_id }}"},
                {"name": "DAG_ID", "value": "{{ dag.dag_id }}"},
                {"name": "DESTINATION_FORMAT", "value": dest_format.lower()},
                {"name": "GCP_PROJECT", "value": PROJECT_ID},
                {"name": "GCP_PROJECT_ID", "value": PROJECT_ID},
                {"name": "GOOGLE_CLOUD_PROJECT", "value": PROJECT_ID},
                {"name": "AUDIT_PROJECT", "value": PROJECT_ID},
                {"name": "STREAM_BATCH_SIZE", "value": "50"},
                {"name": "TIMEOUT_SECONDS", "value": "60"},
                {"name": "ALLOW_MOCK_FALLBACK", "value": "true"},
            ]

            # Optional extra fields from source block
            query = source_cfg.get("query") or cfg.get("query")
            if query:
                env_overrides.append({"name": "QUERY", "value": str(query)})

            secret_id = source_cfg.get("connection_secret_id") or cfg.get("connection_secret_id")
            if secret_id:
                env_overrides.append({"name": "CONNECTION_SECRET_ID", "value": str(secret_id)})

            endpoint = source_cfg.get("connection", {}).get("endpoint_url") or cfg.get("endpoint_url")
            if endpoint:
                env_overrides.append({"name": "ENDPOINT", "value": str(endpoint)})

            layer_id = source_cfg.get("connection", {}).get("layer_id") or cfg.get("arcgis_layer_id") or cfg.get("layer_id")
            if layer_id is not None and str(layer_id).strip():
                env_overrides.append({"name": "LAYER_ID", "value": str(layer_id)})

            run_extraction = CloudRunExecuteJobOperator(
                task_id=f"execute_{job_name.replace('-', '_')}",
                project_id=PROJECT_ID,
                region=REGION,
                job_name=job_name,
                overrides={"container_overrides": [{"env": env_overrides}]},
                deferrable=False,
            )

            # Map destination format to BigQuery source format and GCS pattern
            norm_fmt = dest_format.strip().upper().replace(" ", "_")
            skip_rows = 0
            if norm_fmt in ("SAME_AS_ORIGIN", "ORIGIN"):
                if cfg["source_system"].lower() != "atlas":
                    raise ValueError(
                        f"Destination format 'SAME_AS_ORIGIN' is only allowed for Atlas sources, "
                        f"not allowed for {cfg['source_system']}."
                    )
                # Atlas writes native raw origin to GCS and strongly-typed Parquet for BigQuery ingestion
                # (BigQuery NEWLINE_DELIMITED_JSON rejects nested spatial coordinate arrays)
                bq_source_format = "PARQUET"
                source_patterns = [f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}/*.parquet"]
            elif norm_fmt == "CSV":
                bq_source_format = "CSV"
                source_patterns = [f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}/*.csv*"]
                skip_rows = 1
            elif norm_fmt in ("JSON", "JSONL", "NDJSON"):
                bq_source_format = "NEWLINE_DELIMITED_JSON"
                source_patterns = [f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}/*.json*"]
            else:
                bq_source_format = "PARQUET"
                source_patterns = [f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}/*.parquet"]

            write_disp = dest_cfg.get("write_disposition", "WRITE_TRUNCATE")

            load_kwargs = {
                "task_id": "load_gcs_to_bq_bronze",
                "bucket": gcs_bucket,
                "source_objects": source_patterns,
                "destination_project_dataset_table": f"{PROJECT_ID}.{target_dataset}.{target_table}",
                "source_format": bq_source_format,
                "write_disposition": write_disp,
                "autodetect": True,
                "ignore_unknown_values": True,
            }
            if skip_rows > 0:
                load_kwargs["skip_leading_rows"] = skip_rows

            load_gcs_to_bq_bronze = GCSToBigQueryOperator(**load_kwargs)

            run_extraction >> load_gcs_to_bq_bronze

        dataform_enabled = orch_cfg.get("dataform", {}).get("enabled", True) and os.environ.get("DATAFORM_ENABLED", "true").lower() == "true"
        if dataform_enabled:
            # Match specific tags or fallback to source_system tag (atlas, netsuite, p6)
            dataform_tags = cfg.get("dataform_tags") or [cfg["source_system"]]

            with TaskGroup(group_id="dataform_group", tooltip="Dataform Silver Transformation & Assertions") as dataform_group:
                compile_dataform = DataformCreateCompilationResultOperator(
                    task_id="compile_dataform",
                    project_id=PROJECT_ID,
                    region=REGION,
                    repository_id=DATAFORM_REPO,
                    compilation_result={"workspace": f"projects/{PROJECT_ID}/locations/{REGION}/repositories/{DATAFORM_REPO}/workspaces/dev"},
                )

                invoke_dataform = DataformCreateWorkflowInvocationOperator(
                    task_id="invoke_silver_and_assertions",
                    project_id=PROJECT_ID,
                    region=REGION,
                    repository_id=DATAFORM_REPO,
                    workflow_invocation={
                        "compilation_result": "{{ task_instance.xcom_pull('dataform_group.compile_dataform')['name'] }}",
                        "invocation_config": {
                            "included_tags": dataform_tags,
                            "transitive_dependencies_included": True,
                            "transitive_dependents_included": False,
                            "service_account": DATAFORM_SA,
                        },
                    },
                )

                compile_dataform >> invoke_dataform

            ingestion_group >> dataform_group

    return dag


def discover_all_configs() -> list:
    """Discovers configs from GCS bucket or local filesystem, filtering drafts/templates."""
    configs = []
    seen_tasks = set()

    def _should_include(name: str, data: dict) -> bool:
        filename = name.split("/")[-1]
        if filename.startswith("_"):
            return False
        if not data or "task_id" not in data:
            return False
        # Filter non-active configurations
        status = data.get("metadata_status", "Active")
        if status in ("Template", "Draft", "Inactive"):
            return False
        return True

    # 1. Check GCS bucket location: gs://bkt-elementl-509009-udp-configs/sources/
    if GCS_CONFIG_BUCKET:
        try:
            from google.cloud import storage
            client = storage.Client(project=PROJECT_ID)
            bucket = client.bucket(GCS_CONFIG_BUCKET)
            for blob in bucket.list_blobs(prefix=GCS_CONFIG_PREFIX):
                if blob.name.endswith(".yaml") or blob.name.endswith(".yml"):
                    if blob.name.split("/")[-1].startswith("_"):
                        continue
                    content = blob.download_as_text(encoding="utf-8")
                    data = yaml.safe_load(content)
                    if _should_include(blob.name, data) and data["task_id"] not in seen_tasks:
                        data = _normalize_config(data)
                        seen_tasks.add(data["task_id"])
                        configs.append(data)
                        logger.info("Discovered config from GCS: gs://%s/%s (task: %s)", GCS_CONFIG_BUCKET, blob.name, data["task_id"])
        except Exception as exc:
            logger.debug("GCS config discovery skipped: %s", exc)

    # 2. Local directory fallback
    local_candidates = [
        Path(__file__).resolve().parents[2] / "configs" / "sources",
        Path(__file__).parent / "configs" / "sources",
        Path("/home/airflow/gcs/dags/configs/sources"),
    ]
    for root in local_candidates:
        if root.exists():
            for yaml_file in sorted(root.rglob("*.yaml")):
                if yaml_file.name.startswith("_"):
                    continue
                try:
                    with open(yaml_file, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f)
                    if _should_include(yaml_file.name, data) and data["task_id"] not in seen_tasks:
                        data = _normalize_config(data)
                        seen_tasks.add(data["task_id"])
                        configs.append(data)
                except Exception as exc:
                    logger.warning("Error reading %s: %s", yaml_file, exc)

    return configs


# Instantiate all DAGs dynamically
for config_data in discover_all_configs():
    dag_instance = create_udp_dag(config_data)
    if dag_instance:
        globals()[f"dag_udp_{config_data['task_id']}"] = dag_instance

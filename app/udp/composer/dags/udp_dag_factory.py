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

"""Dynamic Airflow DAG Factory for Elementl UDP (Two-TaskGroup Architecture).

Generates 1 DAG per declarative YAML config across P6 (15 reporting tables),
NetSuite (11 tables), and Atlas (4 GIS/tabular layers).
Each DAG executes two TaskGroups:
  1. ingestion_group: CloudRunExecuteJobOperator + GCS-to-BigQuery Bronze native table load
  2. dataform_group: DataformCreateCompilationResultOperator + DataformCreateWorkflowInvocationOperator
"""

from datetime import datetime, timedelta
from pathlib import Path
import yaml

from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ID = "pid-nse-stg-core-apps-k8ti"
REGION = "us-central1"
RAW_BUCKET = f"bkt-{PROJECT_ID}-udp-bronze-raw"
DATAFORM_REPO = "gcp-dataform-transformations"


def create_udp_dag(cfg: dict) -> DAG:
    task_id = cfg["task_id"]
    dag_id = f"dag_udp_{task_id}"
    default_args = {
        "owner": "elementl-udp",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=f"UDP E2E Pipeline ({cfg['source_system'].upper()} -> Bronze -> Silver): {task_id}",
        schedule_interval=cfg.get("schedule", "@daily"),
        start_date=datetime(2026, 9, 1),
        catchup=False,
        tags=["udp", cfg["source_system"], task_id],
    )

    with dag:
        with TaskGroup(group_id="ingestion_group", tooltip="Cloud Run Job Extraction + Bronze BQ Load") as ingestion_group:
            run_extraction_job = CloudRunExecuteJobOperator(
                task_id=f"execute_{cfg['ingestion_job'].replace('-', '_')}",
                project_id=PROJECT_ID,
                region=REGION,
                job_name=cfg["ingestion_job"],
                overrides={
                    "container_overrides": [
                        {
                            "env": [
                                {"name": "TASK_ID", "value": task_id},
                                {"name": "SOURCE_TABLE", "value": str(cfg.get("source_table", cfg.get("layer_id", "")))},
                                {"name": "TARGET_DATASET", "value": cfg["target_dataset"]},
                                {"name": "TARGET_TABLE", "value": cfg["target_table"]},
                            ]
                        }
                    ]
                },
            )

            load_gcs_to_bq_bronze = GCSToBigQueryOperator(
                task_id="load_gcs_to_bq_bronze",
                bucket=RAW_BUCKET,
                source_objects=[f"{cfg['source_system']}/raw/{cfg['target_table']}/dt={{{{ ds }}}}/*.parquet"],
                destination_project_dataset_table=f"{PROJECT_ID}.{cfg['target_dataset']}.{cfg['target_table']}",
                source_format="PARQUET",
                write_disposition="WRITE_APPEND",
                autodetect=True,
            )

            run_extraction_job >> load_gcs_to_bq_bronze

        with TaskGroup(group_id="dataform_group", tooltip="Dataform Silver Transformation & Bronze-to-Silver Validation") as dataform_group:
            compile_dataform = DataformCreateCompilationResultOperator(
                task_id="compile_dataform",
                project_id=PROJECT_ID,
                region=REGION,
                repository_id=DATAFORM_REPO,
                compilation_result={
                    "git_commitish": "stage",
                },
            )

            invoke_dataform = DataformCreateWorkflowInvocationOperator(
                task_id="invoke_silver_and_assertions",
                project_id=PROJECT_ID,
                region=REGION,
                repository_id=DATAFORM_REPO,
                workflow_invocation={
                    "compilation_result": "{{ task_instance.xcom_pull('dataform_group.compile_dataform')['name'] }}",
                    "invocation_config": {
                        "included_tags": cfg.get("dataform_tags", [task_id]),
                        "transitive_dependencies_included": True,
                        "transitive_dependents_included": False,
                    },
                },
            )

            compile_dataform >> invoke_dataform

        ingestion_group >> dataform_group

    return dag


CONFIG_ROOT = Path(__file__).resolve().parents[2] / "configs" / "sources"
if CONFIG_ROOT.exists():
    for yaml_file in sorted(CONFIG_ROOT.rglob("*.yaml")):
        with open(yaml_file, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
        if config_data and "task_id" in config_data:
            globals()[f"dag_udp_{config_data['task_id']}"] = create_udp_dag(config_data)

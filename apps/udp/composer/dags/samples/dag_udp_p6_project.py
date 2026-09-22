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

"""Sample Ingestion DAG for Primavera P6 Project (Tested on Composer v3)."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ID = "elementl-509009"
REGION = "us-central1"
RAW_BUCKET = "bkt-elementl-509009-udp-bronze-raw"
DATAFORM_REPO = "gcp-dataform-transformations"
DATAFORM_SA = f"sa-data-transform@{PROJECT_ID}.iam.gserviceaccount.com"

default_args = {
    "owner": "udp_platform_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="dag_udp_p6_project",
    default_args=default_args,
    description="Primavera P6 Project Master Schedules (JDBC Ingestion)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["udp", "p6", "p6_project", "composer_v3"],
) as dag:

    with TaskGroup(group_id="ingestion_group", tooltip="Cloud Run Ingestion + Bronze BQ Load") as ingestion_group:
        execute_jdbc_ingestion = CloudRunExecuteJobOperator(
            task_id="execute_jdbc_ingestion",
            project_id=PROJECT_ID,
            region=REGION,
            job_name="jdbc-ingestion",
            overrides={
                "container_overrides": [
                    {
                        "env": [
                            {"name": "TASK_ID", "value": "p6_project"},
                            {"name": "SOURCE_TYPE", "value": "p6"},
                            {"name": "SOURCE_SYSTEM", "value": "p6"},
                            {"name": "SOURCE_TABLE", "value": "PROJECT"},
                            {"name": "TARGET_DATASET", "value": "ds_bronze_p6"},
                            {"name": "TARGET_TABLE", "value": "p6_project"},
                            {"name": "WHERE_CLAUSE", "value": "ROWNUM <= 20"},
                            {"name": "MAX_RECORDS", "value": "20"},
                            {"name": "GCS_BUCKET", "value": RAW_BUCKET},
                            {"name": "GCS_PREFIX", "value": "p6/raw/p6_project/dt={{ ds }}"},
                            {"name": "EXECUTION_DATE", "value": "{{ ds }}"},
                            {"name": "RUN_ID", "value": "{{ run_id }}"},
                            {"name": "DAG_ID", "value": "{{ dag.dag_id }}"},
                            {"name": "DESTINATION_FORMAT", "value": "parquet"},
                            {"name": "GCP_PROJECT", "value": PROJECT_ID},
                            {"name": "GCP_PROJECT_ID", "value": PROJECT_ID},
                            {"name": "GOOGLE_CLOUD_PROJECT", "value": PROJECT_ID},
                            {"name": "AUDIT_PROJECT", "value": PROJECT_ID},
                            {"name": "STREAM_BATCH_SIZE", "value": "50"},
                            {"name": "TIMEOUT_SECONDS", "value": "60"},
                            {"name": "ALLOW_MOCK_FALLBACK", "value": "true"},
                        ]
                    }
                ]
            },
            deferrable=False,
        )

        load_gcs_to_bq_bronze = GCSToBigQueryOperator(
            task_id="load_gcs_to_bq_bronze",
            bucket=RAW_BUCKET,
            source_objects=["p6/raw/p6_project/dt={{ ds }}/*.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.ds_bronze_p6.p6_project",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            ignore_unknown_values=True,
        )

        execute_jdbc_ingestion >> load_gcs_to_bq_bronze

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
                    "included_tags": ["p6"],
                    "transitive_dependencies_included": True,
                    "service_account": DATAFORM_SA,
                },
            },
        )

        compile_dataform >> invoke_dataform

    ingestion_group >> dataform_group

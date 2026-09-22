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

"""Sample Ingestion DAG for Atlas S1-02 Urban Areas (Tested on Composer v3)."""

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
    dag_id="dag_udp_atlas_s1_02_urban_areas",
    default_args=default_args,
    description="U.S. Census Bureau TIGERweb Delineated Urban Areas & Places (Criteria S1-02)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["udp", "atlas", "atlas_s1_02_urban_areas", "composer_v3"],
) as dag:

    with TaskGroup(group_id="ingestion_group", tooltip="Cloud Run Ingestion + Bronze BQ Load") as ingestion_group:
        execute_arcgis_ingestion = CloudRunExecuteJobOperator(
            task_id="execute_arcgis_ingestion",
            project_id=PROJECT_ID,
            region=REGION,
            job_name="arcgis-ingestion",
            overrides={
                "container_overrides": [
                    {
                        "env": [
                            {"name": "TASK_ID", "value": "atlas_s1_02_urban_areas"},
                            {"name": "SOURCE_TYPE", "value": "atlas"},
                            {"name": "SOURCE_SYSTEM", "value": "atlas"},
                            {"name": "SOURCE_TABLE", "value": ""},
                            {"name": "TARGET_DATASET", "value": "ds_bronze_atlas"},
                            {"name": "TARGET_TABLE", "value": "atlas_s1_02_urban_areas"},
                            {"name": "WHERE_CLAUSE", "value": "1=1"},
                            {"name": "MAX_RECORDS", "value": "10"},
                            {"name": "GCS_BUCKET", "value": RAW_BUCKET},
                            {"name": "GCS_PREFIX", "value": "atlas/raw/atlas_s1_02_urban_areas/dt={{ ds }}"},
                            {"name": "EXECUTION_DATE", "value": "{{ ds }}"},
                            {"name": "RUN_ID", "value": "{{ run_id }}"},
                            {"name": "DAG_ID", "value": "{{ dag.dag_id }}"},
                            {"name": "DESTINATION_FORMAT", "value": "geojson_ndjson"},
                            {"name": "GCP_PROJECT", "value": PROJECT_ID},
                            {"name": "GCP_PROJECT_ID", "value": PROJECT_ID},
                            {"name": "GOOGLE_CLOUD_PROJECT", "value": PROJECT_ID},
                            {"name": "AUDIT_PROJECT", "value": PROJECT_ID},
                            {"name": "STREAM_BATCH_SIZE", "value": "50"},
                            {"name": "TIMEOUT_SECONDS", "value": "60"},
                            {"name": "ALLOW_MOCK_FALLBACK", "value": "true"},
                            {"name": "ENDPOINT", "value": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Urban/MapServer"},
                            {"name": "LAYER_ID", "value": "0"},
                        ]
                    }
                ]
            },
            deferrable=False,
        )

        load_gcs_to_bq_bronze = GCSToBigQueryOperator(
            task_id="load_gcs_to_bq_bronze",
            bucket=RAW_BUCKET,
            source_objects=["atlas/raw/atlas_s1_02_urban_areas/dt={{ ds }}/*.parquet"],
            destination_project_dataset_table=f"{PROJECT_ID}.ds_bronze_atlas.atlas_s1_02_urban_areas",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            ignore_unknown_values=True,
        )

        execute_arcgis_ingestion >> load_gcs_to_bq_bronze

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
                    "included_tags": ["atlas"],
                    "transitive_dependencies_included": True,
                    "service_account": DATAFORM_SA,
                },
            },
        )

        compile_dataform >> invoke_dataform

    ingestion_group >> dataform_group

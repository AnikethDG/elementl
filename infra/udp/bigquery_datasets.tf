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

# Consolidated Project BigQuery Datasets (Bronze, Silver, Gold, Assertions & Operations)
locals {
  udp_datasets = {
    # Ingestion (Bronze Landing) Datasets
    ds_bronze_oracle_p6 = "Bronze raw landing dataset for Oracle Primavera P6 (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
    ds_bronze_netsuite  = "Bronze raw landing dataset for Oracle NetSuite (SuiteQL 11 tables)"
    ds_bronze_atlas     = "Bronze raw landing dataset for Atlas GIS & Tabular (envelope schema + layers)"
    # Transformation (Silver, Gold & Dataform Assertions) Datasets
    ds_silver_oracle_p6    = "Silver cleansed & standardized dataset for Oracle Primavera P6"
    ds_silver_netsuite     = "Silver cleansed & standardized dataset for Oracle NetSuite"
    ds_silver_atlas        = "Silver cleansed & standardized dataset for Atlas GIS & Tabular"
    ds_gold                = "Gold analytics & reporting dataset across UDP sources"
    ds_dataform_assertions = "Dataform data quality assertion results dataset"
    # Operational Metadata Dataset
    ds_operations = "Operational metadata dataset driving DAG Factory, watermarks, and ingestion execution logs"
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each                   = local.udp_datasets
  project                    = var.project_id
  dataset_id                 = each.key
  location                   = var.region
  description                = each.value
  delete_contents_on_destroy = false
}

# Ingestion Framework Operational Execution Logs Table in ds_operations
resource "google_bigquery_table" "ingestion_execution_logs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["ds_operations"].dataset_id
  table_id            = "ingestion_execution_logs"
  description         = "Elementl UDP Ingestion Framework Execution and Telemetry Audit Log"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "execution_date"
  }

  clustering = ["task_id", "source_type", "status"]

  schema = jsonencode([
    { name = "task_id", type = "STRING", mode = "REQUIRED", description = "Framework Task ID" },
    { name = "execution_id", type = "STRING", mode = "REQUIRED", description = "Execution / Run ID" },
    { name = "source_type", type = "STRING", mode = "REQUIRED", description = "Source system (netsuite, oracle_p6, atlas)" },
    { name = "status", type = "STRING", mode = "REQUIRED", description = "Execution status (SUCCESS, FAILED, RUNNING)" },
    { name = "execution_date", type = "DATE", mode = "REQUIRED", description = "Logical execution date (YYYY-MM-DD)" },
    { name = "log_timestamp", type = "TIMESTAMP", mode = "REQUIRED", description = "Timestamp when log entry was recorded" },
    { name = "records_extracted", type = "INT64", mode = "NULLABLE", description = "Number of records extracted from source" },
    { name = "records_loaded", type = "INT64", mode = "NULLABLE", description = "Number of records loaded into Bronze" },
    { name = "bytes_processed", type = "INT64", mode = "NULLABLE", description = "Bytes processed" },
    { name = "bytes_written", type = "INT64", mode = "NULLABLE", description = "Total bytes written to GCS" },
    { name = "duration_seconds", type = "FLOAT64", mode = "NULLABLE", description = "Job execution duration in seconds" },
    { name = "start_time", type = "TIMESTAMP", mode = "NULLABLE", description = "Job start timestamp" },
    { name = "end_time", type = "TIMESTAMP", mode = "NULLABLE", description = "Job end timestamp" },
    { name = "error_message", type = "STRING", mode = "NULLABLE", description = "Error message if failed" },
    { name = "dag_id", type = "STRING", mode = "NULLABLE", description = "Airflow DAG ID" },
    { name = "run_id", type = "STRING", mode = "NULLABLE", description = "Airflow DagRun ID" },
    { name = "engine_type", type = "STRING", mode = "NULLABLE", description = "Ingestion engine (cloud_run_job)" },
    { name = "target_dataset", type = "STRING", mode = "NULLABLE", description = "Target BigQuery Bronze dataset" },
    { name = "target_table", type = "STRING", mode = "NULLABLE", description = "Target BigQuery Bronze table" },
    { name = "gcs_output_path", type = "STRING", mode = "NULLABLE", description = "GCS landing path prefix" },
    { name = "created_at", type = "TIMESTAMP", mode = "NULLABLE", description = "Record creation timestamp" }
  ])
}

# Ingestion Framework Incremental Watermarks Table in ds_operations
resource "google_bigquery_table" "ingestion_watermarks" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["ds_operations"].dataset_id
  table_id            = "ingestion_watermarks"
  description         = "Elementl UDP Ingestion Framework Incremental Watermark State Store"
  deletion_protection = false

  clustering = ["task_id", "source_type"]

  schema = jsonencode([
    { name = "task_id", type = "STRING", mode = "REQUIRED", description = "Framework Task ID" },
    { name = "source_type", type = "STRING", mode = "REQUIRED", description = "Source system (netsuite, oracle_p6, atlas)" },
    { name = "watermark_column", type = "STRING", mode = "NULLABLE", description = "Column used for incremental watermarking" },
    { name = "last_watermark_value", type = "STRING", mode = "NULLABLE", description = "Latest ingested high-watermark value" },
    { name = "last_run_id", type = "STRING", mode = "NULLABLE", description = "DagRun ID of the last successful run" },
    { name = "last_updated_at", type = "TIMESTAMP", mode = "NULLABLE", description = "Timestamp of watermark update" }
  ])
}

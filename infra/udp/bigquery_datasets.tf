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

# Consolidated Project BigQuery Datasets & Bronze/Operational Tables (100% Terraform-Managed)
locals {
  udp_datasets = {
    # Standardized Naming Convention Datasets (Elementl UDP Naming Convention Doc + Per-Source Silver Split)
    ds_bronze_oracle_p6           = "Bronze raw landing dataset for Oracle Primavera P6 (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
    ds_bronze_netsuite     = "Bronze raw landing dataset for Oracle NetSuite (SuiteQL 11 tables)"
    ds_bronze_atlas        = "Bronze raw landing dataset for Atlas GIS & Tabular (envelope schema + layers)"
    ds_silver_oracle_p6           = "Silver 1:1 cleansed & standardized models for Oracle Primavera P6"
    ds_silver_netsuite     = "Silver 1:1 cleansed & standardized models for Oracle NetSuite"
    ds_silver_atlas        = "Silver 1:1 cleansed & standardized models for Atlas GIS & Tabular"
    ds_gold                = "Gold conformed & curated business data models and executive views"
    ds_dataform_assertions = "Dataform data quality assertions failed records (Bronze-to-Silver validation)"
    ds_operations          = "Operational metadata table driving DAG Factory, watermarks, and framework audit logs"
    ds_atlas_analytics     = "Exposed analytics views, site suitability parameters, and AI search tables for Project Atlas"
  }

  # Discover all 15 P6 Bronze schemas and 11 NetSuite Bronze schemas from apps/udp/configs/schema/
  p6_schema_files = {
    for f in fileset("${path.module}/../../apps/udp/configs/schema/oracle_p6", "oracle_p6_*_schema.json") :
    replace(f, "_schema.json", "") => "${path.module}/../../apps/udp/configs/schema/oracle_p6/${f}"
  }

  netsuite_schema_files = {
    for f in fileset("${path.module}/../../apps/udp/configs/schema/netsuite", "netsuite_*_schema.json") :
    replace(replace(f, "netsuite_", ""), "_schema.json", "") => "${path.module}/../../apps/udp/configs/schema/netsuite/${f}"
  }
}

resource "google_bigquery_dataset" "datasets" {
  depends_on                 = [google_project_service.udp_apis]
  for_each                   = local.udp_datasets
  project                    = var.project_id
  dataset_id                 = each.key
  location                   = var.region
  description                = each.value
  delete_contents_on_destroy = false
}

# Terraform-Managed Bronze Tables for Oracle Primavera P6 (15 tables in ds_bronze_oracle_p6)
resource "google_bigquery_table" "bronze_oracle_p6_tables" {
  for_each            = local.p6_schema_files
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["ds_bronze_oracle_p6"].dataset_id
  table_id            = each.key
  description         = "Bronze landing table for Oracle Primavera P6 ELEMENTL_PMDB_SBOX_PXRPTUSER.${upper(each.key)}"
  schema              = file(each.value)
  deletion_protection = false
}

# Terraform-Managed Bronze Tables for Oracle NetSuite (11 tables in ds_bronze_netsuite)
resource "google_bigquery_table" "bronze_netsuite_tables" {
  for_each            = local.netsuite_schema_files
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["ds_bronze_netsuite"].dataset_id
  table_id            = each.key
  description         = "Bronze landing table for Oracle NetSuite SuiteQL ${each.key}"
  schema              = file(each.value)
  deletion_protection = false
}

# Terraform-Managed Operational Audit Table (ds_operations.audit_ingestion_runs)
resource "google_bigquery_table" "audit_ingestion_runs" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.datasets["ds_operations"].dataset_id
  table_id            = "audit_ingestion_runs"
  description         = "UDP framework ingestion telemetry, execution status, and watermark tracking table"
  deletion_protection = false
  schema = jsonencode([
    { name = "run_id", type = "STRING", mode = "REQUIRED", description = "Unique execution run identifier" },
    { name = "source_system", type = "STRING", mode = "NULLABLE", description = "Source system (p6, netsuite, atlas)" },
    { name = "source_table", type = "STRING", mode = "NULLABLE", description = "Canonical table or layer identifier" },
    { name = "target_dataset", type = "STRING", mode = "NULLABLE", description = "Target Bronze BigQuery dataset" },
    { name = "target_table", type = "STRING", mode = "NULLABLE", description = "Target Bronze BigQuery table" },
    { name = "status", type = "STRING", mode = "NULLABLE", description = "Execution status (SUCCESS, FAILED, RUNNING)" },
    { name = "records_extracted", type = "INT64", mode = "NULLABLE", description = "Number of records extracted and loaded" },
    { name = "gcs_uri", type = "STRING", mode = "NULLABLE", description = "GCS landing staging URI" },
    { name = "started_at", type = "TIMESTAMP", mode = "NULLABLE", description = "Job start timestamp (UTC)" },
    { name = "completed_at", type = "TIMESTAMP", mode = "NULLABLE", description = "Job completion timestamp (UTC)" }
  ])
}

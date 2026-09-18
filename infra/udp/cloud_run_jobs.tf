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

# Instantiate the 4 Containerized Ingestion Cloud Run Jobs via Reusable Module
locals {
  ingestion_jobs = {
    "arcgis-ingestion" = {
      description = "Atlas ArcGIS MapServer / FeatureServer REST ingestion worker (S1-02, S1-06)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "ARCGIS"
    }
    "bulk-ingestion" = {
      description = "Atlas bulk shapefile/zip download, unpacking, and tabular API ingestion worker (S1-01, S2-20)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "BULK"
    }
    "suiteql-ingestion" = {
      description = "Oracle NetSuite SuiteAnalytics / SuiteQL REST API ingestion worker (11 tables)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "SUITEQL"
    }
    "jdbc-ingestion" = {
      description = "Oracle Primavera P6 TCPS 2484 JDBC/oracledb ingestion worker (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "JDBC_P6"
    }
  }
}

module "udp_cloud_run_jobs" {
  source   = "../modules/cloud-run-jobs"
  for_each = local.ingestion_jobs

  project_id      = var.project_id
  region          = var.region
  job_name        = each.key
  description     = each.value.description
  cpu             = each.value.cpu
  memory          = each.value.memory
  vpc_network     = var.vpc_network
  vpc_subnet      = var.vpc_subnet
  raw_bucket_name = google_storage_bucket.udp_bronze_raw.name

  env_vars = {
    GCP_PROJECT_ID  = var.project_id
    ENVIRONMENT     = var.environment
    RAW_BUCKET_NAME = google_storage_bucket.udp_bronze_raw.name
    ARCHIVE_BUCKET  = google_storage_bucket.udp_bronze_archive.name
    SOURCE_TYPE     = each.value.source_type
    P6_SCHEMA       = "ELEMENTL_PMDB_SBOX_PXRPTUSER"
  }
}

# Resource-Level Least-Privilege IAM (Compensating Control: No Project-Level Data Roles)
resource "google_secret_manager_secret_iam_member" "jdbc_p6_config_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets["secret-p6-db-config"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.udp_cloud_run_jobs["jdbc-ingestion"].service_account_email}"
}

resource "google_secret_manager_secret_iam_member" "jdbc_p6_ca_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets["secret-p6-db-ca-bundle"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.udp_cloud_run_jobs["jdbc-ingestion"].service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "jdbc_p6_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_p6"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${module.udp_cloud_run_jobs["jdbc-ingestion"].service_account_email}"
}

resource "google_secret_manager_secret_iam_member" "suiteql_netsuite_config_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets["secret-netsuite-config"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${module.udp_cloud_run_jobs["suiteql-ingestion"].service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "suiteql_netsuite_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_netsuite"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${module.udp_cloud_run_jobs["suiteql-ingestion"].service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "arcgis_atlas_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_atlas"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${module.udp_cloud_run_jobs["arcgis-ingestion"].service_account_email}"
}

resource "google_bigquery_dataset_iam_member" "bulk_atlas_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_atlas"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${module.udp_cloud_run_jobs["bulk-ingestion"].service_account_email}"
}


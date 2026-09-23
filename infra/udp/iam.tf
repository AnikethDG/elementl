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

# ==============================================================================
# Least-Privilege BigQuery IAM Bindings for Data Transform SA (gcp-sa-nsedusc1-data-transform)
# ==============================================================================

locals {
  dataform_bronze_read_datasets = toset([
    "ds_bronze_oracle_p6",
    "ds_bronze_netsuite",
    "ds_bronze_atlas",
  ])

  dataform_silver_gold_write_datasets = toset([
    "ds_silver_oracle_p6",
    "ds_silver_netsuite",
    "ds_silver_atlas",
    "ds_gold",
    "ds_dataform_assertions",
  ])
}

# 1. Project-level jobUser only (to execute query jobs, no project-level dataEditor role)
resource "google_project_iam_member" "dataform_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].email}"
}

# 2. Read-only access (roles/bigquery.dataViewer) on Bronze datasets
resource "google_bigquery_dataset_iam_member" "dataform_bronze_read_access" {
  for_each   = local.dataform_bronze_read_datasets
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].email}"
}

# 3. Read & Write access (roles/bigquery.dataEditor) on Silver, Gold, and Assertions datasets
resource "google_bigquery_dataset_iam_member" "dataform_silver_gold_write_access" {
  for_each   = local.dataform_silver_gold_write_datasets
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].email}"
}

# Resource-Level Least-Privilege IAM for Single Ingestion SA (gcp-sa-nsedusc1-data-ingest)
resource "google_storage_bucket_iam_member" "udp_ingest_sa_raw_bucket_access" {
  bucket = google_storage_bucket.udp_bronze_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

resource "google_storage_bucket_iam_member" "udp_ingest_sa_staging_bucket_access" {
  bucket = google_storage_bucket.udp_landing_staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

resource "google_storage_bucket_iam_member" "udp_ingest_sa_archive_bucket_access" {
  bucket = google_storage_bucket.udp_bronze_archive.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

resource "google_secret_manager_secret_iam_member" "udp_ingest_sa_secret_access" {
  for_each  = google_secret_manager_secret.udp_secrets
  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

resource "google_project_iam_member" "udp_ingest_sa_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

resource "google_bigquery_dataset_iam_member" "udp_ingest_sa_bronze_dataset_access" {
  for_each   = toset(["ds_bronze_oracle_p6", "ds_bronze_netsuite", "ds_bronze_atlas", "ds_operations"])
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}
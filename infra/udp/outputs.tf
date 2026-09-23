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

output "udp_bronze_raw_bucket" {
  description = "Bronze raw landing GCS bucket name"
  value       = google_storage_bucket.udp_bronze_raw.name
}

output "udp_bronze_archive_bucket" {
  description = "Bronze cold archive GCS bucket name"
  value       = google_storage_bucket.udp_bronze_archive.name
}

output "udp_landing_staging_bucket" {
  description = "Landing staging GCS bucket name"
  value       = google_storage_bucket.udp_landing_staging.name
}

output "udp_dataflow_temp_bucket" {
  description = "Dataflow temporary execution GCS bucket name"
  value       = google_storage_bucket.udp_dataflow_temp.name
}

output "udp_bigquery_datasets" {
  description = "Provisioned BigQuery ingestion dataset IDs"
  value       = [for k, v in google_bigquery_dataset.datasets : v.dataset_id]
}

output "udp_cloud_run_jobs" {
  description = "Provisioned Cloud Run v2 Ingestion Jobs"
  value       = { for k, j in google_cloud_run_v2_job.udp_ingestion_jobs : k => j.id }
}

output "udp_platform_service_accounts" {
  description = "Platform service accounts for UDP Ingestion, Transformation (Dataform), and Orchestration (Composer)"
  value       = { for k, sa in google_service_account.udp_platform_sas : k => sa.email }
}

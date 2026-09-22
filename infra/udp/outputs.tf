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

output "artifact_registry_repository_url" {
  description = "Docker Artifact Registry repository URL for the 4 Cloud Run Jobs in apps/udp/cloud-run-jobs/"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.udp_ingestion_repo.repository_id}"
}

output "cloud_run_job_names" {
  description = "Provisioned Cloud Run v2 Job names for UDP ingestion"
  value       = [for k, v in google_cloud_run_v2_job.udp_ingestion_jobs : v.name]
}

output "cloud_run_job_images" {
  description = "Expected Artifact Registry Docker image URIs for each Cloud Run Job"
  value = {
    for k, v in local.ingestion_jobs :
    k => "${local.artifact_registry_base_uri}/${k}:${var.image_tag}"
  }
}

output "bigquery_dataset_ids" {
  description = "Standardized BigQuery dataset IDs provisioned for UDP"
  value       = [for k, v in google_bigquery_dataset.datasets : v.dataset_id]
}

output "gcs_bucket_names" {
  description = "Provisioned GCS bucket names for UDP landing, staging, raw, archive, and Dataflow temp"
  value = {
    landing_staging = google_storage_bucket.udp_landing_staging.name
    dataflow_temp   = google_storage_bucket.udp_dataflow_temp.name
    bronze_raw      = google_storage_bucket.udp_bronze_raw.name
    bronze_archive  = google_storage_bucket.udp_bronze_archive.name
  }
}

output "dataform_repository_id" {
  description = "GCP Dataform repository name for Bronze -> Silver -> Gold transformations"
  value       = google_dataform_repository.udp_dataform_repo.name
}

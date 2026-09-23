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

# Dedicated Docker Artifact Registry for UDP Cloud Run Jobs (apps/udp/cloud-run-jobs/*)
# Stores the 5 container images built from apps/udp/cloud-run-jobs/:
#   1. arcgis-ingestion  (apps/udp/cloud-run-jobs/arcgis-ingestion/Dockerfile)
#   2. bulk-ingestion    (apps/udp/cloud-run-jobs/bulk-ingestion/Dockerfile)
#   3. jdbc-ingestion    (apps/udp/cloud-run-jobs/jdbc-ingestion/Dockerfile)
#   4. suiteql-ingestion (apps/udp/cloud-run-jobs/suiteQL-ingestion/Dockerfile)
#   5. spatial-data-unpacker  (apps/udp/cloud-run-jobs/spatial-data-unpacker/Dockerfile)

resource "google_artifact_registry_repository" "udp_ingestion_repo" {
  depends_on    = [google_project_service.udp_apis]
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repo_id
  description   = "Docker Artifact Registry for UDP Cloud Run ingestion jobs (arcgis-ingestion, bulk-ingestion, jdbc-ingestion, suiteql-ingestion)"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-minimum-versions"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }
}

# Repository-level IAM: Grant pull & push (writer) access to the UDP Ingestion Platform SA (gcp-sa-nsedusc1-data-ingest)
resource "google_artifact_registry_repository_iam_member" "udp_ingest_platform_sa_repo_writer" {
  project    = var.project_id
  location   = google_artifact_registry_repository.udp_ingestion_repo.location
  repository = google_artifact_registry_repository.udp_ingestion_repo.name
  role       = "roles/artifactregistry.writer"
  member     = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-ingest"].email}"
}

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

# Partitioned UDP Service Accounts (David Odza Security Compensating Controls)
# Enforces strict separation between Orchestration, Bronze Ingestion, and Silver/Gold Transformation.

resource "google_service_account" "sa_composer_orchestrator" {
  project      = var.project_id
  account_id   = "sa-composer-orchestrator"
  display_name = "UDP Cloud Composer 3 Orchestrator SA"
  description  = "Service account for Cloud Composer 3 orchestration and Cloud Run job invocation"
}

resource "google_service_account" "sa_bronze_ingest" {
  project      = var.project_id
  account_id   = "sa-bronze-ingest"
  display_name = "UDP Bronze Layer Ingestion SA"
  description  = "Service account restricted to GCS Bronze landing and BigQuery Bronze raw_* datasets"
}

resource "google_service_account" "sa_silver_transform" {
  project      = var.project_id
  account_id   = "sa-silver-transform"
  display_name = "UDP Silver & Gold Dataform Transformation SA"
  description  = "Service account with read access on Bronze and write access on Silver/Gold datasets"
}

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

# Resource-Level & Service-Account-Level IAM Bindings
# Note: Project-level data roles (e.g. roles/bigquery.admin) are prohibited by
# security compensating controls and omitted so tf-deploy runs cleanly without projectIamAdmin.

resource "google_service_account_iam_member" "composer_act_as_ingest" {
  service_account_id = google_service_account.sa_bronze_ingest.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_composer_orchestrator.email}"
}

resource "google_service_account_iam_member" "composer_act_as_transform" {
  service_account_id = google_service_account.sa_silver_transform.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.sa_composer_orchestrator.email}"
}

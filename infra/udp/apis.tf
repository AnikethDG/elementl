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

# Required Google Cloud APIs for Unified Data Platform (apps/udp & infra/udp)
locals {
  udp_required_apis = toset([
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "composer.googleapis.com",
    "dataform.googleapis.com",
    "iam.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "sourcemanager.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project_service" "udp_apis" {
  for_each           = local.udp_required_apis
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

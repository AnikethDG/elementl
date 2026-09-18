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

# Cloud Composer 3 Private Environment & Secret Manager Declarations
resource "google_secret_manager_secret" "udp_secrets" {
  for_each = toset([
    "secret-p6-db-config",
    "secret-p6-db-ca-bundle",
    "secret-p6-api-username",
    "secret-p6-api-password",
    "secret-netsuite-config",
  ])

  project   = var.project_id
  secret_id = each.key

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_composer_environment" "udp_composer" {
  provider = google-beta
  count    = var.enable_composer ? 1 : 0
  project  = var.project_id
  name     = "composer-udp-${var.environment}"
  region   = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = "composer-3-airflow-2.9.3"
    }

    node_config {
      network    = var.vpc_network
      subnetwork = var.vpc_subnet
    }
  }
}

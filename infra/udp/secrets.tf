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

# Secret Manager Secret Containers for P6, NetSuite, and Atlas Ingestion Jobs
resource "google_secret_manager_secret" "udp_secrets" {
  for_each = toset([
    "secret-p6-db-config",
    "secret-p6-db-ca-bundle",
    "secret-p6-api-username",
    "secret-p6-api-password",
    # NetSuite SuiteQL OAuth 2.0 M2M JWT Secrets (used by suiteql-ingestion/main.py)
    "secret-netsuite-config",
    "secret-netsuite-api-config",
    "secret-netsuite-account-id",
    "secret-netsuite-client-id",
    "secret-netsuite-certificate-id",
    "secret-netsuite-scope",
    "secret-netsuite-private-key",
    # Atlas GIS & Tabular Secrets
    "secret-atlas-api-config"
  ])
  project   = var.project_id
  secret_id = each.value

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

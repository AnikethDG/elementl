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

# Cloud Composer 3 Medium Environment (Composer 3 + Airflow 3) & Secret Manager Declarations
resource "google_secret_manager_secret" "udp_secrets" {
  for_each = toset([
    "secret-p6-db-config",
    "secret-p6-db-ca-bundle",
    "secret-p6-api-username",
    "secret-p6-api-password",
    "secret-netsuite-config",
    "secret-netsuite-api-config",
    "secret-netsuite-account-id",
    "secret-netsuite-client-id",
    "secret-netsuite-certificate-id",
    "secret-netsuite-scope",
    "secret-netsuite-private-key",
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

# Standardized UDP Platform Service Accounts (per Elementl UDP Naming Convention Doc)
locals {
  udp_platform_service_accounts = {
    "gcp-sa-nsedusc1-data-ingest"    = "UDP Ingestion Pipeline Execution Service Account"
    "gcp-sa-nsedusc1-data-transform" = "UDP Dataform Transformation (Bronze -> Silver -> Gold) Service Account"
    "gcp-sa-nsedusc1-composer"       = "UDP Cloud Composer 3 Orchestration Service Account"
  }
}

resource "google_service_account" "udp_platform_sas" {
  for_each     = local.udp_platform_service_accounts
  project      = var.project_id
  account_id   = each.key
  display_name = each.value
}

# Cloud Composer 3 Medium Environment running Composer 3 + Apache Airflow 3
resource "google_composer_environment" "udp_composer" {
  provider = google-beta
  count    = var.enable_composer ? 1 : 0
  project  = var.project_id
  name     = "composer-udp-${var.environment}"
  region   = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_MEDIUM"

    software_config {
      # Cloud Composer 3 with Apache Airflow 3 (e.g., composer-3-airflow-3)
      image_version = var.composer_image_version

      env_variables = {
        GCP_PROJECT_ID         = var.project_id
        GCP_REGION             = var.region
        ENVIRONMENT            = var.environment
        RAW_BUCKET_NAME        = google_storage_bucket.udp_bronze_raw.name
        STAGING_BUCKET         = google_storage_bucket.udp_landing_staging.name
        ARCHIVE_BUCKET         = google_storage_bucket.udp_bronze_archive.name
        P6_SCHEMA              = "ELEMENTL_PMDB_SBOX_PXRPTUSER"
        DATAFORM_REPOSITORY_ID = google_dataform_repository.udp_dataform_repo.name
        ARTIFACT_REGISTRY_URI  = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.udp_ingestion_repo.repository_id}"
      }
    }

    workloads_config {
      scheduler {
        cpu        = 2
        memory_gb  = 4
        storage_gb = 2
        count      = 2
      }
      web_server {
        cpu        = 2
        memory_gb  = 4
        storage_gb = 2
      }
      worker {
        cpu        = 2
        memory_gb  = 8
        storage_gb = 10
        min_count  = 2
        max_count  = 6
      }
      triggerer {
        cpu       = 1
        memory_gb = 2
        count     = 2
      }
    }

    node_config {
      service_account = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-composer"].email
      network         = var.vpc_network
      subnetwork      = var.vpc_subnet
    }
  }
}

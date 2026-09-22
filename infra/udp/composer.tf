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

# Platform Service Accounts for Ingestion, Transformation, and Orchestration
locals {
  platform_service_accounts = {
    "gcp-sa-nsedusc1-data-ingest"    = "UDP Data Ingestion Platform Service Account"
    "gcp-sa-nsedusc1-data-transform" = "UDP Data Transformation (Dataform) Platform Service Account"
    "gcp-sa-nsedusc1-composer"       = "UDP Cloud Composer 3 Orchestration Service Account"
  }
}

resource "google_service_account" "udp_platform_sas" {
  for_each     = local.platform_service_accounts
  project      = var.project_id
  account_id   = each.key
  display_name = each.value
}

# Cloud Composer 3 Environment (Small Instance, Composer 3 + Airflow 3, Private IP)
resource "google_composer_environment" "udp_composer" {
  count   = var.enable_composer ? 1 : 0
  project = var.project_id
  name    = "composer-elementl-udp-${var.environment}"
  region  = var.region

  config {
    software_config {
      image_version = var.composer_image_version

      pypi_packages = {
        "oracledb"     = ""
        "pyarrow"      = ""
        "cryptography" = ""
        "PyJWT"        = ""
      }

      env_variables = {
        GCP_PROJECT_ID         = var.project_id
        GCP_REGION             = var.region
        ENVIRONMENT            = var.environment
        RAW_BUCKET_NAME        = google_storage_bucket.udp_bronze_raw.name
        ARCHIVE_BUCKET_NAME    = google_storage_bucket.udp_bronze_archive.name
        STAGING_BUCKET_NAME    = google_storage_bucket.udp_landing_staging.name
        DATAFLOW_TEMP_BUCKET   = google_storage_bucket.udp_dataflow_temp.name
        DATAFORM_REPOSITORY_ID = "gcp-dataform-transformations"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
      triggerer {
        cpu       = 0.5
        memory_gb = 1
        count     = 1
      }
    }

    node_config {
      service_account      = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-composer"].email
      network              = var.vpc_network != "" ? var.vpc_network : null
      subnetwork           = var.vpc_subnet != "" ? var.vpc_subnet : null
      enable_ip_masq_agent = true
    }
  }
}

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

# Instantiate the 4 Containerized Ingestion Cloud Run Jobs directly (without a separate Terraform module)
locals {
  ingestion_jobs = {
    "arcgis-ingestion" = {
      description = "Atlas ArcGIS MapServer / FeatureServer REST ingestion worker (S1-02 Substations, S1-06 USGS Qfaults, S2-10 FEMA RAPT Hospitals)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "ARCGIS"
    }
    "bulk-ingestion" = {
      description = "Atlas bulk shapefile/zip download, unpacking, and tabular API ingestion worker (S1-01 USGS Streamflow, S2-20 Census, S1-07 USGS NSHM)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "BULK"
    }
    "suiteql-ingestion" = {
      description = "Oracle NetSuite OAuth 2.0 M2M JWT SuiteQL REST API ingestion worker (11 tables)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "SUITEQL"
    }
    "jdbc-ingestion" = {
      description = "Oracle Primavera P6 TCPS 2484 JDBC/oracledb ingestion worker (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
      cpu         = "2"
      memory      = "4Gi"
      source_type = "JDBC_P6"
    }
  }

  netsuite_secret_ids = toset([
    "secret-netsuite-config",
    "secret-netsuite-api-config",
    "secret-netsuite-account-id",
    "secret-netsuite-client-id",
    "secret-netsuite-certificate-id",
    "secret-netsuite-scope",
    "secret-netsuite-private-key",
  ])
}

resource "google_service_account" "udp_job_sa" {
  for_each     = local.ingestion_jobs
  project      = var.project_id
  account_id   = substr("sa-${each.key}", 0, 28)
  display_name = "UDP Runtime SA for ${each.key}"
  description  = "Dedicated least-privilege service account for Cloud Run Job ${each.key}"
}

resource "google_storage_bucket_iam_member" "udp_job_raw_bucket_access" {
  for_each = local.ingestion_jobs
  bucket   = google_storage_bucket.udp_bronze_raw.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.udp_job_sa[each.key].email}"
}

resource "google_cloud_run_v2_job" "udp_ingestion_jobs" {
  for_each = local.ingestion_jobs
  project  = var.project_id
  name     = each.key
  location = var.region

  template {
    template {
      service_account = google_service_account.udp_job_sa[each.key].email
      max_retries     = 2
      timeout         = "3600s"

      dynamic "vpc_access" {
        for_each = var.vpc_subnet != "" ? [1] : []
        content {
          egress = "ALL_TRAFFIC"
          network_interfaces {
            network    = var.vpc_network != "" ? var.vpc_network : null
            subnetwork = var.vpc_subnet
          }
        }
      }

      containers {
        image = "us-docker.pkg.dev/cloudrun/container/job:latest"

        resources {
          limits = {
            cpu    = each.value.cpu
            memory = each.value.memory
          }
        }

        env {
          name  = "GCP_PROJECT_ID"
          value = var.project_id
        }
        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }
        env {
          name  = "RAW_BUCKET_NAME"
          value = google_storage_bucket.udp_bronze_raw.name
        }
        env {
          name  = "ARCHIVE_BUCKET"
          value = google_storage_bucket.udp_bronze_archive.name
        }
        env {
          name  = "SOURCE_TYPE"
          value = each.value.source_type
        }
        env {
          name  = "P6_SCHEMA"
          value = "ELEMENTL_PMDB_SBOX_PXRPTUSER"
        }
        env {
          name  = "NETSUITE_PRIVATE_KEY_SECRET_ID"
          value = "secret-netsuite-private-key"
        }
        env {
          name  = "NETSUITE_SCOPE"
          value = "rest_webservices"
        }
      }
    }
  }

  lifecycle {
    # Prevent config drift when Cloud Build updates container image digests
    ignore_changes = [
      template[0].template[0].containers[0].image,
      client,
      client_version,
    ]
  }
}

# Resource-Level Least-Privilege IAM (Compensating Control: No Project-Level Data Roles)
resource "google_secret_manager_secret_iam_member" "jdbc_p6_config_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets["secret-p6-db-config"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.udp_job_sa["jdbc-ingestion"].email}"
}

resource "google_secret_manager_secret_iam_member" "jdbc_p6_ca_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets["secret-p6-db-ca-bundle"].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.udp_job_sa["jdbc-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "jdbc_p6_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_p6"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["jdbc-ingestion"].email}"
}

resource "google_secret_manager_secret_iam_member" "suiteql_netsuite_secrets_access" {
  for_each  = local.netsuite_secret_ids
  project   = var.project_id
  secret_id = google_secret_manager_secret.udp_secrets[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.udp_job_sa["suiteql-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "suiteql_netsuite_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_netsuite"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["suiteql-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "arcgis_atlas_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_atlas"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["arcgis-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "bulk_atlas_raw_dataset_access" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets["raw_atlas"].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["bulk-ingestion"].email}"
}



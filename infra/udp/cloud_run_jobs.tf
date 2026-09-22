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

# 4 Containerized Ingestion Cloud Run v2 Jobs (backed by Artifact Registry & apps/udp/cloud-run-jobs/)
locals {
  artifact_registry_base_uri = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.udp_ingestion_repo.repository_id}"

  ingestion_jobs = {
    "arcgis-ingestion" = {
      description     = "Atlas ArcGIS MapServer / FeatureServer REST ingestion worker (apps/udp/cloud-run-jobs/arcgis-ingestion)"
      dockerfile_path = "apps/udp/cloud-run-jobs/arcgis-ingestion/Dockerfile"
      cpu             = "2"
      memory          = "4Gi"
      source_type     = "ARCGIS"
      bronze_dataset  = "ds_bronze_atlas"
      silver_dataset  = "ds_silver_atlas"
    }
    "bulk-ingestion" = {
      description     = "Atlas bulk shapefile/zip download, unpacking, and tabular API ingestion worker (apps/udp/cloud-run-jobs/bulk-ingestion)"
      dockerfile_path = "apps/udp/cloud-run-jobs/bulk-ingestion/Dockerfile"
      cpu             = "2"
      memory          = "4Gi"
      source_type     = "BULK"
      bronze_dataset  = "ds_bronze_atlas"
      silver_dataset  = "ds_silver_atlas"
    }
    "suiteql-ingestion" = {
      description     = "Oracle NetSuite OAuth 2.0 M2M JWT SuiteQL REST API ingestion worker (apps/udp/cloud-run-jobs/suiteQL-ingestion)"
      dockerfile_path = "apps/udp/cloud-run-jobs/suiteQL-ingestion/Dockerfile"
      cpu             = "2"
      memory          = "4Gi"
      source_type     = "SUITEQL"
      bronze_dataset  = "ds_bronze_netsuite"
      silver_dataset  = "ds_silver_netsuite"
    }
    "jdbc-ingestion" = {
      description     = "Oracle Primavera P6 TCPS 2484 JDBC/oracledb ingestion worker for ELEMENTL_PMDB_SBOX_PXRPTUSER (apps/udp/cloud-run-jobs/jdbc-ingestion)"
      dockerfile_path = "apps/udp/cloud-run-jobs/jdbc-ingestion/Dockerfile"
      cpu             = "2"
      memory          = "4Gi"
      source_type     = "JDBC_P6"
      bronze_dataset  = "ds_bronze_p6"
      silver_dataset  = "ds_silver_p6"
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

resource "google_storage_bucket_iam_member" "udp_job_staging_bucket_access" {
  for_each = local.ingestion_jobs
  bucket   = google_storage_bucket.udp_landing_staging.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.udp_job_sa[each.key].email}"
}

resource "google_cloud_run_v2_job" "udp_ingestion_jobs" {
  depends_on = [
    google_artifact_registry_repository.udp_ingestion_repo,
    google_artifact_registry_repository_iam_member.udp_job_sa_repo_reader,
  ]
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
        # Pulls container image built from apps/udp/cloud-run-jobs/<job>/Dockerfile in Artifact Registry
        image = "${local.artifact_registry_base_uri}/${each.key}:${var.image_tag}"

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
          name  = "STAGING_BUCKET"
          value = google_storage_bucket.udp_landing_staging.name
        }
        env {
          name  = "BRONZE_DATASET"
          value = each.value.bronze_dataset
        }
        env {
          name  = "SILVER_DATASET"
          value = each.value.silver_dataset
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

# Allow Cloud Composer 3 SA to execute the 4 Cloud Run Jobs via CloudRunExecuteJobOperator
resource "google_cloud_run_v2_job_iam_member" "composer_run_invoker" {
  for_each = local.ingestion_jobs
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.udp_ingestion_jobs[each.key].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-composer"].email}"
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
  for_each   = toset(["ds_bronze_p6", "ds_operations"])
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
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
  for_each   = toset(["ds_bronze_netsuite", "ds_operations"])
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["suiteql-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "arcgis_atlas_raw_dataset_access" {
  for_each   = toset(["ds_bronze_atlas", "ds_operations"])
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["arcgis-ingestion"].email}"
}

resource "google_bigquery_dataset_iam_member" "bulk_atlas_raw_dataset_access" {
  for_each   = toset(["ds_bronze_atlas", "ds_operations"])
  project    = var.project_id
  dataset_id = google_bigquery_dataset.datasets[each.key].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.udp_job_sa["bulk-ingestion"].email}"
}

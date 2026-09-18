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

# Composite Resource: Cloud Run v2 Job + Dedicated Runtime Service Account + Resource-Level IAM
resource "google_service_account" "job_sa" {
  project      = var.project_id
  account_id   = substr("sa-${var.job_name}", 0, 28)
  display_name = "UDP Runtime SA for ${var.job_name}"
  description  = "Dedicated least-privilege service account for Cloud Run Job ${var.job_name}"
}

resource "google_storage_bucket_iam_member" "raw_bucket_access" {
  count  = var.raw_bucket_name != "" ? 1 : 0
  bucket = var.raw_bucket_name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.job_sa.email}"
}

resource "google_cloud_run_v2_job" "job" {
  project  = var.project_id
  name     = var.job_name
  location = var.region

  template {
    template {
      service_account = google_service_account.job_sa.email
      max_retries     = var.max_retries
      timeout         = var.timeout

      dynamic "vpc_access" {
        for_each = var.vpc_subnet != "" ? [1] : []
        content {
          egress = var.vpc_egress
          network_interfaces {
            network    = var.vpc_network != "" ? var.vpc_network : null
            subnetwork = var.vpc_subnet
          }
        }
      }

      containers {
        image = var.image

        resources {
          limits = {
            cpu    = var.cpu
            memory = var.memory
          }
        }

        dynamic "env" {
          for_each = var.env_vars
          content {
            name  = env.key
            value = env.value
          }
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

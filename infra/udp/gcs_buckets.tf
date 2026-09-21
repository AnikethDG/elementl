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

# UDP Bronze Landing and Archive Buckets with GCS Object Lifecycle Management
resource "google_storage_bucket" "udp_bronze_raw" {
  project                     = var.project_id
  name                        = "bkt-${var.project_id}-udp-bronze-raw"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  # Transition processed raw landing objects after 30 days to Archive storage class
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
    condition {
      age = 30
    }
  }
}

resource "google_storage_bucket" "udp_bronze_archive" {
  project                     = var.project_id
  name                        = "bkt-${var.project_id}-udp-bronze-archive"
  location                    = var.region
  storage_class               = "ARCHIVE"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "udp_landing_staging" {
  project                     = var.project_id
  name                        = "bkt-${var.project_id}-udp-landing-staging"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }
}

resource "google_storage_bucket" "udp_dataflow_temp" {
  project                     = var.project_id
  name                        = "bkt-${var.project_id}-udp-dataflow-temp"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }
}


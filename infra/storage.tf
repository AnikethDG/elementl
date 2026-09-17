# ------------------------------------------------------------------------------
# 5. Test Bucket (development only)
# ------------------------------------------------------------------------------
# Throwaway scratch bucket. Deliberately disposable: no versioning, soft delete
# off, force_destroy on, and a 7-day sweep so it cannot accrue cost unnoticed.
#
# Gated on the project ID rather than var.environment so the bucket cannot be
# created anywhere else -- not by a mis-set _ENVIRONMENT substitution, and not
# by a bare `terraform apply` that falls back to the terraform.tfvars project.
# Hardcoding an ID is acceptable for a test resource; do not copy this pattern
# for anything permanent.
locals {
  test_bucket_project = "pid-nse-dev-core-apps-dz09"
}

resource "google_storage_bucket" "test" {
  count      = var.project_id == local.test_bucket_project ? 1 : 0
  depends_on = [google_project_service.apis]

  project  = var.project_id
  name     = "${var.project_id}-test-bucket"
  location = var.region

  force_destroy               = true
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7
    }
  }
}

resource "google_storage_bucket_iam_member" "test_access" {
  count  = var.project_id == local.test_bucket_project ? 1 : 0
  bucket = google_storage_bucket.test[0].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloud_run_sa["service-api"].email}"
}

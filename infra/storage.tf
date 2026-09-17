# ------------------------------------------------------------------------------
# 5. Test Bucket (development only)
# ------------------------------------------------------------------------------
# Throwaway scratch bucket. Deliberately disposable: no versioning, soft delete
# off, force_destroy on, and a 7-day sweep so it cannot accrue cost unnoticed.
resource "google_storage_bucket" "test" {
  count      = var.environment == "development" ? 1 : 0
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
  count  = var.environment == "development" ? 1 : 0
  bucket = google_storage_bucket.test[0].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.cloud_run_sa["service-api"].email}"
}

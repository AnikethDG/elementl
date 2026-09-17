# ------------------------------------------------------------------------------
# 6. Shared Test Bucket (all environments)
# ------------------------------------------------------------------------------
# Deliberately ungated: one of these is created in every environment this
# Terraform is applied to, named after the project so each is distinct and
# globally unique.
#
# Disposable by design, in prod as well as dev: no versioning, soft delete off,
# force_destroy on, and a 7-day sweep. Do not put anything here you would miss.
#
# No IAM binding: nothing reads this bucket, and the Terraform runner lacks
# storage.buckets.setIamPolicy. Add one alongside whatever first needs access.
resource "google_storage_bucket" "shared_test" {
  depends_on = [google_project_service.apis]

  project  = var.project_id
  name     = "${var.project_id}-test-bucket-shared"
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

  labels = {
    environment = var.environment
    managed-by  = "terraform"
    purpose     = "test"
  }
}

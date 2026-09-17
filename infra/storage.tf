# ------------------------------------------------------------------------------
# 5. Cloud Storage Buckets
# ------------------------------------------------------------------------------
# Buckets are private by default: uniform bucket-level access (no per-object
# ACLs) and enforced public access prevention. Application access is granted
# explicitly through the Cloud Run runtime service accounts below.
resource "google_storage_bucket" "buckets" {
  for_each   = var.buckets
  depends_on = [google_project_service.apis]

  name     = coalesce(each.value.name, "${var.project_id}-${each.key}")
  location = coalesce(each.value.location, var.region)
  project  = var.project_id

  storage_class = each.value.storage_class
  force_destroy = each.value.force_destroy

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = each.value.versioning
  }

  dynamic "lifecycle_rule" {
    for_each = each.value.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action_type
        storage_class = lifecycle_rule.value.storage_class
      }
      condition {
        age                   = lifecycle_rule.value.age
        num_newer_versions    = lifecycle_rule.value.num_newer_versions
        with_state            = lifecycle_rule.value.with_state
        matches_storage_class = lifecycle_rule.value.matches_storage_class
      }
    }
  }

  labels = merge(
    {
      environment = var.environment
      managed-by  = "terraform"
    },
    each.value.labels
  )
}

# ------------------------------------------------------------------------------
# 5a. Bucket IAM for Cloud Run Runtime Service Accounts
# ------------------------------------------------------------------------------
# Flatten the bucket -> service grants into a single map so each binding is an
# independent, additive (non-authoritative) IAM member.
locals {
  bucket_iam_grants = merge([
    for bucket_key, bucket in var.buckets : {
      for service_key in bucket.service_accounts :
      "${bucket_key}/${service_key}" => {
        bucket_key  = bucket_key
        service_key = service_key
        role        = bucket.service_account_role
      }
    }
  ]...)
}

resource "google_storage_bucket_iam_member" "service_access" {
  for_each = local.bucket_iam_grants

  bucket = google_storage_bucket.buckets[each.value.bucket_key].name
  role   = each.value.role
  member = "serviceAccount:${google_service_account.cloud_run_sa[each.value.service_key].email}"
}

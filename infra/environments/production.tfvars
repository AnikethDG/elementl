# ==============================================================================
# Production Environment Configuration (Prod GCP Project)
# ==============================================================================
# Update with your dedicated Production GCP Project ID
project_id         = "pid-nse-prd-core-apps-6aw8"
region             = "us-central1"
environment        = "production"
artifact_repo_name = "app-repo"

services = {
  "service-api" = {
    description        = "Primary API service (Production)"
    image_placeholder  = "us-docker.pkg.dev/cloudrun/container/hello"
    port               = 8080
    cpu                = "2"
    memory             = "1Gi"
    min_instance_count = 1
    max_instance_count = 20
    allow_unauth       = true
    ingress            = "INGRESS_TRAFFIC_ALL"
    env_vars = {
      ENVIRONMENT       = "production"
      ENVIRONMENT_SHORT = "prd"
      LOG_LEVEL         = "WARNING"
    }
  }
}

buckets = {
  "app-data" = {
    storage_class    = "STANDARD"
    versioning       = true
    force_destroy    = false
    service_accounts = ["service-api"]
    lifecycle_rules = [
      {
        # Age off old object versions, keeping the 3 most recent
        action_type        = "Delete"
        with_state         = "ARCHIVED"
        num_newer_versions = 3
        age                = 90
      },
      {
        # Cool current objects that have not been touched in a year
        action_type   = "SetStorageClass"
        storage_class = "NEARLINE"
        with_state    = "LIVE"
        age           = 365
      }
    ]
  }
}

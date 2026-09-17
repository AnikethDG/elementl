# ==============================================================================
# Development Environment Configuration (Dev GCP Project)
# ==============================================================================
project_id         = "pid-nse-dev-core-apps-dz09" # Dev GCP Project ID
region             = "us-central1"
environment        = "development"
artifact_repo_name = "app-repo"

services = {
  "service-api" = {
    description        = "Primary API service (Dev)"
    image_placeholder  = "us-docker.pkg.dev/cloudrun/container/hello"
    port               = 8080
    cpu                = "1"
    memory             = "512Mi"
    min_instance_count = 0
    max_instance_count = 2
    allow_unauth       = true
    ingress            = "INGRESS_TRAFFIC_ALL"
    env_vars = {
      ENVIRONMENT       = "development"
      ENVIRONMENT_SHORT = "dev"
      LOG_LEVEL         = "DEBUG"
    }
  }
}

# Test bucket. Resolves to "<project_id>-test-bucket" -- a bare "test-bucket"
# is not available, GCS bucket names are globally unique across all projects.
#
# Built to be thrown away: versioning is off so there are no noncurrent
# versions to sweep up, force_destroy lets `terraform destroy` remove the
# bucket while it still holds objects, and the lifecycle rule empties it on a
# 7-day rolling basis so it cannot quietly accrue storage cost.
buckets = {
  "test-bucket" = {
    storage_class    = "STANDARD"
    versioning       = false
    force_destroy    = true
    service_accounts = ["service-api"]
    lifecycle_rules = [
      {
        action_type = "Delete"
        with_state  = "LIVE"
        age         = 7
      }
    ]
  }
}

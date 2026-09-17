# ==============================================================================
# Staging Environment Configuration (Staging GCP Project)
# ==============================================================================
# Update with your dedicated Staging GCP Project ID
project_id         = "pid-nse-stg-core-apps-k8ti"
region             = "us-central1"
environment        = "staging"
artifact_repo_name = "app-repo"

services = {
  "service-api" = {
    description        = "Primary API service (Staging)"
    image_placeholder  = "us-docker.pkg.dev/cloudrun/container/hello"
    port               = 8080
    cpu                = "1"
    memory             = "512Mi"
    min_instance_count = 0
    max_instance_count = 5
    allow_unauth       = true
    ingress            = "INGRESS_TRAFFIC_ALL"
    env_vars = {
      ENVIRONMENT       = "staging"
      ENVIRONMENT_SHORT = "stg"
      LOG_LEVEL         = "INFO"
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
        action_type        = "Delete"
        with_state         = "ARCHIVED"
        num_newer_versions = 2
        age                = 14
      }
    ]
  }
}

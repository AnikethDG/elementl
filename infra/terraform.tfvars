# Project Configuration
project_id         = "p-nsedusc1-core-app-fe-01-irzf"
region             = "us-central1"
artifact_repo_name = "app-repo"

# Services Configuration
services = {
  "service-api" = {
    description        = "Primary API service"
    image_placeholder  = "us-docker.pkg.dev/cloudrun/container/hello"
    port               = 8080
    cpu                = "1"
    memory             = "512Mi"
    min_instance_count = 0
    max_instance_count = 10
    allow_unauth       = true
    ingress            = "INGRESS_TRAFFIC_ALL"
    env_vars = {
      ENVIRONMENT = "production"
      LOG_LEVEL   = "info"
    }
  }
}

# Storage Configuration
# Bucket name resolves to "<project_id>-app-data"
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
        num_newer_versions = 3
        age                = 30
      }
    ]
  }
}

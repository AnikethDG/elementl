# ==============================================================================
# Development Environment Configuration (Dev GCP Project)
# ==============================================================================
project_id         = "elementl-2" # Target GCP Project ID
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

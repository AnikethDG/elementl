# ==============================================================================
# Development Environment Configuration (Dev GCP Project)
# ==============================================================================
project_id         = "pid-exc-dev-core-apps-fdzr" # Dev GCP Project ID
region             = "us-central1"
environment        = "dev"
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
      ENVIRONMENT = "dev"
      LOG_LEVEL   = "debug"
    }
  }
}

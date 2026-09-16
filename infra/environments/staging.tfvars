# ==============================================================================
# Staging Environment Configuration (Staging GCP Project)
# ==============================================================================
# Update with your dedicated Staging GCP Project ID
project_id         = "p-nsedusc1-core-app-fe-01-staging"
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
      ENVIRONMENT = "staging"
      LOG_LEVEL   = "info"
    }
  }
}

# ==============================================================================
# Production Environment Configuration (Prod GCP Project)
# ==============================================================================
# Update with your dedicated Production GCP Project ID
project_id         = "pid-nse-prd-core-apps-6aw8"
region             = "us-central1"
environment        = "prd"
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
      ENVIRONMENT = "production"
      LOG_LEVEL   = "warn"
    }
  }
}

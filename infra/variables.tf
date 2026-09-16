variable "project_id" {
  description = "The Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "The default GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "The target deployment environment (development, staging, production)"
  type        = string
  default     = "development"
}

variable "artifact_repo_name" {
  description = "The name of the Artifact Registry repository for Docker images"
  type        = string
  default     = "app-repo"
}

variable "enable_apis" {
  description = "List of required GCP APIs to enable"
  type        = list(string)
  default = [
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "iam.googleapis.com"
  ]
}

variable "services" {
  description = "Map of Cloud Run services to provision shells for"
  type = map(object({
    description        = optional(string, "Managed Cloud Run service")
    image_placeholder  = optional(string, "us-docker.pkg.dev/cloudrun/container/hello")
    port               = optional(number, 8080)
    cpu                = optional(string, "1")
    memory             = optional(string, "512Mi")
    min_instance_count = optional(number, 0)
    max_instance_count = optional(number, 10)
    allow_unauth       = optional(bool, true)
    ingress            = optional(string, "INGRESS_TRAFFIC_ALL") # INGRESS_TRAFFIC_ALL or INGRESS_TRAFFIC_INTERNAL_ONLY
    env_vars           = optional(map(string), {})
  }))
  default = {
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
      }
    }
  }
}

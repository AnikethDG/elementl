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
    "iam.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com"
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

variable "buckets" {
  description = "Map of Cloud Storage buckets to provision. The map key is the short name; the full bucket name defaults to \"<project_id>-<key>\" to satisfy the global uniqueness requirement."
  type = map(object({
    name          = optional(string)             # Override the generated "<project_id>-<key>" name
    location      = optional(string)             # Defaults to var.region
    storage_class = optional(string, "STANDARD") # STANDARD, NEARLINE, COLDLINE, ARCHIVE
    versioning    = optional(bool, true)
    force_destroy = optional(bool, false) # Keep false in prod: allows `terraform destroy` to delete objects
    labels        = optional(map(string), {})

    # null keeps the GCS default of 7 days; 0 disables soft delete entirely
    soft_delete_retention_seconds = optional(number)

    # Cloud Run services (keys of var.services) granted access to this bucket
    service_accounts     = optional(list(string), [])
    service_account_role = optional(string, "roles/storage.objectAdmin")

    lifecycle_rules = optional(list(object({
      action_type           = string           # Delete or SetStorageClass
      storage_class         = optional(string) # Required when action_type = SetStorageClass
      age                   = optional(number)
      num_newer_versions    = optional(number)
      with_state            = optional(string) # LIVE, ARCHIVED, ANY
      matches_storage_class = optional(list(string))
    })), [])
  }))
  # Empty by default: a bucket is created only in environments that opt in
  default = {}
}

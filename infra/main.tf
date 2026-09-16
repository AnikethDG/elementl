provider "google" {
  project = var.project_id
  region  = var.region
}

# ------------------------------------------------------------------------------
# 1. Enable Required GCP APIs
# ------------------------------------------------------------------------------
resource "google_project_service" "apis" {
  for_each           = toset(var.enable_apis)
  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

# ------------------------------------------------------------------------------
# 2. Artifact Registry for Container Images
# ------------------------------------------------------------------------------
resource "google_artifact_registry_repository" "repo" {
  depends_on    = [google_project_service.apis]
  location      = var.region
  repository_id = var.artifact_repo_name
  description   = "Docker repository for Cloud Run monorepo services"
  format        = "DOCKER"
}

# ------------------------------------------------------------------------------
# 3. Dedicated Runtime Service Accounts (Least Privilege)
# ------------------------------------------------------------------------------
resource "google_service_account" "cloud_run_sa" {
  for_each     = var.services
  account_id   = "sa-${each.key}"
  display_name = "Runtime Service Account for ${each.key}"
  description  = "Dedicated service account for Cloud Run service ${each.key}"
}

# ------------------------------------------------------------------------------
# 4. Cloud Run Services (Shell / Scaffold)
# ------------------------------------------------------------------------------
# The Deployment Handoff Pattern:
# Terraform provisions the infrastructure shell, while Cloud Build deploys
# application code and updates the container image tag. The `ignore_changes`
# block prevents Terraform from rolling back revisions deployed by Cloud Build.
resource "google_cloud_run_v2_service" "services" {
  for_each   = var.services
  depends_on = [google_artifact_registry_repository.repo, google_service_account.cloud_run_sa]

  name        = each.key
  location    = var.region
  description = each.value.description
  ingress     = each.value.ingress

  template {
    service_account = google_service_account.cloud_run_sa[each.key].email

    scaling {
      min_instance_count = each.value.min_instance_count
      max_instance_count = each.value.max_instance_count
    }

    containers {
      image = each.value.image_placeholder

      ports {
        container_port = each.value.port
      }

      resources {
        limits = {
          cpu    = each.value.cpu
          memory = each.value.memory
        }
      }

      dynamic "env" {
        for_each = each.value.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }
    }
  }

  lifecycle {
    # CRITICAL: Prevent Terraform from overriding subsequent Cloud Build deployments
    ignore_changes = [
      template[0].containers[0].image,
      template[0].revision,
      client,
      client_version
    ]
  }
}

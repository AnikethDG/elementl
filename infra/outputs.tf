output "project_id" {
  description = "The GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "The GCP region"
  value       = var.region
}

output "artifact_registry_id" {
  description = "Artifact Registry Repository ID"
  value       = google_artifact_registry_repository.repo.id
}

output "artifact_registry_url" {
  description = "Docker repository URL for pushing images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.repo.repository_id}"
}

output "service_urls" {
  description = "Deployed Cloud Run service URIs"
  value = {
    for k, v in google_cloud_run_v2_service.services : k => v.uri
  }
}

output "service_accounts" {
  description = "Dedicated runtime service account emails for each service"
  value = {
    for k, v in google_service_account.cloud_run_sa : k => v.email
  }
}

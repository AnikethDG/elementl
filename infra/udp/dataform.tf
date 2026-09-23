# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Provision the Google-managed Dataform Service Agent in this project
# Identity: service-<SERVICES_PROJECT_NUMBER>@gcp-sa-dataform.iam.gserviceaccount.com
resource "google_project_service_identity" "dataform_sa" {
  provider = google-beta
  project  = var.project_id
  service  = google_project_service.udp_apis["dataform.googleapis.com"].service
}

# Create the secret and initial dummy version for GitHub PAT / token
resource "google_secret_manager_secret" "dataform_git_token" {
  secret_id = "secret-dataform-git-token"
  project   = var.project_id

  replication {
    user_managed {
      replicas {
        location = var.region
      }
    }
  }
}

resource "google_secret_manager_secret_version" "dataform_git_token_version" {
  secret      = google_secret_manager_secret.dataform_git_token.id
  secret_data = "dummy-git-token-replace-me"

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# Allow Dataform Service Agent to read the GitHub PAT secret in Secret Manager
resource "google_secret_manager_secret_iam_member" "dataform_git_token_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.dataform_git_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_project_service_identity.dataform_sa.email}"
}

# Allow Dataform Service Agent to impersonate custom execution SA (gcp-sa-nsedusc1-data-transform)
resource "google_service_account_iam_member" "dataform_agent_token_creator" {
  service_account_id = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_project_service_identity.dataform_sa.email}"
}

resource "google_service_account_iam_member" "dataform_agent_sa_user" {
  service_account_id = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_project_service_identity.dataform_sa.email}"
}

# 4. Allow Cloud Composer SA to compile & invoke Dataform workflows
resource "google_project_iam_member" "composer_dataform_editor" {
  project = var.project_id
  role    = "roles/dataform.editor"
  member  = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-composer"].email}"
}

resource "google_service_account_iam_member" "composer_dataform_sa_user" {
  service_account_id = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.udp_platform_sas["gcp-sa-nsedusc1-composer"].email}"
}


# Dataform Repository (Connected via HTTPS PAT to Git repo)
resource "google_dataform_repository" "elt_repository" {
  provider        = google-beta
  project         = var.project_id
  region          = var.region
  name            = "gcp-dataform-transformations"
  display_name    = "Elementl Data Platform ELT Repository"
  service_account = google_service_account.udp_platform_sas["gcp-sa-nsedusc1-data-transform"].email

  git_remote_settings {
    url                                 = var.dataform_git_url
    default_branch                      = "main"
    authentication_token_secret_version = "${google_secret_manager_secret.dataform_git_token.id}/versions/latest"
  }

  workspace_compilation_overrides {
    default_database = var.project_id
    schema_suffix    = "$${workspaceName}"
  }

  depends_on = [
    google_project_service.udp_apis["dataform.googleapis.com"],
    google_secret_manager_secret_iam_member.dataform_git_token_access,
    google_service_account_iam_member.dataform_agent_token_creator,
    google_service_account_iam_member.dataform_agent_sa_user,
    google_secret_manager_secret.dataform_git_token,
    google_secret_manager_secret_version.dataform_git_token_version
  ]
}

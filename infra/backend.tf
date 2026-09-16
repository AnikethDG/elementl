terraform {
  backend "gcs" {
    # Replace with the GCS bucket created during your project creation
    # e.g., "p-nsedusc1-core-app-fe-01-irzf-tfstate" or your project's default bucket
    bucket = "p-nsedusc1-core-app-fe-01-irzf-tfstate"
    prefix = "terraform/infra/state"
  }
}

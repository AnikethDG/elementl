terraform {
  backend "gcs" {
    # Replace with the GCS bucket created during your project creation
    # e.g., "p-nsedusc1-core-app-fe-01-irzf-tfstate" or your project's default bucket
    bucket = "gcs-pid-ec-cmn-app-tfstate-2b36"
    prefix = "terraform/app-infra/development/pid-exc-dev-core-apps-fdzr/infra/state"
  }
}

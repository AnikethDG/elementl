terraform {
  backend "gcs" {
    # Non-sensitive application-infrastructure shared state bucket
    bucket = "gcs-pid-ns-cmn-app-tfstate-iv0b"
    prefix = "terraform/app-infra/development/pid-nse-dev-core-apps-dz09/infra/state"
  }
}

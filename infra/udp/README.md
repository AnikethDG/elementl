# Elementl Power — Unified Data Platform (UDP) Infrastructure (`infra/udp/`)

This directory houses the **100% Terraform-managed Google Cloud Infrastructure (`infra/udp/`)** for the **Elementl Unified Data Platform (UDP)** Ingestion Framework (`apps/udp/`).

* **Primary Target Environment (Non-Sensitive Stage):** `pid-nse-stg-core-apps-k8ti` (`environments/staging.tfvars`)
* **Development:** `pid-nse-dev-core-apps-dz09`
* **Production:** `pid-nse-prd-core-apps-6aw8`

All UDP ingestion GCP resources—including the Docker Artifact Registry (`udp-ingestion-jobs`), the 4 Cloud Run v2 Jobs, the **Cloud Composer 3 Small** environment (running **Composer 3** and **Apache Airflow 3**), the 4 BigQuery Ingestion & Operational datasets (`ds_bronze_oracle_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, `ds_operations`), the Bronze & Operational ingestion tables (`bronze_oracle_p6_tables`, `bronze_netsuite_tables`, `ingestion_execution_logs`, `ingestion_watermarks`), the 5 GCS buckets (`bkt-*`), and the 12 Secret Manager secrets (`secrets.tf`)—are declared and provisioned **purely via Terraform** in `infra/udp/` under an isolated Terraform state file.

---

## 1. Dual Terraform State File Architecture (`infra/` vs `infra/udp/`)

How `infra/` (core web application infrastructure) and `infra/udp/` (UDP data platform infrastructure) maintain completely separate Terraform state files inside the same environment GCS state bucket (`gs://gcs-<project_id>-tfstate`):

```text
gs://gcs-pid-nse-stg-core-apps-k8ti-tfstate/
├── terraform/
│   ├── infra/
│   │   └── state/
│   │       └── default.tfstate    <-- Managed by Root `infra/` (infra/backend.tf: prefix = "terraform/infra/state")
│   │                                  Resources: Core web app Artifact Registry (`app-repo`), Cloud Run (`service-api`), SAs
│   └── udp/
│       └── state/
│           └── default.tfstate    <-- Managed by `infra/udp/` (infra/udp/backend.tf: prefix = "terraform/udp/state")
│                                      Resources: UDP Artifact Registry (`udp-ingestion-jobs`), 4 Cloud Run Jobs,
│                                      Composer 3 Small + Airflow 3, BigQuery ingestion datasets/tables, GCS buckets, Secrets
```

### Why This Works Without Collisions
1. **Distinct GCS Backend State Prefixes:**
   * `infra/backend.tf` sets `prefix = "terraform/infra/state"`.
   * `infra/udp/backend.tf` sets `prefix = "terraform/udp/state"`.
2. **Strict Resource Ownership Separation:**
   * No GCP resource is declared in both `infra/` and `infra/udp/`. The `udp-ingestion-jobs` Docker Artifact Registry is declared exclusively in `infra/udp/artifact_registry.tf` (tracked in `terraform/udp/state/default.tfstate`), while `app-repo` is declared exclusively in `infra/main.tf` (tracked in `terraform/infra/state/default.tfstate`).
3. **CI/CD Pipeline Support (`infra/cloudbuild.yaml` & `infra/udp/cloud_build.yaml`):**
   * Both pipelines target the non-sensitive staging GCP project `pid-nse-stg-core-apps-k8ti` (`environments/staging.tfvars`) by default and initialize `infra/` with `-backend-config="prefix=terraform/infra/state"` and `infra/udp/` with `-backend-config="prefix=terraform/udp/state"`.

---

## 2. Terraform Infrastructure Files (`infra/udp/`)

| Path | Purpose |
| :--- | :--- |
| `infra/udp/apis.tf` | Enables all required Google Cloud APIs (`artifactregistry`, `run`, `cloudbuild`, `composer`, `bigquery`, `dataform`, `secretmanager`, `storage`, `iam`) and provisions the 3 platform service accounts (`gcp-sa-nsedusc1-data-ingest`, `gcp-sa-nsedusc1-data-transform`, `gcp-sa-nsedusc1-composer`). |
| `infra/udp/artifact_registry.tf` | Provisions the Docker Artifact Registry repository `udp-ingestion-jobs` (`${var.region}-docker.pkg.dev/${var.project_id}/udp-ingestion-jobs`), image retention cleanup policies, and repository-level `roles/artifactregistry.reader` / `roles/artifactregistry.writer` IAM bindings for the 4 Cloud Run Jobs and ingestion platform SA. |
| `infra/udp/cloud_run_jobs.tf` | Provisions the 4 Cloud Run v2 Jobs (`arcgis-ingestion`, `bulk-ingestion`, `jdbc-ingestion`, `suiteql-ingestion`) pointing to `${var.region}-docker.pkg.dev/${var.project_id}/udp-ingestion-jobs/<job-name>:${var.image_tag}`, with dedicated runtime service accounts (`sa-*`), Direct VPC Egress (`sub-ns-stg-usc1`, NAT IP `136.115.148.145`), Composer `roles/run.invoker` IAM, and resource-level Secret Manager & BigQuery dataset IAM bindings. |
| `infra/udp/composer.tf` | Provisions the **Cloud Composer 3 Small Environment** (`environment_size = "ENVIRONMENT_SIZE_SMALL"`, `image_version = "composer-3-airflow-3"` running **Cloud Composer 3** and **Apache Airflow 3**), Small `workloads_config` (`scheduler`: 0.5 vCPU / 2 GB RAM; `web_server`: 0.5 vCPU / 2 GB RAM; `worker`: 0.5 vCPU / 2 GB RAM, 1–3 workers; `triggerer`: 0.5 vCPU / 1 GB RAM), and Airflow environment variables. |
| `infra/udp/secrets.tf` | Provisions all 12 Secret Manager secret containers (`secret-p6-db-config`, `secret-p6-db-ca-bundle`, `secret-p6-api-username`, `secret-p6-api-password`, `secret-netsuite-config`, `secret-netsuite-api-config`, `secret-netsuite-account-id`, `secret-netsuite-client-id`, `secret-netsuite-certificate-id`, `secret-netsuite-scope`, `secret-netsuite-private-key`, `secret-atlas-api-config`) with regional user-managed replication. |
| `infra/udp/bigquery_datasets.tf` | Provisions only the **BigQuery ingestion & operational datasets** (`ds_bronze_oracle_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, `ds_operations`) and the **operational framework tables** (`ds_operations.ingestion_execution_logs` and `ds_operations.ingestion_watermarks`). Source Bronze tables are created dynamically by the ingestion framework at runtime, and Silver/Gold tables and datasets are managed separately by Dataform. |
| `infra/udp/dataform.tf` | Intentionally left blank so Akshya can author Dataform Terraform resources independently. |
| `infra/udp/gcs_buckets.tf` | Provisions the 5 UDP Cloud Storage buckets (`bkt-${var.project_id}-udp-landing-staging`, `bkt-${var.project_id}-udp-dataflow-temp`, `bkt-${var.project_id}-udp-bronze-raw`, `bkt-${var.project_id}-udp-bronze-archive`, `bkt-${var.project_id}-udp-configs`) with uniform bucket-level access, lifecycle rules, and IAM bindings. |
| `infra/udp/backend.tf` | Configures the GCS remote state backend with isolated prefix `prefix = "terraform/udp/state"`. |
| `infra/udp/variables.tf` | Defines input variables (`project_id`, `region`, `environment`, `vpc_network`, `vpc_subnet`, `artifact_registry_repo_id`, `image_tag`, `enable_composer`, `composer_image_version`). |
| `infra/udp/outputs.tf` | Exports `udp_bronze_raw_bucket`, `udp_bronze_archive_bucket`, `udp_landing_staging_bucket`, `udp_dataflow_temp_bucket`, `udp_configs_bucket`, `udp_bigquery_datasets`, `udp_cloud_run_jobs`, and `udp_job_service_accounts`. |
| `infra/udp/versions.tf` | Declares Terraform `>= 1.5.0` and `hashicorp/google` / `hashicorp/google-beta` `~> 5.0` provider constraints. |
| `infra/udp/cloud_build.yaml` | Standalone UDP Cloud Build pipeline targeting the non-sensitive staging GCP project `pid-nse-stg-core-apps-k8ti` (`environments/staging.tfvars`) with state prefix `terraform/udp/state`. |
| `infra/udp/environments/{development,staging,production}.tfvars` | Environment-specific `.tfvars` files mapping UDP Terraform variables (`enable_composer = true`, `composer_image_version = "composer-3-airflow-3"`) to Dev (`pid-nse-dev-core-apps-dz09`), Staging (`pid-nse-stg-core-apps-k8ti`), and Prod (`pid-nse-prd-core-apps-6aw8`). |

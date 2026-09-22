# Elementl Power — Unified Data Platform (UDP) Ingestion & Infrastructure Repository

This repository houses the **Elementl Unified Data Platform (UDP)** Ingestion Framework (`apps/udp/`) and its **100% Terraform-managed Google Cloud Infrastructure (`infra/udp/`)** across the consolidated Google Cloud environments:

* **Development:** `pid-nse-dev-core-apps-dz09`
* **Staging:** `pid-nse-stg-core-apps-k8ti`
* **Production:** `pid-nse-prd-core-apps-6aw8`

All GCP resources—including the Docker Artifact Registry (`udp-ingestion-jobs`), the 4 Cloud Run v2 Jobs, the Cloud Composer 3 Medium environment (running **Composer 3** and **Apache Airflow 3**), the 10 BigQuery Medallion datasets (`ds_*`), the 26 Bronze & Operational BigQuery tables, the 4 GCS buckets (`bkt-*`), the 11 Secret Manager secrets, and the GCP Dataform repository—are declared and provisioned **purely via Terraform** in `infra/udp/`.

---

## 1. Repository Top-Level Architecture

```text
.
├── README.md                  # Repository architecture and file-by-file reference guide
├── requirements.txt           # Root Python dependencies for local development & CI testing
├── apps/
│   └── udp/                   # Unified Data Platform Application Layer (Cloud Run Jobs, Composer 3 / Airflow 3 DAGs, YAML Configs, Schemas, Docs, Tests)
└── infra/
    ├── cloudbuild.yaml        # Root Cloud Build Terraform validation & plan pipeline (validates infra/ and infra/udp/)
    └── udp/                   # Self-contained UDP Terraform Infrastructure Module (Artifact Registry, Cloud Run Jobs, Composer 3 Medium + Airflow 3, BigQuery, GCS, Dataform)
```

---

## 2. Detailed Purpose of Every Folder & File

### 2.1 UDP Application Layer (`apps/udp/`)

#### Cloud Run Ingestion Jobs (`apps/udp/cloud-run-jobs/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/cloud-run-jobs/cloudbuild.yaml` | Unified Cloud Build pipeline that builds and pushes all 4 Docker images (`arcgis-ingestion`, `bulk-ingestion`, `suiteql-ingestion`, `jdbc-ingestion`) to the Terraform-managed Artifact Registry (`${_REGION}-docker.pkg.dev/${PROJECT_ID}/udp-ingestion-jobs/<job-name>:latest`). |
| `apps/udp/cloud-run-jobs/common/__init__.py` | Package initializer for the shared Cloud Run Job extraction core (`common`). |
| `apps/udp/cloud-run-jobs/common/README.md` | Developer guide for the shared extraction classes (`BaseExtractor`, `JobConfig`, `run_ingestion_job`). |
| `apps/udp/cloud-run-jobs/common/base_extractor.py` | Abstract base class (`BaseExtractor`) implementing GCS staging uploads (`.parquet`, `.jsonl`, `.csv`, `same_as_origin`), temporal date/timestamp coercion, Secret Manager PEM resolution, and BigQuery `ds_operations.audit_ingestion_runs` telemetry logging. |
| `apps/udp/cloud-run-jobs/common/job_config.py` | Environment variable parser and validator (`JobConfig`) that loads runtime parameters injected by Cloud Composer (`SOURCE_NAME`, `TABLE_NAME`, `QUERY`, `GCS_URI`, `TARGET_DATASET`, `TARGET_TABLE`) and blocks unrendered Jinja templates. |
| `apps/udp/cloud-run-jobs/common/job_runner.py` | Standardized execution entrypoint (`run_ingestion_job`) managing structured JSON logging, error handling, and exit codes across all 4 Cloud Run Jobs. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/main.py` | **Oracle Primavera P6 (`ELEMENTL_PMDB_SBOX_PXRPTUSER`)** JDBC/python-oracledb Thin Mode TCPS (port `2484`) ingestion worker supporting full and incremental (`last_recalc_date` / `update_date`) extraction for the 15 P6 tables. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/Dockerfile` | Container image definition for `jdbc-ingestion` (`us-central1-docker.pkg.dev/<project>/udp-ingestion-jobs/jdbc-ingestion:latest`). |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/cloud_build.yaml` | Standalone Cloud Build configuration for building, pushing, and updating the `jdbc-ingestion` Cloud Run Job. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/requirements.txt` | Python dependencies (`oracledb`, `pyarrow`, `pandas`, `google-cloud-storage`, `google-cloud-bigquery`, `google-cloud-secret-manager`) for `jdbc-ingestion`. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/main.py` | **Oracle NetSuite (`SuiteQL`)** REST API ingestion worker implementing OAuth 2.0 Client Credentials (M2M) `PS256` / `ES256` JWT assertion signing via `secret-netsuite-private-key`, pagination (`limit=1000`), HATEOAS link stripping, and GCS Parquet/JSONL output for the 11 NetSuite tables. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/Dockerfile` | Container image definition for `suiteql-ingestion` (`us-central1-docker.pkg.dev/<project>/udp-ingestion-jobs/suiteql-ingestion:latest`). |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/cloud_build.yaml` | Standalone Cloud Build configuration for building, pushing, and updating the `suiteql-ingestion` Cloud Run Job. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/requirements.txt` | Python dependencies (`PyJWT`, `cryptography`, `requests`, `pyarrow`, `pandas`, `google-cloud-*`) for `suiteql-ingestion`. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/main.py` | **Project Atlas ArcGIS FeatureServer / MapServer** REST ingestion worker extracting GeoJSON features into the standardized BigQuery Bronze Envelope Schema (`feature_id`, `properties` JSON, `geometry` GEOGRAPHY, `ingested_at`). |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/clients/arcgis_client.py` | Reusable ArcGIS REST client handling `resultOffset` pagination, spatial reference projection (`outSR=4326`), Esri JSON to GeoJSON conversion, and exponential backoff retries. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/clients/auth.py` | Token authentication handler for ArcGIS Online / Enterprise portals and public Federal GIS endpoints. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/Dockerfile` | Container image definition for `arcgis-ingestion` (`us-central1-docker.pkg.dev/<project>/udp-ingestion-jobs/arcgis-ingestion:latest`). |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/cloud_build.yaml` | Standalone Cloud Build configuration for building, pushing, and updating the `arcgis-ingestion` Cloud Run Job. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/requirements.txt` | Python dependencies (`requests`, `shapely`, `pyarrow`, `google-cloud-*`) for `arcgis-ingestion`. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/main.py` | **Project Atlas Bulk File & Tabular API** ingestion worker supporting HTTP ZIP/Shapefile/GeoPackage downloads, US Census API JSON matrices (`S2-20`), and USGS NWIS (`S1-01`) tabular feeds. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/clients/auth.py` | API key and bearer token authentication helper for external bulk data portals (Census, USGS, EIA). |
| `apps/udp/cloud-run-jobs/bulk-ingestion/Dockerfile` | Container image definition matching GDAL/Fiona/Geopandas system libraries for `bulk-ingestion` (`us-central1-docker.pkg.dev/<project>/udp-ingestion-jobs/bulk-ingestion:latest`). |
| `apps/udp/cloud-run-jobs/bulk-ingestion/cloud_build.yaml` | Standalone Cloud Build configuration for building, pushing, and updating the `bulk-ingestion` Cloud Run Job. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/requirements.txt` | Python dependencies (`geopandas`, `fiona`, `shapely`, `pyarrow`, `requests`, `google-cloud-*`) for `bulk-ingestion`. |

#### Cloud Composer 3 & Airflow 3 Orchestration (`apps/udp/composer/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/composer/dags/udp_dag_factory.py` | Dynamic Apache Airflow 3 DAG Factory that scans `apps/udp/configs/sources/{p6,netsuite,atlas}/*.yaml` and generates a 4-stage pipeline per source: (1) `CloudRunExecuteJobOperator` extraction to GCS (`bkt-<project>-udp-landing-staging`), (2) `GCSToBigQueryOperator` load to Bronze (`ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`), (3) Dataform compilation & invocation (`ds_silver_p6`, `ds_silver_netsuite`, `ds_silver_atlas`, `ds_gold`), and (4) `GCSObjectMoveOperator` archival to `bkt-<project>-udp-bronze-archive`. |
| `apps/udp/composer/dags/samples/dag_udp_p6_project.py` | Sample reference DAG demonstrating end-to-end orchestration for Oracle Primavera P6 `PROJECT` (`ELEMENTL_PMDB_SBOX_PXRPTUSER.PROJECT` -> `ds_bronze_p6.project` -> `ds_silver_p6.stg_p6_project`). |
| `apps/udp/composer/dags/samples/dag_udp_netsuite_department.py` | Sample reference DAG demonstrating end-to-end orchestration for Oracle NetSuite `department` (`ds_bronze_netsuite.department` -> `ds_silver_netsuite.stg_netsuite_department`). |
| `apps/udp/composer/dags/samples/dag_udp_atlas_s1_02_urban_areas.py` | Sample reference DAG demonstrating end-to-end orchestration for Atlas `S1-02` Urban Areas (`ds_bronze_atlas.s1_02_urban_areas` -> `ds_silver_atlas.stg_atlas_s1_02_urban_areas`). |
| `apps/udp/composer/dags/samples/dag_udp_sample_atlas_same_as_origin.py` | Sample reference DAG demonstrating zero-conversion passthrough (`same_as_origin`) ingestion and landing for Atlas datasets. |

#### Declarative Source YAMLs & Schemas (`apps/udp/configs/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/configs/sources/_template_source_reference.yaml` | Annotated onboarding blueprint showing all supported YAML keys for adding a new table in <5 minutes without modifying Python code. |
| `apps/udp/configs/sources/p6/*.yaml` (17 files) | Declarative source definitions for the 15 in-scope Oracle Primavera P6 tables in `ELEMENTL_PMDB_SBOX_PXRPTUSER` (`p6_project.yaml`, `p6_wbs.yaml`, `p6_wbscategory.yaml`, `p6_activity.yaml`, `p6_udfvalue.yaml`, `p6_udftype.yaml`, `p6_wbsspread.yaml`, `p6_activityspread.yaml`, `p6_epsspread.yaml`, `p6_resourceassignmentspread.yaml`, `p6_projectspread.yaml`, `p6_refrdelete.yaml`, `p6_activitycode.yaml`, `p6_activitycodetype.yaml`, `p6_activitycodeassignment.yaml`) plus 2 format validation samples (`sample_p6_csv.yaml`, `sample_p6_json.yaml`). |
| `apps/udp/configs/sources/netsuite/*.yaml` (13 files) | Declarative SuiteQL source definitions for the 11 in-scope Oracle NetSuite tables (`netsuite_account.yaml`, `netsuite_budgets.yaml`, `netsuite_classification.yaml`, `netsuite_customer.yaml`, `netsuite_department.yaml`, `netsuite_entity.yaml`, `netsuite_location.yaml`, `netsuite_subsidiary.yaml`, `netsuite_transaction.yaml`, `netsuite_transactionline.yaml`, `netsuite_vendor.yaml`) plus 2 format validation samples (`sample_netsuite_csv.yaml`, `sample_netsuite_json.yaml`). |
| `apps/udp/configs/sources/atlas/*.yaml` (7 files) | Declarative source definitions for the 4 in-scope Atlas layers (`atlas_s1_01_population_density.yaml`, `atlas_s1_02_urban_areas.yaml`, `atlas_s1_06_quaternary_faults.yaml`, `atlas_s2_20_cooling_water_supply.yaml`) plus 3 format validation samples (`sample_atlas_csv.yaml`, `sample_atlas_parquet.yaml`, `sample_atlas_same_as_origin.yaml`). |
| `apps/udp/configs/schema/p6/*.json` (15 files) | BigQuery Bronze JSON column schemas (`p6_*_schema.json`) read directly by `infra/udp/bigquery_datasets.tf` (`google_bigquery_table.bronze_p6_tables`) and the DAG Factory. |
| `apps/udp/configs/schema/netsuite/*.json` (11 files) | BigQuery Bronze JSON column schemas (`netsuite_*_schema.json`) read directly by `infra/udp/bigquery_datasets.tf` (`google_bigquery_table.bronze_netsuite_tables`) and the DAG Factory. |

#### Environment Overrides, Docs, Tests & Utilities (`apps/udp/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/environments/{dev,staging,production}.json` | Airflow Variable JSON bundles defining project IDs, region, GCS bucket names, BigQuery dataset mappings, and Dataform repository IDs per environment. |
| `apps/udp/docs/**` | Architecture reference documentation (`architecture_and_flows.md`, `index.html`, `unit_testing.md`, `integration_testing.md`) and system diagrams (`assets/diagrams/*`). |
| `apps/udp/tests/unit/*.py` | Unit test suite (`26 passed`) validating `BaseExtractor`, `BulkExtractor`, `JDBCExtractor`, `SuiteQLExtractor`, `JobConfig`, and `udp_dag_factory.py`. |
| `apps/udp/tests/integration/*.py` | Live GCP integration test suite verifying BigQuery datasets, GCS landing buckets, Cloud Run Job definitions, Composer DAG parsing, and `ds_operations` audit logging. |
| `apps/udp/utilities/table_onboarding.py` | CLI generator tool that scaffolds a new YAML source definition and BigQuery JSON schema file for rapid table onboarding. |
| `apps/udp/utilities/test_udp_end_to_end.py` | End-to-end verification utility for triggering and validating a complete Bronze -> Silver -> Gold run across P6, NetSuite, and Atlas. |

---

### 2.2 Terraform Infrastructure Layer (`infra/udp/`)

| Path | Purpose |
| :--- | :--- |
| `infra/udp/apis.tf` | Enables all required Google Cloud APIs (`artifactregistry`, `run`, `cloudbuild`, `composer`, `bigquery`, `dataform`, `secretmanager`, `storage`, `iam`). |
| `infra/udp/artifact_registry.tf` | Provisions the Docker Artifact Registry repository `udp-ingestion-jobs` (`${var.region}-docker.pkg.dev/${var.project_id}/udp-ingestion-jobs`), image retention cleanup policies, and repository-level `roles/artifactregistry.reader` / `roles/artifactregistry.writer` IAM bindings for the 4 Cloud Run Jobs and ingestion platform SA. |
| `infra/udp/cloud_run_jobs.tf` | Provisions the 4 Cloud Run v2 Jobs (`arcgis-ingestion`, `bulk-ingestion`, `jdbc-ingestion`, `suiteql-ingestion`) pointing to `${var.region}-docker.pkg.dev/${var.project_id}/udp-ingestion-jobs/<job-name>:${var.image_tag}`, with dedicated runtime service accounts (`sa-*`), Direct VPC Egress (`sub-ns-stg-usc1`, NAT IP `136.115.148.145`), Composer `roles/run.invoker` IAM, and resource-level Secret Manager & BigQuery dataset IAM bindings. |
| `infra/udp/composer.tf` | Provisions the **Cloud Composer 3 Medium Environment** (`environment_size = "ENVIRONMENT_SIZE_MEDIUM"`, `image_version = "composer-3-airflow-3"` running **Cloud Composer 3** and **Apache Airflow 3**), Medium `workloads_config`, Airflow environment variables, the 3 platform service accounts (`gcp-sa-nsedusc1-data-ingest`, `gcp-sa-nsedusc1-data-transform`, `gcp-sa-nsedusc1-composer`), and all 11 Secret Manager secrets (`secret-p6-*`, `secret-netsuite-*`). |
| `infra/udp/bigquery_datasets.tf` | Provisions all 10 standardized `ds_*` BigQuery datasets (`ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, `ds_silver_p6`, `ds_silver_netsuite`, `ds_silver_atlas`, `ds_gold`, `ds_dataform_assertions`, `ds_operations`, `ds_atlas_analytics`), all 15 P6 Bronze tables (`google_bigquery_table.bronze_p6_tables`), all 11 NetSuite Bronze tables (`google_bigquery_table.bronze_netsuite_tables`), and the operational audit table (`ds_operations.audit_ingestion_runs`) purely via Terraform. |
| `infra/udp/gcs_buckets.tf` | Provisions the 4 UDP Cloud Storage buckets (`bkt-${var.project_id}-udp-landing-staging`, `bkt-${var.project_id}-udp-dataflow-temp`, `bkt-${var.project_id}-udp-bronze-raw`, `bkt-${var.project_id}-udp-bronze-archive`) with uniform bucket-level access, 90-day Archive lifecycle rules, and IAM bindings. |
| `infra/udp/dataform.tf` | Provisions the GCP Dataform repository (`df-${var.project_id}-udp-transformations`) in `us-central1` and binds `gcp-sa-nsedusc1-data-transform` to the Bronze, Silver, Gold, and Assertions datasets. |
| `infra/udp/backend.tf` | Configures the GCS remote state backend (`prefix = "terraform/udp/state"`). |
| `infra/udp/variables.tf` | Defines input variables (`project_id`, `region`, `environment`, `vpc_network`, `vpc_subnet`, `artifact_registry_repo_id`, `image_tag`, `enable_composer`, `composer_image_version`). |
| `infra/udp/outputs.tf` | Exports `artifact_registry_repository_url`, `cloud_run_job_names`, `cloud_run_job_images`, `bigquery_dataset_ids`, `gcs_bucket_names`, and `dataform_repository_id`. |
| `infra/udp/versions.tf` | Declares Terraform `>= 1.5.0` and `hashicorp/google` / `hashicorp/google-beta` `~> 5.0` provider constraints. |
| `infra/udp/cloud_build.yaml` | CI/CD Cloud Build pipeline for running `terraform init`, `terraform validate`, and `terraform plan` inside `infra/udp/`. |
| `infra/udp/environments/{development,staging,production}.tfvars` | Environment-specific `.tfvars` files mapping UDP Terraform variables (`enable_composer = true`, `composer_image_version = "composer-3-airflow-3"`, `artifact_registry_repo_id = "udp-ingestion-jobs"`) to Dev (`pid-nse-dev-core-apps-dz09`), Staging (`pid-nse-stg-core-apps-k8ti`), and Prod (`pid-nse-prd-core-apps-6aw8`). |

---

## 3. Detailed Explanation of Modified & Standardized Files

1. **`infra/udp/artifact_registry.tf` (Added inside `infra/udp/`)**
   * Moved the Docker Artifact Registry definition into `infra/udp/artifact_registry.tf` so `infra/udp/` is 100% self-contained.
   * Provisions `google_artifact_registry_repository.udp_ingestion_repo` (`repository_id = "udp-ingestion-jobs"`, `format = "DOCKER"`) to host the 4 container images built from `apps/udp/cloud-run-jobs/` (`arcgis-ingestion`, `bulk-ingestion`, `jdbc-ingestion`, `suiteql-ingestion`).
   * Adds repository-level IAM bindings granting `roles/artifactregistry.reader` to each Cloud Run Job's runtime service account (`google_service_account.udp_job_sa`) and `roles/artifactregistry.writer` to `gcp-sa-nsedusc1-data-ingest`.

2. **`infra/udp/cloud_run_jobs.tf` (Updated Image URIs & Composer Invoker IAM)**
   * Replaced the placeholder `us-docker.pkg.dev/cloudrun/container/job:latest` image with the exact Artifact Registry Docker image URI for each worker:
     `${var.region}-docker.pkg.dev/${var.project_id}/udp-ingestion-jobs/${each.key}:${var.image_tag}`.
   * Added `google_cloud_run_v2_job_iam_member.composer_run_invoker` granting `roles/run.invoker` to `gcp-sa-nsedusc1-composer` so Cloud Composer 3 DAGs can trigger all 4 Cloud Run Jobs.
   * Removed legacy `raw_p6`, `raw_netsuite`, and `raw_atlas` IAM bindings so permissions bind strictly to `ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, and `ds_operations`.

3. **`infra/udp/composer.tf` (Upgraded to Medium Instance + Composer 3 & Airflow 3)**
   * Upgraded `environment_size` from `ENVIRONMENT_SIZE_SMALL` to **`ENVIRONMENT_SIZE_MEDIUM`**.
   * Upgraded `image_version` from `composer-3-airflow-2.9.3` to **`composer-3-airflow-3`** (Cloud Composer 3 with **Apache Airflow 3**), parameterized via `var.composer_image_version`.
   * Added `workloads_config` tuned for a Medium Composer 3 environment (`scheduler`: 2 vCPU / 4 GB RAM x 2; `web_server`: 2 vCPU / 4 GB RAM; `worker`: 2 vCPU / 8 GB RAM, 2–6 workers; `triggerer`: 1 vCPU / 2 GB RAM x 2) and injected runtime Airflow environment variables (`GCP_PROJECT_ID`, `RAW_BUCKET_NAME`, `STAGING_BUCKET`, `ARCHIVE_BUCKET`, `P6_SCHEMA`, `DATAFORM_REPOSITORY_ID`, `ARTIFACT_REGISTRY_URI`).

4. **`infra/udp/bigquery_datasets.tf` (100% Terraform-Managed Datasets & Bronze/Audit Tables)**
   * Removed the ad-hoc `bootstrap_bq_tables.sh` script and legacy `raw_*` datasets.
   * Declared all 10 standardized `ds_*` datasets (`ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, `ds_silver_p6`, `ds_silver_netsuite`, `ds_silver_atlas`, `ds_gold`, `ds_dataform_assertions`, `ds_operations`, `ds_atlas_analytics`) and added `google_bigquery_table` resources that dynamically load the 15 P6 schemas (`apps/udp/configs/schema/p6/p6_*_schema.json`), 11 NetSuite schemas (`apps/udp/configs/schema/netsuite/netsuite_*_schema.json`), and `ds_operations.audit_ingestion_runs` directly via Terraform.

5. **`apps/udp/cloud-run-jobs/cloudbuild.yaml` (Added 4th Worker Image & Standardized Substitutions)**
   * Added the missing `bulk-ingestion` Docker build step alongside `arcgis-ingestion`, `suiteql-ingestion`, and `jdbc-ingestion`.
   * Replaced hardcoded project references (`elementl-509009`) with parameterized `${_REGION}-docker.pkg.dev/${PROJECT_ID}/${_REPOSITORY}/<job-name>:${_TAG}` targeting the Terraform-managed `udp-ingestion-jobs` Artifact Registry.

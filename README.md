# Elementl Unified Data Platform (UDP) — Application & Infrastructure Repository

This repository implements the **Elementl Phase 2 Unified Data Platform (UDP)** on Google Cloud (`pid-nse-stg-core-apps-k8ti`), providing metadata-driven data ingestion and orchestration across **Oracle Primavera P6 (`ELEMENTL_PMDB_SBOX_PXRPTUSER`, 15 tables)**, **Oracle NetSuite (`SuiteQL`, 11 tables)**, and **Atlas (`ArcGIS` & `Bulk`, 4 GIS/tabular layers)** into a BigQuery Medallion Architecture (`Bronze -> Silver -> Gold`) orchestrated by **Cloud Composer 3**, **Cloud Run v2 Jobs**, and **Dataform**.

All files in this repository adhere to the Google Cloud Professional Services (PSO) code delivery guidelines (`go/pso-code-guidance`).

---

## High-Level Repository Layout

The repository is organized strictly into two domain layers:

```text
.
├── README.md                  # Complete repository guide & file-by-file reference
├── .gitignore                 # Git ignore rules for Python, Terraform, and local env files
├── requirements.txt           # Root Python dependencies for local testing & CLI utilities
├── apps/
│   └── udp/                   # Unified Data Platform Application Layer (Cloud Run Jobs, Composer DAGs, YAML Configs, Schemas, Docs, Tests)
└── infra/
    ├── *.tf / cloudbuild.yaml # Root GCP project Terraform foundation & Cloud Build CI/CD pipeline
    └── udp/                   # UDP Terraform Infrastructure Definitions (BigQuery, GCS, Cloud Run Jobs, Secrets, Composer, Dataform)
```

---

## Purpose of Every Folder and File

### 1. Root Files

| Path | Purpose |
| :--- | :--- |
| `README.md` | Master architectural overview and file-by-file directory reference for the entire repository. |
| `.gitignore` | Excludes Python bytecode (`__pycache__/`, `*.pyc`), virtual environments (`.venv/`), Terraform working caches (`.terraform/`, `*.tfstate`), and local secret files (`.env*`). |
| `requirements.txt` | Root Python dependencies for running unit/integration tests (`pytest`), Airflow DAG parsing (`apache-airflow`), and Cloud Run extraction clients (`oracledb`, `PyJWT`, `cryptography`, `requests`, `google-cloud-bigquery`, `google-cloud-storage`, `google-cloud-secret-manager`, `pyarrow`, `pyyaml`). |

---

### 2. Application Layer (`apps/udp/`)

Contains the complete Unified Data Platform application codebase, including the 4 containerized Cloud Run Jobs, the dynamic Cloud Composer 3 DAG factory, the 30 declarative source YAML configurations, BigQuery JSON schemas, architecture documentation, and unit/integration test suites.

#### 2.1 Environment Configurations (`apps/udp/environments/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/README.md` | Quick-start guide for the `apps/udp/` application module, test suites, and runtime commands. |
| `apps/udp/environments/dev.json` | Runtime environment configuration for Development (`pid-nse-dev-core-apps-dz09`). |
| `apps/udp/environments/staging.json` | Runtime environment configuration for Staging/QA (`pid-nse-stg-core-apps-k8ti`), defining `bkt-pid-nse-stg-core-apps-k8ti-udp-landing-staging`, `ds_bronze_*`, `ds_silver_*`, `ds_gold`, `ds_operations`, and partitioned service accounts. |
| `apps/udp/environments/production.json` | Runtime environment configuration for Production (`pid-nse-prd-core-apps-6aw8`). |

#### 2.2 Containerized Extraction Workers (`apps/udp/cloud-run-jobs/`)

External source extraction is isolated from Airflow into 4 dedicated Cloud Run v2 Jobs running inside Shared VPC subnet `sub-ns-stg-usc1` (egressing via Cloud NAT static IP `136.115.148.145`).

| Path | Purpose |
| :--- | :--- |
| `apps/udp/cloud-run-jobs/cloudbuild.yaml` | Multi-stage Cloud Build configuration that builds and pushes all 4 Cloud Run Job container images (`jdbc-ingestion`, `suiteql-ingestion`, `arcgis-ingestion`, `bulk-ingestion`) to Artifact Registry. |
| **`apps/udp/cloud-run-jobs/common/`** | **Shared Python extraction framework used across all 4 Cloud Run Jobs:** |
| `apps/udp/cloud-run-jobs/common/README.md` | Developer documentation for the shared extraction base classes and audit telemetry. |
| `apps/udp/cloud-run-jobs/common/__init__.py` | Package initializer exporting `BaseExtractor`, `JobConfig`, and `JobRunner`. |
| `apps/udp/cloud-run-jobs/common/base_extractor.py` | Abstract `BaseExtractor` implementing dual Bronze persistence (raw JSONL/Parquet writes to GCS `bkt-*-udp-landing-staging` + native BigQuery loads into `ds_bronze_*`), high-watermark state lookup, and run-level audit logging into `ds_operations.audit_ingestion_runs`. |
| `apps/udp/cloud-run-jobs/common/job_config.py` | `JobConfig` parser that loads entity YAML configurations, environment JSONs, CLI arguments (`--source-system`, `--entity-name`), and GCP Secret Manager payloads. |
| `apps/udp/cloud-run-jobs/common/job_runner.py` | `JobRunner` execution harness providing structured JSON logging, retry handling, execution timing, and non-zero failure exit codes for Airflow task monitoring. |
| **`apps/udp/cloud-run-jobs/jdbc-ingestion/`** | **Oracle Primavera P6 (`ELEMENTL_PMDB_SBOX_PXRPTUSER`) Extraction Worker (`crj-*-jdbc-ingestion`):** |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/main.py` | Connects to AWS RDS Oracle 19c (`db-ora-prd-01.ctk6fk2blyyo.us-gov-west-1.rds.amazonaws.com:2484/orcl`) over TCPS (TLS 1.2) using `oracledb` thin mode and Secret Manager (`secret-p6-db-config`, `secret-p6-db-ca-bundle`), extracting the 15 in-scope P6 tables from `ELEMENTL_PMDB_SBOX_PXRPTUSER` into GCS and BigQuery `ds_bronze_p6`. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/Dockerfile` | Container image definition for the P6 `jdbc-ingestion` worker. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/cloud_build.yaml` | Standalone Cloud Build specification for `jdbc-ingestion`. |
| `apps/udp/cloud-run-jobs/jdbc-ingestion/requirements.txt` | Python dependencies (`oracledb`, `google-cloud-bigquery`, `google-cloud-storage`, `google-cloud-secret-manager`, `pyarrow`). |
| **`apps/udp/cloud-run-jobs/suiteQL-ingestion/`** | **Oracle NetSuite (`SuiteQL`) Extraction Worker (`crj-*-suiteql-ingestion`):** |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/main.py` | Authenticates with Oracle NetSuite REST Web Services using OAuth 2.0 Client Credentials M2M JWT (`PS256`/`RS256` signed with the private key from the 5 NetSuite secrets in Secret Manager), executes paginated SuiteQL queries (`POST /services/rest/query/v1/suiteql`) for the 11 in-scope NetSuite tables, and lands data into GCS and BigQuery `ds_bronze_netsuite`. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/Dockerfile` | Container image definition for the NetSuite `suiteQL-ingestion` worker. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/cloud_build.yaml` | Standalone Cloud Build specification for `suiteQL-ingestion`. |
| `apps/udp/cloud-run-jobs/suiteQL-ingestion/requirements.txt` | Python dependencies (`PyJWT`, `cryptography`, `requests`, `google-cloud-bigquery`, `google-cloud-storage`, `google-cloud-secret-manager`). |
| **`apps/udp/cloud-run-jobs/arcgis-ingestion/`** | **Atlas GIS (`ArcGIS REST`) Extraction Worker (`crj-*-arcgis-ingestion`):** |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/main.py` | Extracts spatial vector layers (`S1-02` Urban Areas, `S1-06` Quaternary Faults) from ArcGIS FeatureServer/MapServer endpoints into GCS and BigQuery `ds_bronze_atlas`. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/clients/__init__.py` | ArcGIS client subpackage initializer. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/clients/arcgis_client.py` | Paginated ArcGIS REST client supporting `resultOffset`/`resultRecordCount`, geometry serialization (WKT/GeoJSON), and spatial reference (`EPSG:4326`) standardization. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/clients/auth.py` | Authentication handler for token-protected ArcGIS endpoints. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/Dockerfile` | Container image definition for the `arcgis-ingestion` worker. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/cloud_build.yaml` | Standalone Cloud Build specification for `arcgis-ingestion`. |
| `apps/udp/cloud-run-jobs/arcgis-ingestion/requirements.txt` | Python dependencies for GIS/ArcGIS extraction. |
| **`apps/udp/cloud-run-jobs/bulk-ingestion/`** | **Atlas Bulk / Archive (`CSV/ZIP/Shapefile`) Extraction Worker (`crj-*-bulk-ingestion`):** |
| `apps/udp/cloud-run-jobs/bulk-ingestion/main.py` | Downloads bulk tabular and shapefile/ZIP archives (`S1-01` Population Density, `S2-20` Cooling Water Supply), unpacks archive contents into GCS `bkt-*-udp-landing-staging`, and loads Bronze envelope tables into BigQuery `ds_bronze_atlas`. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/clients/__init__.py` | Bulk client subpackage initializer. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/clients/auth.py` | HTTP session and header authentication handler for bulk data feeds. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/Dockerfile` | Container image definition for the `bulk-ingestion` worker. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/cloud_build.yaml` | Standalone Cloud Build specification for `bulk-ingestion`. |
| `apps/udp/cloud-run-jobs/bulk-ingestion/requirements.txt` | Python dependencies for bulk HTTP and archive extraction. |

#### 2.3 Cloud Composer 3 Orchestration (`apps/udp/composer/dags/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/composer/dags/udp_dag_factory.py` | Dynamic Airflow DAG Factory that scans `apps/udp/configs/sources/**/*.yaml` at parse time and constructs a standardized 2-TaskGroup DAG per entity: **TaskGroup 1 (`ingestion_group`)** triggers the corresponding Cloud Run Job (`CloudRunExecuteJobOperator`) and lands GCS/BigQuery Bronze (`ds_bronze_*`); **TaskGroup 2 (`dataform_transformation_group`)** compiles and executes the Dataform Silver SQLX model (`ds_silver_*`) and assertions (`ds_dataform_assertions`). |
| `apps/udp/composer/dags/samples/dag_udp_p6_project.py` | Sample reference DAG demonstrating end-to-end orchestration for Oracle Primavera P6 `PROJECT` (`ELEMENTL_PMDB_SBOX_PXRPTUSER.PROJECT` -> `ds_bronze_p6.project` -> `ds_silver_p6.stg_p6_project`). |
| `apps/udp/composer/dags/samples/dag_udp_netsuite_department.py` | Sample reference DAG demonstrating end-to-end orchestration for Oracle NetSuite `department` (`ds_bronze_netsuite.department` -> `ds_silver_netsuite.stg_netsuite_department`). |
| `apps/udp/composer/dags/samples/dag_udp_atlas_s1_02_urban_areas.py` | Sample reference DAG demonstrating end-to-end orchestration for Atlas `S1-02` Urban Areas (`ds_bronze_atlas.s1_02_urban_areas` -> `ds_silver_atlas.stg_atlas_s1_02_urban_areas`). |
| `apps/udp/composer/dags/samples/dag_udp_sample_atlas_same_as_origin.py` | Sample reference DAG demonstrating zero-conversion passthrough (`same_as_origin`) ingestion and landing for Atlas datasets. |

#### 2.4 Declarative Source YAMLs & Schemas (`apps/udp/configs/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/configs/sources/_template_source_reference.yaml` | Annotated onboarding blueprint showing all supported YAML keys for adding a new table in <5 minutes without modifying Python code. |
| `apps/udp/configs/sources/p6/*.yaml` (17 files) | Declarative source definitions for the 15 in-scope Oracle Primavera P6 tables in `ELEMENTL_PMDB_SBOX_PXRPTUSER` (`p6_project.yaml`, `p6_wbs.yaml`, `p6_wbscategory.yaml`, `p6_activity.yaml`, `p6_udfvalue.yaml`, `p6_udftype.yaml`, `p6_wbsspread.yaml`, `p6_activityspread.yaml`, `p6_epsspread.yaml`, `p6_resourceassignmentspread.yaml`, `p6_projectspread.yaml`, `p6_refrdelete.yaml`, `p6_activitycode.yaml`, `p6_activitycodetype.yaml`, `p6_activitycodeassignment.yaml`) plus 2 format validation samples (`sample_p6_csv.yaml`, `sample_p6_json.yaml`). |
| `apps/udp/configs/sources/netsuite/*.yaml` (13 files) | Declarative SuiteQL source definitions for the 11 in-scope Oracle NetSuite tables (`netsuite_account.yaml`, `netsuite_budgets.yaml`, `netsuite_classification.yaml`, `netsuite_customer.yaml`, `netsuite_department.yaml`, `netsuite_entity.yaml`, `netsuite_location.yaml`, `netsuite_subsidiary.yaml`, `netsuite_transaction.yaml`, `netsuite_transactionline.yaml`, `netsuite_vendor.yaml`) plus 2 format validation samples (`sample_netsuite_csv.yaml`, `sample_netsuite_json.yaml`). |
| `apps/udp/configs/sources/atlas/*.yaml` (7 files) | Declarative source definitions for the 4 in-scope Atlas layers (`atlas_s1_01_population_density.yaml`, `atlas_s1_02_urban_areas.yaml`, `atlas_s1_06_quaternary_faults.yaml`, `atlas_s2_20_cooling_water_supply.yaml`) plus 3 format validation samples (`sample_atlas_csv.yaml`, `sample_atlas_parquet.yaml`, `sample_atlas_same_as_origin.yaml`). |
| `apps/udp/configs/schema/p6/*.json` (15 files) | BigQuery Bronze JSON column schemas (`p6_*_schema.json`) corresponding to the 15 P6 `ELEMENTL_PMDB_SBOX_PXRPTUSER` tables. |
| `apps/udp/configs/schema/netsuite/*.json` (11 files) | BigQuery Bronze JSON column schemas (`netsuite_*_schema.json`) corresponding to the 11 NetSuite SuiteQL tables. |

#### 2.5 Documentation, Tests, and CLI Utilities (`apps/udp/docs/`, `apps/udp/tests/`, `apps/udp/utilities/`)

| Path | Purpose |
| :--- | :--- |
| `apps/udp/docs/README.md` | Index of architecture, sequence diagrams, and testing documentation. |
| `apps/udp/docs/architecture_and_flows.md` & `.html` | Comprehensive UDP architecture specification covering Medallion storage (`ds_bronze_*`, `ds_silver_*`, `ds_gold`), network egress (`136.115.148.145`), and execution sequence flows. |
| `apps/udp/docs/unit_testing.md` | Guide for running hermetic unit tests with mock GCP/database clients. |
| `apps/udp/docs/integration_testing.md` | Guide for executing live GCP integration validation against BigQuery, GCS, Cloud Run Jobs, and Composer. |
| `apps/udp/docs/index.html`, `_reference_template.html`, `build_reference.py` | HTML documentation builder and interactive visual reference portal. |
| `apps/udp/docs/assets/` | Architecture diagrams (`01_medallion_architecture.png` through `09_schema_evolution_architecture.png`, SVG diagrams, and `mermaid.min.js`). |
| `apps/udp/tests/README.md`, `pytest.ini`, `conftest.py`, `findings.md` | Pytest configuration, shared test fixtures, and Composer v2/v3 validation report. |
| `apps/udp/tests/unit/test_*.py` (6 files) | Unit tests for `BaseExtractor`, `JobConfig`, `jdbc-ingestion`, `suiteQL-ingestion`, `bulk-ingestion`, and `udp_dag_factory.py`. |
| `apps/udp/tests/integration/test_*.py` (5 files) | Live GCP integration tests verifying BigQuery datasets (`ds_*`), GCS landing buckets, Cloud Run v2 Jobs, Composer DAGs, and operational audit logging. |
| `apps/udp/utilities/table_onboarding.py` | CLI generator that scaffolds a new source YAML and BigQuery schema JSON for rapid table onboarding. |
| `apps/udp/utilities/test_udp_end_to_end.py` | End-to-end validation script verifying extraction -> GCS staging -> BigQuery Bronze -> Dataform Silver. |
| `apps/udp/utilities/bootstrap_bq_tables.sh` | Idempotent `bq` CLI bootstrap script that creates and populates all 10 standardized `ds_*` BigQuery datasets and Bronze/Silver/Gold/Audit tables in `pid-nse-stg-core-apps-k8ti`. |

---

### 3. Terraform Infrastructure Layer (`infra/udp/` and `infra/`)

Provisions all Google Cloud resources in the consolidated project (`pid-nse-stg-core-apps-k8ti`) with dataset-level and bucket-level compensating IAM controls. #### 3.1 UDP Terraform Definitions (`infra/udp/`)

| Path | Purpose |
| :--- | :--- |
| `bigquery_datasets.tf` | Provisions the 10 standardized BigQuery datasets (`ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas`, `ds_silver_p6`, `ds_silver_netsuite`, `ds_silver_atlas`, `ds_gold`, `ds_dataform_assertions`, `ds_operations`, `ds_atlas_analytics`) and enforces dataset-level IAM (`roles/bigquery.dataEditor` / `roles/bigquery.dataViewer` scoped per dataset to `gcp-sa-nsedusc1-data-ingest`, `gcp-sa-nsedusc1-data-transform`, and `gcp-sa-nsedusc1-composer`). |
| `gcs_buckets.tf` | Provisions `bkt-${var.project_id}-udp-landing-staging` (with 90-day Archive object lifecycle) and `bkt-${var.project_id}-udp-dataflow-temp`, plus bucket-level `roles/storage.objectAdmin` / `roles/storage.objectViewer` bindings. |
| `cloud_run_jobs.tf` | Provisions the 4 Cloud Run v2 Jobs (`crj-${var.project_id}-jdbc-ingestion`, `crj-${var.project_id}-suiteql-ingestion`, `crj-${var.project_id}-arcgis-ingestion`, `crj-${var.project_id}-bulk-ingestion`) attached to Shared VPC subnet `sub-ns-stg-usc1` (`PRIVATE_RANGES_ONLY`), and declares the 9 Secret Manager secrets (`secret-p6-db-config`, `secret-p6-db-ca-bundle`, `secret-p6-api-username`, `secret-p6-api-password`, `secret-netsuite-account-id`, `secret-netsuite-consumer-key`, `secret-netsuite-certificate-id`, `secret-netsuite-certificate-private-key`, `secret-netsuite-scope`) with `user_managed` replication in `us-central1`. |
| `composer.tf` | Declaratively uploads and syncs `apps/udp/composer/dags/`, `apps/udp/configs/`, and `apps/udp/environments/` into the Cloud Composer 3 DAGs GCS bucket (`google_storage_bucket_object` with MD5 hash tracking). |
| `dataform.tf` | Provisions the GCP Dataform repository (`df-${var.project_id}-udp-transformations`) in `us-central1` and binds `gcp-sa-nsedusc1-data-transform` and the Dataform service agent to the Bronze, Silver, Gold, and Assertions datasets. |
| `variables.tf` | Declares input variables (`project_id`, `region`, `environment`, `vpc_network`, `vpc_subnetwork`, `composer_dags_bucket`, and service account emails). |
| `backend.tf` | Configures the remote GCS Terraform state backend (`gs://gcs-pid-nse-stg-core-apps-k8ti-tfstate`, prefix `terraform/udp/state`). |
| `versions.tf` | Specifies Terraform (`>= 1.5`) and Google provider (`~> 5.0`) version constraints. |
| `cloud_build.yaml` | Standalone Cloud Build Terraform validation and plan pipeline for `infra/udp/`. |

#### 3.2 Root Project Foundation (`infra/`)

| Path | Purpose |
| :--- | :--- |
| `infra/cloudbuild.yaml` | Primary Cloud Build trigger pipeline that runs `terraform init`, `terraform validate`, and `terraform plan` across both root `infra/` and `infra/udp/`. |
| `infra/apis.tf` | Enables required GCP APIs (`bigquery.googleapis.com`, `run.googleapis.com`, `composer.googleapis.com`, `dataform.googleapis.com`, `secretmanager.googleapis.com`, `artifactregistry.googleapis.com`). |
| `infra/artifact_registry.tf` | Provisions the Docker Artifact Registry repository (`ar-${var.project_id}-udp-images`) in `us-central1`. |
| `infra/service_accounts.tf` & `infra/iam.tf` | Declares baseline project service accounts and least-privilege execution bindings (`roles/bigquery.jobUser`, `roles/run.invoker`). |
| `infra/storage.tf` & `infra/storage_shared.tf` | Provisions foundational shared storage buckets for the core-apps project. |
| `infra/main.tf`, `variables.tf`, `outputs.tf`, `backend.tf`, `versions.tf` | Root Terraform configuration, remote state backend (`gs://gcs-pid-ns-cmn-app-tfstate-iv0b`), and module outputs. |

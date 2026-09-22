# Integration Testing Guide: Elementl Unified Data Platform (UDP)

This guide documents the integration testing architecture, operational workflows, and validation procedures for the Elementl UDP ingestion framework against Google Cloud Platform project `elementl-509009`.

---

## 1. Architecture & Verification Scope

The integration test suite validates that the entire end-to-end data ingestion stack operates seamlessly across Google Cloud resources:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 Google Cloud Platform                                  │
│                               Project: elementl-509009                                 │
│                                                                                        │
│  ┌─────────────────────────────────┐        ┌───────────────────────────────────────┐  │
│  │   Cloud Storage: Configs        │        │   Cloud Composer 2 (Airflow)          │  │
│  │   bkt-*-udp-configs/sources/    │───────▶│   composer-elementl-dev               │  │
│  │   (30 Declarative YAMLs)        │        │   • Dynamic DAG Factory Parser        │  │
│  └─────────────────────────────────┘        │   • 30 Registered Ingestion DAGs      │  │
│                                             └───────────────────┬───────────────────┘  │
│                                                                 │                      │
│                                                                 ▼                      │
│  ┌─────────────────────────────────┐        ┌───────────────────────────────────────┐  │
│  │   Artifact Registry             │        │   Cloud Run Ingestion Jobs            │  │
│  │   udp-ingestion-jobs/*:latest   │───────▶│   • suiteql-ingestion                 │  │
│  │   (Cloud Build Images)          │        │   • jdbc-ingestion                    │  │
│  └─────────────────────────────────┘        │   • arcgis-ingestion                  │  │
│                                             │   • bulk-ingestion                    │  │
│                                             └───────────────────┬───────────────────┘  │
│                                                                 │                      │
│                     ┌───────────────────────────────────────────┴──────────┐           │
│                     ▼                                                      ▼           │
│  ┌─────────────────────────────────────┐         ┌─────────────────────────────────┐   │
│  │   Cloud Storage: Bronze Landing     │         │   BigQuery Datasets             │   │
│  │   bkt-*-udp-bronze-raw              │         │   • ds_bronze_netsuite          │   │
│  │   • Snappy Parquet Partitions       │────────▶│   • ds_bronze_p6                │   │
│  │   • _job_results/<task>/<run>.json  │         │   • ds_bronze_atlas             │   │
│  └─────────────────────────────────────┘         │   • ds_silver                   │   │
│                                                  │   • ds_operations               │   │
│                                                  │     - ingestion_execution_logs  │   │
│                                                  │     - ingestion_watermarks      │   │
│                                                  │     - registered_tables         │   │
│                                                  └─────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Infrastructure & Target Datasets

Integration testing verifies live cloud infrastructure deployed in `us-central1`:

| Component | Target Resource ID | Purpose |
| :--- | :--- | :--- |
| **GCP Project** | `elementl-509009` | Dedicated sandbox and ingestion test project. |
| **Bronze GCS Bucket** | `bkt-elementl-509009-udp-bronze-raw` | Object landing lake storing raw Snappy Parquet files and job manifests. |
| **Configs GCS Bucket**| `bkt-elementl-509009-udp-configs` | Storage bucket holding all 30 source YAML configurations. |
| **NetSuite Bronze** | `elementl-509009.ds_bronze_netsuite` | BigQuery landing tables for 11 NetSuite SuiteQL entities. |
| **Primavera P6 Bronze**| `elementl-509009.ds_bronze_p6` | BigQuery landing tables for 15 Primavera P6 Oracle entities. |
| **Atlas Bronze** | `elementl-509009.ds_bronze_atlas` | BigQuery landing tables for USGS, Census, and ArcGIS layers. |
| **Silver Staging** | `elementl-509009.ds_silver` | Dataform transformation target dataset. |
| **Operations / Audit**| `elementl-509009.ds_operations` | Central telemetry (`ingestion_execution_logs`) and watermarks. |
| **Artifact Registry** | `udp-ingestion-jobs` | Docker repository storing worker images built via Cloud Build. |
| **Cloud Run Jobs** | `suiteql-ingestion`, `jdbc-ingestion`, `arcgis-ingestion`, `bulk-ingestion` | Serverless ingestion batch jobs. |
| **Cloud Composer 2** | `composer-elementl-dev` | Private IP Airflow 2 orchestration cluster in custom VPC. |

---

## 3. Pre-Flight Execution Prerequisites

### 1. Authenticate to Google Cloud
Verify that your active user has the necessary roles (`roles/bigquery.admin`, `roles/storage.admin`, `roles/run.admin`, `roles/composer.user`):
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project elementl-509009
```

### 2. Verify Cloud Resource Attribution
When executing CLI commands or test runners, ensure proper agent attribution headers are set:
```bash
export CLOUDSDK_METRICS_ENVIRONMENT="datacloud.jetski"
```

### 3. Activate Python Environment
```bash
cd /usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw
source .venv/bin/activate
```

---

## 4. Running the Integration Tests

The integration test suite uses pytest markers (`-m integration`) to run specifically against the live GCP environment.

### Run the Complete Integration Suite
```bash
.venv/bin/python3 -m pytest -m integration -v app/udp/tests/integration
```

### Run Focused Test Suites

#### 1. BigQuery Datasets & Schema Assertions
Verifies that all 5 target datasets exist, that `ds_operations` contains all required tables, and that `ingestion_execution_logs` is properly day-partitioned on `execution_date` and clustered on `task_id, source_type, status`.
```bash
.venv/bin/python3 -m pytest -v app/udp/tests/integration/test_bigquery_datasets.py
```

#### 2. Cloud Storage Landing Lake & Configurations
Verifies that the Bronze raw landing bucket exists, the config bucket contains all 30 source YAMLs, and validates real read/write/delete lifecycle permissions.
```bash
.venv/bin/python3 -m pytest -v app/udp/tests/integration/test_gcs_landing.py
```

#### 3. Operational Audit Telemetry & Watermarks
Executes a live insert into `ds_operations.ingestion_execution_logs`, verifies column persistence and queryability, and tests watermark read/write operations in `ds_operations.ingestion_watermarks`.
```bash
.venv/bin/python3 -m pytest -v app/udp/tests/integration/test_audit_logging.py
```

#### 4. Cloud Run Jobs & Artifact Registry Images
Verifies all 4 jobs are deployed, confirms their container images point to the Artifact Registry repository (`udp-ingestion-jobs`), and triggers an end-to-end smoke execution.
```bash
.venv/bin/python3 -m pytest -v app/udp/tests/integration/test_cloud_run_jobs.py
```

#### 5. Cloud Composer 2 & Dynamic DAG Factory
Verifies that `composer-elementl-dev` is in `RUNNING` state, verifies that `udp_dag_factory.py` and the source YAMLs are synced in the environment bucket, and validates that `airflow dags list-import-errors` reports zero errors.
```bash
.venv/bin/python3 -m pytest -v app/udp/tests/integration/test_composer_dags.py
```

---

## 5. Verification Checklist & Invariants

When the integration tests pass, the following invariants are guaranteed:

1. **Isolation & Medallion Alignment**:
   - Upstream raw data never writes directly to silver or gold datasets.
   - Raw records land strictly in `gs://bkt-elementl-509009-udp-bronze-raw/<source>/raw/<task>/dt=<date>/data_<timestamp>.parquet`.
   - Bronze BigQuery tables strictly reside in `ds_bronze_netsuite`, `ds_bronze_p6`, or `ds_bronze_atlas`.
2. **NetSuite Safety Cap**:
   - Every NetSuite extraction enforces a hard ceiling of at most 10 records per execution.
3. **Auditability & Observability**:
   - Every job execution publishes outcome manifests to `_job_results/<task_id>/<run_id>.json`.
   - Every execution logs an audit telemetry row in BigQuery `ds_operations.ingestion_execution_logs`.
   - Incremental extraction state is safely maintained in `ds_operations.ingestion_watermarks`.
4. **Dynamic Pipeline Scalability**:
   - Adding a new source table requires only adding a declarative YAML file to `gs://bkt-elementl-509009-udp-configs/sources/`. The Airflow DAG Factory automatically discovers it and instantiates the pipeline.

---

## 6. Troubleshooting Common Issues

| Symptom | Probable Cause | Resolution |
| :--- | :--- | :--- |
| `DefaultCredentialsError` / `403 Forbidden` | Expired OAuth token or missing Application Default Credentials. | Run `gcloud auth application-default login` and verify active account. |
| `Unrendered Jinja template detected` | DAG passed literal `{{ ds }}` without rendering. | Check Airflow operator template rendering and pass rendered dates. |
| `Permission 'run.jobs.get' denied` | Missing IAM role on Cloud Run or service account. | Grant `roles/run.admin` and `roles/iam.serviceAccountUser` to the caller. |
| DAG not appearing in Airflow UI | GCS sync delay between bucket and Composer worker containers. | Cloud Composer sync takes 30–60 seconds. Wait 1 minute and rerun `airflow dags list`. |

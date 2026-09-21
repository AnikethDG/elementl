# Elementl Unified Data Platform (UDP) Test Suite

This directory contains the automated test suite for the Elementl UDP data ingestion framework, structured into **Unit Tests** (fast, isolated, mock-driven) and **Integration Tests** (live validation against Google Cloud Platform project `pid-nse-stg-core-apps-k8ti`).

For detailed documentation and architecture guides, refer to:
- 📖 [**Unit Testing Guide (`app/udp/docs/unit_testing.md`)**](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/docs/unit_testing.md): Testing methodology, mock strategies, fail-fast validations, and CI pipeline rules.
- 📖 [**Integration Testing Guide (`app/udp/docs/integration_testing.md`)**](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/docs/integration_testing.md): Live GCP validation across BigQuery, GCS landing, Cloud Run Jobs, and Cloud Composer 2.

---


## Directory Structure

```text
app/udp/tests/
├── conftest.py                     # Shared fixtures, mock stubs, and GCP project config
├── pytest.ini                      # Pytest configuration with unit & integration markers
├── unit/                           # Isolated unit tests (no cloud/network dependencies)
│   ├── test_base_extractor.py      # Parquet serialization, temporal coercion, PEM resolution
│   ├── test_bulk_extractor.py      # Atlas Census matrix & USGS NWIS payload normalization
│   ├── test_jdbc_extractor.py      # Oracle P6 data type sanitization & schema resolution
│   ├── test_job_config.py          # JobConfig validation, fail-fast rules, Jinja check
│   ├── test_suiteql_extractor.py   # NetSuite <=10 cap enforcement, HATEOAS links removal
│   └── test_udp_dag_factory.py     # Local YAML discovery and dynamic Airflow DAG generation
├── integration/                    # Live GCP integration tests against project pid-nse-stg-core-apps-k8ti
│   ├── test_gcs_landing.py         # Bronze Raw lake & Config bucket existence, read/write/delete
│   ├── test_bigquery_datasets.py   # ds_bronze_*, ds_silver, ds_operations schemas & partitioning
│   ├── test_audit_logging.py       # Live telemetry insert/query & watermark state persistence
│   ├── test_cloud_run_jobs.py      # Cloud Run Jobs existence, Artifact Registry images, execution
│   └── test_composer_dags.py       # Composer 2 environment status, DAG bucket sync, import errors
└── README.md                       # This documentation file
```

---

## Prerequisites & Setup

1. **Python Environment**:
   Python 3.11+ with project dependencies:
   ```bash
   cd /usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw
   source .venv/bin/activate
   ```

2. **GCP Authentication (Required for Integration Tests)**:
   Ensure your active gcloud session or Application Default Credentials (ADC) has access to `pid-nse-stg-core-apps-k8ti`:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   gcloud config set project pid-nse-stg-core-apps-k8ti
   ```

---

## Running Unit Tests

Unit tests are fast, fully mocked, and require no GCP credentials, network connection, or live cloud services.

```bash
# Run all unit tests
.venv/bin/python3 -m pytest -m unit -v app/udp/tests/unit

# Run a specific unit test module
.venv/bin/python3 -m pytest -v app/udp/tests/unit/test_job_config.py
.venv/bin/python3 -m pytest -v app/udp/tests/unit/test_suiteql_extractor.py
```

---

## Running Integration Tests

Integration tests validate deployed infrastructure, permissions, and live operations directly in project `pid-nse-stg-core-apps-k8ti`.

### 1. Run All Integration Tests
```bash
.venv/bin/python3 -m pytest -m integration -v app/udp/tests/integration
```

### 2. Run Targeted Subsets

- **BigQuery Datasets & Schema Validation**:
  ```bash
  .venv/bin/python3 -m pytest -v app/udp/tests/integration/test_bigquery_datasets.py
  ```
  *Validates `ds_bronze_netsuite`, `ds_bronze_p6`, `ds_bronze_atlas`, `ds_silver`, and `ds_operations` (including day partitioning and clustering on `ingestion_execution_logs`).*

- **Cloud Storage Landing & Config Buckets**:
  ```bash
  .venv/bin/python3 -m pytest -v app/udp/tests/integration/test_gcs_landing.py
  ```
  *Validates `bkt-pid-nse-stg-core-apps-k8ti-udp-bronze-raw` and `bkt-pid-nse-stg-core-apps-k8ti-udp-configs/sources/`.*

- **Audit Telemetry & Watermarks**:
  ```bash
  .venv/bin/python3 -m pytest -v app/udp/tests/integration/test_audit_logging.py
  ```
  *Performs live write, query, and verification against `ds_operations.ingestion_execution_logs` and `ds_operations.ingestion_watermarks`.*

- **Cloud Run Jobs & Artifact Registry**:
  ```bash
  .venv/bin/python3 -m pytest -v app/udp/tests/integration/test_cloud_run_jobs.py
  ```
  *Verifies all 4 jobs are deployed with images from `us-central1-docker.pkg.dev/pid-nse-stg-core-apps-k8ti/udp-ingestion-jobs/` and executes a smoke test run.*

- **Cloud Composer Environment & DAG Sync**:
  ```bash
  .venv/bin/python3 -m pytest -v app/udp/tests/integration/test_composer_dags.py
  ```
  *Validates `composer-elementl-dev` state, verifies DAG bucket sync, and confirms zero Airflow import errors.*

---

## Environment Variables Reference

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `GCP_PROJECT_ID` | `pid-nse-stg-core-apps-k8ti` | Target GCP project for integration testing. |
| `GCP_REGION` | `us-central1` | Target GCP region for Cloud Run, Composer, and BigQuery. |
| `RAW_BUCKET_NAME` | `bkt-pid-nse-stg-core-apps-k8ti-udp-bronze-raw` | GCS landing bucket for bronze parquet files. |
| `GCS_CONFIG_BUCKET` | `bkt-pid-nse-stg-core-apps-k8ti-udp-configs` | GCS bucket containing declarative YAML configurations. |
| `COMPOSER_ENV_NAME`| `composer-elementl-dev` | Target Cloud Composer 2 instance name. |

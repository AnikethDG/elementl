# Elementl UDP — Cloud Run Ingestion Jobs: Common Core Framework

## 📌 Overview & Purpose

The `common/` directory serves as the **centralized platform core SDK and shared runtime contract** for all Elementl Unified Data Platform (UDP) Cloud Run Ingestion Jobs:
* **NetSuite SuiteQL Ingestion** (`suiteQL-ingestion`)
* **Oracle Primavera P6 JDBC Ingestion** (`jdbc-ingestion`)
* **Elementl Atlas ArcGIS REST Ingestion** (`arcgis-ingestion`)
* **Elementl Atlas Bulk Ingestion** (`bulk-ingestion`)

### Why This Folder Exists
Instead of duplicating credential retrieval, data type normalization, Parquet serialization, Airflow XCom callbacks, and BigQuery audit logging in every individual ingestion microservice, `common/` consolidates these cross-cutting platform concerns into a **single source of truth**.

By centralizing these shared responsibilities, all ingestion jobs:
1. Adhere to the same **GCS Bronze partition layout** (`year=YYYY/month=MM/day=DD/`).
2. Write standardized **Snappy-compressed Apache Parquet** with unified timestamp casting.
3. Emit identical **execution telemetry and audit metrics** to BigQuery (`ds_operations.ingestion_execution_logs`).
4. Support the two-way **Airflow XCom handshake** via GCS job results.

---

## 🧱 Module Structure & Responsibilities

```
common/
├── __init__.py           # Package initializer exposing common modules
├── base_extractor.py     # Base class for extractors: Secrets, Parquet streaming, GCS writes
├── job_config.py         # Strongly-typed configuration dataclasses & CLI validators
├── job_runner.py         # Standard job harness: timing, Airflow XCom callbacks, audit logging
└── README.md             # This documentation
```

### 1. `base_extractor.py` (`BaseExtractor`)
The foundational abstract base class from which every source extractor inherits.
* **GCP Secret Manager Resolution**: Resolves credentials at runtime (e.g. NetSuite OAuth 2.0 M2M RSA private keys, client IDs, P6 Oracle database passwords, ArcGIS API tokens) without baking secrets into images or environment variables.
* **Temporal Coercion (`_coerce_temporal`)**: Automatically detects and normalizes timestamps, dates, and ISO 8601 strings into consistent UTC PyArrow-compatible types.
* **Streaming GCS Parquet Serialization (`write_records_to_gcs`)**:
  * Converts in-memory Python record batches directly into PyArrow Tables.
  * Writes Snappy-compressed Parquet files directly to Google Cloud Storage (`gs://<bucket>/<source>/<table_name>/year=YYYY/month=MM/day=DD/*.parquet`).
  * Emits row counts, compressed byte counts, and exact GCS URIs for downstream tasks.

### 2. `job_runner.py` (`JobRunner`)
The universal lifecycle harness and entrypoint orchestrator for containerized jobs.
* **CLI Argument Parsing**: Parses standardized runtime arguments passed by Cloud Composer / Airflow (`--config-json`, `--task-id`, `--run-id`, `--execution-date`, `--target-bucket`).
* **Airflow XCom Callback via GCS**: Because Cloud Run Jobs execute asynchronously and cannot return an HTTP response body to Airflow, `JobRunner` writes a structured outcome manifest to:
  `gs://<bucket>/_job_results/<task_id>/<run_id>.json`
  Airflow DAGs read this JSON file to populate XCom with row counts, output paths, and execution status.
* **Audit Telemetry Logging**: Automatically inserts a row into BigQuery `ds_operations.ingestion_execution_logs` containing execution status (`SUCCESS` / `FAILED`), elapsed duration, bytes written, record counts, and stack traces on failure.

### 3. `job_config.py` (`IngestionJobConfig`)
Defines the strongly-typed schema for all ingestion job configurations:
* Validates source definitions, table names, primary keys, watermark columns, and extraction filters (`where_clause`).
* Provides helper methods for JSON deserialization and environment variable overrides.

---

## 🚀 How It Is Used in Ingestion Jobs

Each ingestion job extends `BaseExtractor` and executes via `JobRunner`:

```python
from common.base_extractor import BaseExtractor
from common.job_runner import JobRunner

class NetSuiteSuiteQLExtractor(BaseExtractor):
    def extract(self, config: dict) -> dict:
        # 1. Fetch credentials via self.get_secret(...)
        # 2. Query source system (e.g. SuiteQL REST endpoint)
        # 3. Stream records to GCS Bronze using self.write_records_to_gcs(...)
        return {
            "rows_extracted": count,
            "gcs_path": gcs_uri,
            "bytes_written": byte_size,
        }

if __name__ == "__main__":
    extractor = NetSuiteSuiteQLExtractor()
    JobRunner.run(extractor.extract)
```

---

## 📦 Container Build & Packaging

When building container images for Cloud Run, the build context is set to `apps/udp/cloud-run-jobs/` so that `common/` is bundled alongside the specific job code:

```bash
# Example: Building suiteQL-ingestion from apps/udp/cloud-run-jobs
gcloud builds submit \
  --config=suiteQL-ingestion/cloud_build.yaml \
  apps/udp/cloud-run-jobs
```

In each job Dockerfile:
```dockerfile
FROM python:3.11-slim
WORKDIR /app

COPY suiteQL-ingestion/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Shared platform core
COPY common/ common/

# Job-specific code
COPY suiteQL-ingestion/main.py main.py

ENTRYPOINT ["python3", "main.py"]
```

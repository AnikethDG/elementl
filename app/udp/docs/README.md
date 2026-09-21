# Elementl Unified Data Platform (UDP) — Comprehensive Technical Reference & Architecture Specification

---

## 📑 Table of Contents

1. [Executive Summary & Architectural Tenets](#1-executive-summary--architectural-tenets)
2. [End-to-End Medallion Data Lifecycle](#2-end-to-end-medallion-data-lifecycle)
3. [Dynamic DAG Factory & Airflow Automation](#3-dynamic-dag-factory--airflow-automation)
   - [How DAGs Get Created](#how-dags-get-created)
   - [What Triggers New DAG Generation](#what-triggers-new-dag-generation)
   - [DAG Anatomy & TaskGroup Execution Flow](#dag-anatomy--taskgroup-execution-flow)
   - [Downstream Gold Analytical DAGs](#downstream-gold-analytical-dags)
4. [GCS Bronze Landing & Hive Partition Directory Hierarchy](#4-gcs-bronze-landing--hive-partition-directory-hierarchy)
   - [Where the Structure Is Specified in Code](#where-the-structure-is-specified-in-code)
   - [Storage Hierarchy Diagram](#storage-hierarchy-diagram)
   - [Path Formula & Segment Breakdown](#path-formula--segment-breakdown)
   - [Parquet File Contents & Audit Column Enrichment](#parquet-file-contents--audit-column-enrichment)
   - [Engineering Rationale for Hive-Style Partitioning](#engineering-rationale-for-hive-style-partitioning)
5. [Cloud Run Jobs: Enterprise Batch Ingestion Engine](#5-cloud-run-jobs-enterprise-batch-ingestion-engine)
   - [Why Cloud Run Jobs Are the Exclusive Ingestion Engine](#why-cloud-run-jobs-are-the-exclusive-ingestion-engine)
   - [Specialized Microservice Jobs Inventory](#specialized-microservice-jobs-inventory)
   - [Airflow Lifecycle via CloudRunExecuteJobOperator](#airflow-lifecycle-via-cloudrunexecutejoboperator)
6. [Centralized `common/` Core Library](#6-centralized-common-core-library)
   - [Why This Folder Exists](#why-this-folder-exists)
   - [Core Modules Breakdown](#core-modules-breakdown)
   - [Container Build & Docker Context Strategy](#container-build--docker-context-strategy)
7. [Source System Extractors & Implementation Nuances](#7-source-system-extractors--implementation-nuances)
   - [NetSuite ERP (SuiteQL & OAuth 2.0 M2M JWT)](#netsuite-erp-suiteql--oauth-20-m2m-jwt)
   - [The NetSuite <= 10 Records Triple-Guardrail](#the-netsuite--10-records-triple-guardrail)
   - [Oracle Primavera P6 (TCPS 2484 & Wallet Auth)](#oracle-primavera-p6-tcps-2484--wallet-auth)
   - [Elementl Atlas (ArcGIS REST & Bulk Spatial Unpacker)](#elementl-atlas-arcgis-rest--bulk-spatial-unpacker)
8. [Declarative Metadata & Schema Catalog](#8-declarative-metadata--schema-catalog)
   - [Source YAML Configuration Contract](#source-yaml-configuration-contract)
   - [Schema Definition Files](#schema-definition-files)
   - [Onboarded Table Inventory (30 Entities)](#onboarded-table-inventory-30-entities)
9. [Dataform Transformation Layer (Silver & Gold)](#9-dataform-transformation-layer-silver--gold)
   - [Compilation & Invocation Model](#compilation--invocation-model)
   - [Silver Cleansing & Deduplication](#silver-cleansing--deduplication)
   - [Gold Cross-Table Spatial Enrichment](#gold-cross-table-spatial-enrichment)
10. [Observability, Telemetry & BigQuery Audit Logging](#10-observability-telemetry--bigquery-audit-logging)
    - [Audit Table Schema](#audit-table-schema)
    - [Airflow XCom Callback Handshake](#airflow-xcom-callback-handshake)
11. [Networking, Security & Infrastructure Topology](#11-networking-security--infrastructure-topology)
    - [Serverless VPC Access & Static Cloud NAT Egress](#serverless-vpc-access--static-cloud-nat-egress)
    - [GCP Secret Manager Integration](#gcp-secret-manager-integration)
    - [Service Accounts & IAM Permissions](#service-accounts--iam-permissions)
12. [Verification Suite & Operational Runbooks](#12-verification-suite--operational-runbooks)
    - [Running the Automated E2E Test](#running-the-automated-e2e-test)
    - [Deploying Cloud Run Services](#deploying-cloud-run-services)
    - [Onboarding a New Source Table (Step-by-Step)](#onboarding-a-new-source-table-step-by-step)

---

## 1. Executive Summary & Architectural Tenets

The **Elementl Unified Data Platform (UDP)** Ingestion Framework is an enterprise-grade, metadata-driven replication and transformation framework built natively on **Google Cloud Platform (GCP)**. It automates the ingestion of enterprise records from:
1. **NetSuite ERP** via SuiteQL and OAuth 2.0 Machine-to-Machine (M2M) JWT.
2. **Oracle Primavera P6 EPPM** via secure JDBC over TCPS on port 2484.
3. **Elementl Atlas** via ArcGIS REST Geospatial Feature Services and bulk spatial archives.

### Core Architectural Tenets:
* **Zero Hardcoded Pipelines**: No individual DAG or Cloud Run container is hardcoded for a single table. All pipelines are driven dynamically by declarative YAML specifications.
* **Strict Medallion Decoupling**: Data **never** flows directly from an external source into BigQuery tables. Data lands immutably in Google Cloud Storage as partitioned Apache Parquet files (Bronze Lake) before being loaded into BigQuery (Bronze Raw), cleansed in Dataform (Silver Conformed), and aggregated into views (Gold Marts).
* **Multi-Environment Portability**: All project IDs, regions, bucket names, and Dataform repositories are parameterized across environments (`dev.json`, `staging.json`, `production.json`).
* **Auditability & Traceability**: Every extraction, file write, and BigQuery load publishes structured telemetry to BigQuery audit tables and GCS XCom manifests.

---

## 2. End-to-End Medallion Data Lifecycle

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                   UPSTREAM SOURCE SYSTEMS                                │
│        NetSuite ERP (SuiteQL)  │  Oracle Primavera P6 (JDBC)  │  Atlas Geospatial REST   │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
                                             │  1. Extract & Stream via Cloud Run
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                     GCS BRONZE LAKE                                      │
│  gs://bkt-<project>-udp-bronze-raw/<source_system>/raw/<target_table>/dt=YYYY-MM-DD/     │
│  - Format: Snappy-compressed Apache Parquet                                              │
│  - Added Columns: ingestion_date (DATE), ingestion_timestamp (TIMESTAMP UTC)             │
│  - Partitioning: Native Hive-style date partition (dt=YYYY-MM-DD)                         │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
                                             │  2. Batch Load (GCSToBigQueryOperator)
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                    BIGQUERY BRONZE RAW                                   │
│  Datasets: ds_bronze_netsuite  │  ds_bronze_p6  │  ds_bronze_atlas                       │
│  Target Project: p-nsedusc1-data-landing                                                 │
│  - Tables: Raw landing mirrors source schema without transformations                     │
│  - Write Mode: WRITE_APPEND / WRITE_TRUNCATE per task config                             │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
                                             │  3. Dataform Compilation & Workflow
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                   BIGQUERY SILVER CONFORMED                              │
│  Dataset:  ds_silver                                                                     │
│  Target Project: p-nsedusc1-data-warehouse                                               │
│  - Transformations: Type casting, timestamp timezone normalization                       │
│  - Data Quality: Automated assertions (failed records stored in ds_dataform_assertions)   │
│  - Deduplication: Windowed ROW_NUMBER() over primary_key ORDER BY watermark DESC         │
└────────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
                                             │  4. Analytical Aggregation & Spatial Joins
                                             ▼
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                     BIGQUERY GOLD MARTS                                  │
│  Datasets: ds_gold (Warehouse)  │  ds_atlas_analytics (Consumer Project)                 │
│  Target Project: p-nsedusc1-data-warehouse (Gold)  │  p-nsedusc1-data-atlas (Consumer)    │
│  - Atlas GeoViz: ST_INTERSECTS, ST_BUFFER joins across Quaternary Faults & Urban Areas   │
│  - Financial & Project Marts: Project Cost vs Progress, Resource Utilization             │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

### Official BigQuery Dataset & Project Naming Standards

| Dataset Layer | Target GCP Project | Naming Pattern / Example | Description / Use Case |
| :--- | :--- | :--- | :--- |
| **Bronze (Landing / Raw)** | `p-nsedusc1-data-landing` | `ds_bronze_p6`, `ds_bronze_netsuite`, `ds_bronze_atlas` | Raw/landing data. Each source has its own dataset for table isolation (naming conflict avoidance). |
| **Silver (Structured / Cleansed)** | `p-nsedusc1-data-warehouse` | `ds_silver` | Cleansed, standardized (and audit columns added) source-like representation of P6, NetSuite, and Atlas tables. |
| **Gold (Conformed / Curated)** | `p-nsedusc1-data-warehouse` | `ds_gold` | Curated business data models and analytical marts. |
| **Dataform Assertions Results** | `p-nsedusc1-data-warehouse` | `ds_dataform_assertions` | Datasets storing Dataform assertions failed records. |
| **Operations / Framework** | `p-nsedusc1-data-services` | `ds_operations` | Operational metadata table driving the DAG Factory, dynamic pipeline generation, and audit logs (`ds_operations.ingestion_execution_logs`). |
| **Consumer (Atlas Analytics)** | `p-nsedusc1-data-atlas` | `ds_atlas_analytics` | Exposed analytics views, site suitability parameters, and AI search tables for Project Atlas. |


---

## 3. Dynamic DAG Factory & Airflow Automation

The DAG creation layer is orchestrated by [`app/udp/composer/dags/udp_dag_factory.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/composer/dags/udp_dag_factory.py).

### How DAGs Get Created

1. **Airflow Scheduler Parsing**:
   * The Cloud Composer / Airflow scheduler executes Python files in the DAGs folder at a regular interval (default: every 30–60 seconds).
   * When `udp_dag_factory.py` is parsed, it invokes `discover_all_configs()`.

2. **Dual-Path Configuration Discovery**:
   * **Path 1 (Primary - GCS Cloud Bucket)**: Scans `gs://<GCS_CONFIG_BUCKET>/sources/` (e.g. `gs://bkt-pid-nse-stg-core-apps-k8ti-udp-configs/sources/`). It downloads and parses all `.yaml` files.
   * **Path 2 (Fallback - Local Filesystem)**: Scans local directories (`app/udp/configs/sources/` or `/home/airflow/gcs/dags/configs/sources/`).
   * Deduplication ensures each `task_id` is loaded only once per scheduler evaluation.

3. **Dynamic Airflow Registration**:
   For every discovered configuration dictionary `cfg`, the factory calls `create_udp_dag(cfg)`. The returned DAG object is injected directly into Python global scope:
   ```python
   globals()[f"dag_udp_{config_data['task_id']}"] = dag_instance
   ```
   Airflow detects the variable in `globals()` and registers the DAG into the Airflow metadata database.

### What Triggers New DAG Generation

* **Zero-Downtime Hot Onboarding**:
  To add a new table pipeline to the platform, **no code needs to be written or deployed, and Airflow does not need to be restarted**.
  An engineer or automated script simply saves a new YAML file into the GCS bucket:
  ```bash
  gcloud storage cp new_table.yaml gs://bkt-pid-nse-stg-core-apps-k8ti-udp-configs/sources/netsuite/
  ```
  Within 30–60 seconds, the Airflow scheduler reads the new file from GCS, constructs the DAG, and renders it in the Composer UI ready for scheduling.
* **Updating Pipelines**:
  Modifying `schedule`, `where_clause`, `max_records`, or `dataform_tags` in the GCS YAML immediately updates the active DAG on the next scheduler parse cycle.

### DAG Anatomy & TaskGroup Execution Flow

Each generated DAG (`dag_udp_<task_id>`) executes two sequential task groups:

```
[start]
   │
   ▼
┌──────────────────────────────────────────────────────────┐
│ TaskGroup: ingestion_group                               │
│                                                          │
│  ┌─────────────────────────┐   ┌───────────────────────┐ │
│  │ execute_cloud_run_job   │──▶│ load_gcs_to_bq_bronze │ │
│  │(CloudRunExecuteJobOper) │   │  (GCSToBigQuery)      │ │
│  └─────────────────────────┘   └───────────────────────┘ │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────┐
│ TaskGroup: dataform_group                                │
│                                                          │
│  ┌─────────────────────────┐   ┌───────────────────────┐ │
│  │ compile_dataform        │──▶│ invoke_silver_model   │ │
│  │ (git_commitish: stage)  │   │ (included_tags: [tag])│ │
│  └─────────────────────────┘   └───────────────────────┘ │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
                        [end]
```

1. **`ingestion_group`**:
   * **Extraction Operator** (`CloudRunExecuteJobOperator`): Executes the dedicated Cloud Run Job container asynchronously with dynamic container environment overrides (`TASK_ID`, `SOURCE_SYSTEM`, `TARGET_DATASET`, `TARGET_TABLE`, `WHERE_CLAUSE`, `MAX_RECORDS`, `GCS_BUCKET`, `GCS_PREFIX`, `EXECUTION_DATE`, `RUN_ID`, `DAG_ID`).
   * **GCS-to-BigQuery Bronze Load Operator** (`GCSToBigQueryOperator`): Reads the partitioned Parquet files from `RAW_BUCKET` using prefix:
     `{source_system}/raw/{target_table}/dt={{ ds }}/*.parquet`
     and appends them directly into `{PROJECT_ID}.{target_dataset}.{target_table}` with schema autodetection.
2. **`dataform_group`**:
   * **`compile_dataform`** (`DataformCreateCompilationResultOperator`): Compiles the Dataform SQLX code repository against the target Git branch (e.g. `stage` or `main`).
   * **`invoke_silver_and_assertions`** (`DataformCreateWorkflowInvocationOperator`): Executes only the models and assertions matching the table tag (`included_tags: [task_id]`), ensuring execution isolation.

---


## 4. GCS Bronze Landing & Hive Partition Directory Hierarchy

### Where the Structure Is Specified in Code

The GCS Bronze landing folder layout is programmatically enforced in:
1. **GCS Prefix Dynamic Binding** — [`app/udp/composer/dags/udp_dag_factory.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/composer/dags/udp_dag_factory.py):
   ```python
   {"name": "GCS_PREFIX", "value": f"{cfg['source_system']}/raw/{target_table}/dt={{{{ ds }}}}"}
   ```
2. **Timestamped Parquet Key** — [`app/udp/cloud-run-jobs/common/base_extractor.py:L161`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/base_extractor.py#L161):
   ```python
   filename = f"{prefix.strip('/')}/data_{now_utc.strftime('%Y%m%d_%H%M%S')}.parquet"
   ```
3. **Airflow Partition Ingestion Pattern** — [`app/udp/composer/dags/udp_dag_factory.py:L156`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/composer/dags/udp_dag_factory.py#L156):
   ```python
   source_objects=[f"{cfg['source_system']}/raw/{cfg['target_table']}/dt={{{{ ds }}}}/*.parquet"]
   ```
4. **Airflow XCom Job Manifest Key** — [`app/udp/cloud-run-jobs/common/job_runner.py:L70`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/job_runner.py#L70):
   ```python
   result_key = f"_job_results/{cfg.task_id}/{cfg.run_id}.json"
   ```

### Storage Hierarchy Diagram

```text
gs://<raw_landing_bucket>/
│
├── netsuite/                                   <-- Source System isolation
│   └── raw/                                    <-- Medallion Zone (Bronze)
│       ├── netsuite_account/                   <-- Target Table
│       │   ├── dt=2026-09-18/                  <-- Hive Execution Date Partition
│       │   │   └── data_20260918_110958.parquet
│       │   └── dt=2026-09-19/
│       │       └── data_20260919_020000.parquet
│       ├── netsuite_classification/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_111000.parquet
│       ├── netsuite_customer/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_111005.parquet
│       └── ... (all 11 NetSuite tables)
│
├── p6/                                         <-- Oracle Primavera P6
│   └── raw/
│       ├── p6_activity/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_020000.parquet
│       ├── p6_project/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_020000.parquet
│       └── ... (all 15 P6 tables)
│
├── atlas/                                      <-- Elementl Atlas Geospatial
│   └── raw/
│       ├── atlas_s1_01_population_density/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_020000.parquet
│       ├── atlas_s1_06_quaternary_faults/
│       │   └── dt=2026-09-18/
│       │       └── data_20260918_020000.parquet
│       └── ... (Atlas spatial layers)
│
└── _job_results/                               <-- Airflow XCom metadata
    ├── netsuite_account/
    │   └── manual__2026-09-18T11:09:57.json
    └── ...
```

### Path Formula & Segment Breakdown

$$	ext{gs://}\mathbf{\{raw\_landing\_bucket\}}	ext{/}\mathbf{\{source\_system\}}	ext{/raw/}\mathbf{\{target\_table\}}	ext{/dt=}\mathbf{\{YYYY-MM-DD\}}	ext{/data\_}\mathbf{\{YYYYMMDD\_HHMMSS\}}	ext{.parquet}$$

| Segment | Example | Purpose & Origin |
| :--- | :--- | :--- |
| **`{raw_landing_bucket}`** | `bkt-pid-nse-stg-core-apps-k8ti-udp-bronze-raw` | GCS Bucket resolved dynamically from `environments/*.json` (`raw_landing_bucket`) or `RAW_BUCKET_NAME`. Decoupled from table YAMLs for multi-environment portability. |
| **`{source_system}`** | `netsuite`, `p6`, `atlas` | `source_system` key in YAML. Establishes top-level boundary between disparate source technologies. |
| **`raw`** | `raw` | Identifies the raw Bronze landing zone within the Medallion architecture. |
| **`{target_table}`** | `netsuite_classification` | `target_table` key in YAML. Isolates records into individual table prefixes. |
| **`dt={YYYY-MM-DD}`** | `dt=2026-09-18` | Standard Hive-style date partition, corresponding to Airflow logical execution date (`{{ ds }}`). |
| **`data_*.parquet`** | `data_20260918_111000.parquet` | Snappy-compressed columnar Parquet file stamped with the UTC execution time. |

### Parquet File Contents & Audit Column Enrichment

Every record written to Parquet is enriched with two platform audit attributes:
```python
{
    # ... all source fields extracted from source API or database ...
    "ingestion_date": datetime.date(2026, 9, 18),         # DATE: Matches Hive partition
    "ingestion_timestamp": datetime.datetime(...)         # TIMESTAMP: Exact UTC extraction moment
}
```

* **Format**: Apache Parquet 2.6 columnar table.
* **Compression**: Snappy compression (`compression="snappy"`), balancing high read speed with ~70% compression ratio.
* **Temporal Coercion**: Any string matching ISO-8601 timestamps (`YYYY-MM-DDTHH:MM:SSZ`) or dates (`YYYY-MM-DD`) is parsed into native PyArrow timestamp/date types to eliminate BigQuery string-to-date casting overhead.

### Engineering Rationale for Hive-Style Partitioning

1. **BigQuery Date Partition Pruning**:
   BigQuery natively understands `dt=YYYY-MM-DD` directory paths. When loading via `GCSToBigQueryOperator` or querying via BigLake external tables, BigQuery automatically infers a virtual column `dt`. Queries filtering on `dt = CURRENT_DATE()` scan *only* the matching GCS prefix, preventing expensive full bucket scans.
2. **Deterministic Idempotency & Backfills**:
   If an Airflow DAG run fails or historical data is backfilled for `2026-09-10`, the job targets `.../dt=2026-09-10/`. It reloads only that day’s partition without affecting past or future data.
3. **Partition-Level Storage Lifecycle**:
   GCS lifecycle management rules can transition folders older than 90 days (`*/dt=2024-*`) to Nearline or Coldline storage classes, minimizing long-term storage costs.
4. **Audit Traceability**:
   The timestamp inside `data_YYYYMMDD_HHMMSS.parquet` matches the `execution_id` recorded in BigQuery `ds_operations.ingestion_execution_logs`.

---

## 5. Cloud Run Jobs: Enterprise Batch Ingestion Engine

The Elementl Unified Data Platform (UDP) relies exclusively on **Cloud Run Jobs** for all production data extractions.

### Why Cloud Run Jobs Are the Exclusive Ingestion Engine

Early prototypes explored HTTP Cloud Functions, but enterprise production workloads require Cloud Run Jobs for crucial architectural reasons:

1. **No HTTP Request/Response Timeouts (24-Hour Limits)**:
   * NetSuite SuiteQL queries traversing millions of accounting records, Oracle Primavera P6 TCPS JDBC relational dumps, and multi-gigabyte spatial ZIP unpack operations exceed standard HTTP request timeouts.
   * Cloud Run Jobs execute as **run-to-completion batch tasks** capable of executing for up to **24 hours**.
2. **Container & Driver Isolation**:
   * Each source platform has distinct native driver requirements (e.g. `oracledb` Thin mode with SSL wallets, ESRI geospatial libraries, cryptography for RSA M2M tokens).
   * Segregating these into specialized Cloud Run Job containers prevents monolithic dependency conflicts and bloat.
3. **Native Airflow Integration**:
   * Cloud Composer manages jobs asynchronously via `CloudRunExecuteJobOperator`. Airflow initiates the job and receives event notifications upon completion without holding synchronous HTTP socket connections open.

### Specialized Microservice Jobs Inventory

| Service Directory | Container Image | Target Source Platform | Extraction Protocol |
| :--- | :--- | :--- | :--- |
| `suiteql-ingestion/` | `udp-ingestion-jobs/suiteql-ingestion` | NetSuite ERP | SuiteQL REST API via OAuth 2.0 M2M JWT |
| `jdbc-ingestion/` | `udp-ingestion-jobs/jdbc-ingestion` | Oracle Primavera P6 EPPM | Oracle JDBC / `python-oracledb` over TCPS 2484 |
| `arcgis-ingestion/` | `udp-ingestion-jobs/arcgis-ingestion` | Elementl Atlas (ArcGIS) | ESRI Feature Service REST API (GeoJSON) |
| `bulk-ingestion/` | `udp-ingestion-jobs/bulk-ingestion` | Elementl Atlas (Bulk Spatial) | HTTP Bulk Downloader & In-Memory ZIP Unpacker |

### Airflow Lifecycle via CloudRunExecuteJobOperator

```text
Composer Airflow DAG
       │
       ▼  (1. Dispatches CloudRunExecuteJobOperator with container overrides)
Cloud Run Job Container Execution
  - Parses JobConfig.from_env()
  - Extracts records from Source API/DB
  - Coerces temporal strings into PyArrow types
  - Writes Snappy Parquet to GCS Bronze path
  - Writes completion manifest to gs://<bucket>/_job_results/<task_id>/<run_id>.json
  - Publishes telemetry row into BigQuery ds_operations.ingestion_execution_logs
       │
       ▼  (2. Job finishes with Exit Code 0)
Airflow GCSToBigQueryOperator
  - Reads Parquet files from gs://<bucket>/<source>/raw/<table_name>/dt={{ ds }}/*.parquet
  - Appends rows to BigQuery Bronze table ({PROJECT_ID}.{target_dataset}.{target_table})
```

---

## 6. Centralized `common/` Core Library

### Why This Folder Exists
In early iterations, each Cloud Run job (`suiteql-ingestion`, `jdbc-ingestion`, `arcgis-ingestion`, `bulk-ingestion`) duplicated utility code in its own `common/` subfolder. This violated DRY principles and created code drift risk.

All shared logic has been **consolidated into a single source of truth**:
[`app/udp/cloud-run-jobs/common/`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/)

### Core Modules Breakdown

1. **[`base_extractor.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/base_extractor.py) (`BaseExtractor`)**:
   * Abstract base class for all source extractors.
   * `get_secret(secret_id)`: Fetches private keys, passwords, and tokens from GCP Secret Manager.
   * `_coerce_temporal(item)`: Normalizes ISO date and timestamp strings into UTC types.
   * `write_records_to_gcs(...)`: Ingests Python dict batches, constructs PyArrow Table, applies Snappy compression, and uploads directly to GCS Bronze partitioned path.
2. **[`job_runner.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/job_runner.py) (`JobRunner`)**:
   * Universal CLI entrypoint harness.
   * Captures execution timing and handles uncaught exceptions.
   * **Airflow XCom Handshake**: Writes execution outcome to `gs://<bucket>/_job_results/<task_id>/<run_id>.json`.
   * **Audit Logger**: Directly inserts telemetry rows into BigQuery `ds_operations.ingestion_execution_logs`.
3. **[`job_config.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/cloud-run-jobs/common/job_config.py) (`IngestionJobConfig`)**:
   * Strongly-typed dataclass schemas for runtime configuration.
   * Validates parameter presence and rejects unrendered Jinja templates.

### Container Build & Docker Context Strategy

Each job Dockerfile is built using `app/udp/cloud-run-jobs` as the build context root. This allows Docker to copy the shared `common/` module alongside the job's code:

```dockerfile
FROM python:3.11-slim
WORKDIR /app

# Copy dependencies and install
COPY suiteql-ingestion/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy centralized common core
COPY common/ common/

# Copy job-specific implementation
COPY suiteql-ingestion/main.py main.py

ENTRYPOINT ["python3", "main.py"]
```

In `cloud_build.yaml`:
```yaml
steps:
  - name: 'gcr.io/cloud-builders/docker'
    dir: 'app/udp/cloud-run-jobs'
    args:
      - 'build'
      - '-f'
      - 'suiteql-ingestion/Dockerfile'
      - '-t'
      - '${_REGION}-docker.pkg.dev/${_TARGET_PROJECT_ID}/udp-ingestion-jobs/suiteql-ingestion:${SHORT_SHA}'
      - '.'
```

---

## 7. Source System Extractors & Implementation Nuances

### NetSuite ERP (SuiteQL & OAuth 2.0 M2M JWT)

* **Protocol**: NetSuite SuiteTalk REST API via SuiteQL queries (`POST /services/rest/query/v1/suiteql`).
* **Authentication**: Machine-to-Machine (M2M) OAuth 2.0 JWT using 5 Secret Manager secrets:
  1. `client_id`: NetSuite Integration Client ID.
  2. `certificate_id`: NetSuite digital certificate ID.
  3. `private_key`: PEM-encoded RSA 2048 private key.
  4. `account_id`: NetSuite Realm / Account ID (e.g. `1234567_SB1`).
  5. `token_url`: NetSuite OAuth token endpoint (`https://<account>.suitetalk.api.netsuite.com/services/rest/auth/oauth2/v1/token`).

### The NetSuite <= 10 Records Triple-Guardrail

To prevent performance degradation and comply with client licensing constraints, **NetSuite queries are strictly capped at 10 records maximum**. This guardrail is enforced at three independent defense-in-depth levels:

1. **Declarative Level (YAML)**:
   Every NetSuite YAML file explicitly declares:
   ```yaml
   where_clause: "ROWNUM <= 10"
   max_records: 10
   ```
2. **Job Configuration Level (`common/job_config.py` & `common/base_extractor.py`)**:
   The runtime configuration enforces `max_records <= 10` for NetSuite tasks, defaulting to 10 if an override attempts to request more.
3. **Job Extractor Query Rewriter Level (`suiteql-ingestion/main.py`)**:
   The query rewriter automatically detects and injects `ROWNUM <= 10`:
   ```python
   def _apply_row_limit(query: str, max_records: int) -> str:
       # Inserts or rewrites WHERE/AND ROWNUM <= 10
   ```

### Oracle Primavera P6 (TCPS 2484 & Wallet Auth)

* **Protocol**: Direct relational database connection via Oracle JDBC / `python-oracledb` in Thin mode.
* **Security & Encryption**: Oracle TCPS (SSL/TLS over port 2484) authenticated via an Oracle Auto-Login Wallet (`cwallet.sso`) or database credentials stored in Secret Manager.
* **Mixed Precision Coercion**:
  P6 database schemas frequently have numerical columns containing mixed integer and floating-point values across rows. To prevent BigQuery schema collision errors, the extractor automatically coerces numerical columns with decimal presence into `FLOAT64` across all row batches.

### Elementl Atlas (ArcGIS REST & Bulk Spatial Unpacker)

* **ArcGIS REST Feature Services** (`arcgis-ingestion`):
  * Queries ESRI ArcGIS Feature Services via REST endpoint (`query?f=geojson&where=1=1`).
  * Serializes geometry attributes (Points, Polygons, Multipolygons) and spatial properties into structured columns.
* **Bulk Ingestion & Spatial Unpacker** (`bulk-ingestion`):
  * Downloads zipped GIS shapefile packages and spatial datasets from HTTP/S3 endpoints.
  * Unpacks ZIP archives in memory, parses geometry envelopes, and lands raw records into GCS Bronze.

---

## 8. Declarative Metadata & Schema Catalog

### Source YAML Configuration Contract

Located under `app/udp/configs/sources/<system>/<table_name>.yaml`:

```yaml
task_id: netsuite_account                      # Unique task identifier
source_system: netsuite                        # netsuite | p6 | atlas
ingestion_job: suiteql-ingestion               # Designated Cloud Run Job
source_table: account                          # Source table/entity name
primary_key: id                                # Primary key for deduplication
watermark_column: lastmodifieddate             # High-watermark column for incremental loads
where_clause: "ROWNUM <= 10"                   # SQL filter / guardrail
max_records: 10                                # Extraction row cap
target_dataset: ds_bronze_netsuite            # BigQuery Bronze dataset
target_table: netsuite_account                 # BigQuery Bronze table
silver_dataset: ds_silver                      # BigQuery Silver dataset
silver_table: stg_netsuite_account             # BigQuery Silver table
dataform_tags:                                 # Tags executed in Dataform workflow
  - netsuite
  - netsuite_account
schedule: "@daily"                             # Airflow cron expression (@daily, @hourly, etc.)
```

### Schema Definition Files
Located under `app/udp/configs/schema/<system>/<table_name>_schema.json`:
* Defines the explicit schema types (`INTEGER`, `STRING`, `FLOAT`, `TIMESTAMP`, `BOOLEAN`) used for schema enforcement and drift validation in Silver Dataform transformations.

### Onboarded Table Inventory (30 Entities)

| Source System | Total Entities | Entity Names |
| :--- | :--- | :--- |
| **NetSuite** | 11 | `account`, `budgets`, `classification`, `customer`, `department`, `entity`, `location`, `subsidiary`, `transaction`, `transactionline`, `vendor` |
| **Primavera P6** | 15 | `activity`, `activitycode`, `activitycodeassignment`, `activitycodetype`, `activityspread`, `epsspread`, `project`, `projectspread`, `refrdelete`, `resourceassignmentspread`, `udftype`, `udfvalue`, `wbs`, `wbscategory`, `wbsspread` |
| **Elementl Atlas** | 4 | `atlas_s1_01_population_density`, `atlas_s1_02_urban_areas`, `atlas_s1_06_quaternary_faults`, `atlas_s2_20_cooling_water_supply` |

---

## 9. Dataform Transformation Layer (Silver & Gold)

> [!NOTE]
> **Repository Boundary**: The actual `.sqlx` model definitions, assertions, and `workflow_settings.yaml` reside in the separate, dedicated Dataform repository: `gcp-dataform-transformations` (configured in `environments/*.json`).
> This repository (`elementl-data-ingestion-fw`) acts as the **Ingestion & Orchestration Controller** — it extracts data, lands Parquet into GCS Bronze, loads BigQuery Bronze tables, and triggers the compilation & execution of the remote Dataform repository via Airflow.

### Compilation & Invocation Model

Airflow interacts with Google Cloud Dataform via two dedicated operators:
1. `DataformCreateCompilationResultOperator`: Compiles the remote Git repository workspace (`gcp-dataform-transformations`) against the configured commitish (`stage` or `main`).
2. `DataformCreateWorkflowInvocationOperator`: Executes actions filtered strictly by the task’s `dataform_tags`.

### Silver Cleansing & Deduplication

In BigQuery Silver (`ds_silver`), Dataform SQLX scripts execute standardized transformations:
* **Deduplication**: Resolves duplicate records ingested across batches:
  ```sql
  QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY lastmodifieddate DESC, ingestion_timestamp DESC) = 1
  ```
* **Type Enforcement**: Casts Bronze string columns into strongly-typed timestamps, dates, and decimals based on the schema catalog.
* **Assertions**: Executes automated data quality tests (failed assertion records tracked in `ds_dataform_assertions`).

### Gold Cross-Table Spatial Enrichment

* Gold layers are provisioned as **materialized views or tables** in BigQuery (`ds_gold`).
* Example: `ds_gold.enriched_project_hazards` evaluates proximity between project construction sites and environmental hazards using BigQuery GIS functions:
  ```sql
  SELECT
    p.project_id,
    p.project_name,
    f.fault_name,
    ST_DISTANCE(p.geom, f.geom) AS distance_meters
  FROM ds_silver.stg_p6_projects p
  JOIN ds_silver.stg_atlas_s1_06_quaternary_faults f
    ON ST_DWITHIN(p.geom, f.geom, 50000)
  ```

---

## 10. Observability, Telemetry & BigQuery Audit Logging

### Audit Table Schema: `ds_operations.ingestion_execution_logs`

Every execution from Cloud Run Jobs automatically logs an audit row:

| Column Name | BigQuery Type | Description |
| :--- | :--- | :--- |
| `task_id` | `STRING` | Identifier of the ingested task (e.g. `netsuite_department`). |
| `execution_id` | `STRING` | Unique execution ID (`exec_<task_id>_<YYYYMMDDHHMMSS>`). |
| `source_type` | `STRING` | Originating source platform (`netsuite`, `p6`, `atlas`). |
| `status` | `STRING` | Execution outcome (`SUCCESS` or `FAILED`). |
| `records_extracted` | `INTEGER` | Number of records extracted from upstream API/DB. |
| `records_loaded` | `INTEGER` | Number of records appended into BigQuery Bronze table. |
| `bytes_processed` | `INTEGER` | Compressed Parquet byte size written to GCS Bronze. |
| `start_time` | `TIMESTAMP` | UTC start timestamp of the extraction. |
| `end_time` | `TIMESTAMP` | UTC completion timestamp. |
| `duration_seconds` | `FLOAT` | Total elapsed duration in seconds. |
| `log_timestamp` | `TIMESTAMP` | UTC timestamp when the audit record was written. |
| `error_message` | `STRING` | Error description / stack trace if status is `FAILED`. |

### Airflow XCom Callback Handshake

Because Cloud Run Jobs run asynchronously, `JobRunner` writes an outcome manifest to:
`gs://<bucket>/_job_results/<task_id>/<run_id>.json`

```json
{
  "task_id": "netsuite_account",
  "run_id": "manual__2026-09-18T11:09:57",
  "status": "SUCCESS",
  "records_extracted": 10,
  "bytes_written": 4120,
  "gcs_uris": ["gs://bkt-.../netsuite/raw/netsuite_account/dt=2026-09-18/data_110958.parquet"],
  "duration_seconds": 3.84
}
```
The Airflow DAG reads this JSON back to populate task XCom, enabling downstream operators to make conditional routing decisions based on extracted row counts.

---

## 11. Networking, Security & Infrastructure Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    Google Cloud Platform                    │
│                                                             │
│  ┌───────────────────────┐       ┌───────────────────────┐  │
│  │   Cloud Run Job /     │       │    Cloud Composer     │  │
│  │   Cloud Run Job       │       │    (Apache Airflow)   │  │
│  └───────────┬───────────┘       └───────────┬───────────┘  │
│              │                               │              │
│              ▼                               │              │
│  ┌───────────────────────┐                   │              │
│  │ Serverless VPC Access │                   │              │
│  │ (cr-conn-us-central1) │                   │              │
│  └───────────┬───────────┘                   │              │
│              │                               │              │
│              ▼                               │              │
│  ┌───────────────────────┐                   │              │
│  │   Cloud NAT Gateway   │                   │              │
│  │   (Static Egress IP)  │                   │              │
│  └───────────┬───────────┘                   │              │
│              │                               │              │
└──────────────┼───────────────────────────────┼──────────────┘
               │                               │
               │ Outbound via Static IP        │ IAM ID Token
               ▼                               ▼
     ┌──────────────────┐            ┌──────────────────┐
     │  NetSuite ERP    │            │ BigQuery / GCS / │
     │  & Oracle P6     │            │ Secret Manager   │
     │  IP Whitelist    │            │ IAM Auth         │
     └──────────────────┘            └──────────────────┘
```

### Serverless VPC Access & Static Cloud NAT Egress
* Cloud Run services are attached to a **Serverless VPC Access Connector** (`cr-conn-us-central1`).
* All outbound Internet traffic routes through a **Cloud NAT Gateway** configured with reserved static external IP addresses:
  * **Dev**: `136.112.137.11` (from `dev.json`)
  * **Staging**: `136.115.148.145` (from `staging.json`)
* This allows enterprise firewalls at NetSuite and Oracle P6 hosting facilities to whitelist a single static IP address for all Cloud Run requests.

### GCP Secret Manager Integration
No passwords, OAuth tokens, or private keys are stored in source code, Docker images, or environment variables. All secrets are stored in Google Cloud Secret Manager and accessed via runtime IAM roles.

---

## 12. Verification Suite & Operational Runbooks

### Running the Automated E2E Test

The framework includes a fully automated end-to-end integration test:
[`app/udp/utilities/test_udp_end_to_end.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/utilities/test_udp_end_to_end.py)

```bash
python3 app/udp/utilities/test_udp_end_to_end.py
```

### Verification Pipeline Sequence:
1. **GCS Config Discovery**: Saves test YAML to `gs://bkt-*-udp-configs/sources/` and verifies DAG factory discovery.
2. **Cloud Run Job Execution**: Dispatches containerized job with environment overrides.
3. **NetSuite <= 10 Records Constraint**: Asserts that records extracted do not exceed 10 records.
4. **GCS Parquet & BigQuery Landing**: Confirms Snappy Parquet landed in GCS Bronze, rows appended into `ds_bronze_*` BigQuery table, and audit row inserted into `ds_operations.ingestion_execution_logs`.

### Deploying Cloud Run Services

```bash
# Build and deploy a Cloud Run Job (e.g. suiteql-ingestion)
gcloud builds submit   --config=app/udp/cloud-run-jobs/suiteql-ingestion/cloud_build.yaml   app/udp/cloud-run-jobs
```

### Onboarding a New Source Table (Step-by-Step)

1. **Step 1 — Create Schema Definition**:
   Add `app/udp/configs/schema/<system>/<table_name>_schema.json` with field names and data types.
2. **Step 2 — Create Declarative YAML**:
   Add `app/udp/configs/sources/<system>/<table_name>.yaml`:
   ```yaml
   task_id: netsuite_my_new_table
   source_system: netsuite
   ingestion_job: suiteql-ingestion
   source_table: my_new_table
   primary_key: id
   where_clause: "ROWNUM <= 10"
   max_records: 10
   target_dataset: ds_bronze_netsuite
   target_table: netsuite_my_new_table
   silver_dataset: ds_silver
   silver_table: stg_netsuite_my_new_table
   dataform_tags:
     - netsuite
     - netsuite_my_new_table
   schedule: "@daily"
   ```
3. **Step 3 — Upload to GCS**:
   ```bash
   gcloud storage cp app/udp/configs/sources/<system>/<table_name>.yaml gs://bkt-*-udp-configs/sources/<system>/
   ```
4. **Step 4 — Automatic Activation**:
   Within 60 seconds, Airflow's DAG factory will discover the file from GCS and dynamically create `dag_udp_netsuite_my_new_table`.

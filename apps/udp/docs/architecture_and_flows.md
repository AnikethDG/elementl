# Elementl Unified Data Platform — Ingestion & Transformation Architecture Guide

An enterprise-grade, metadata-driven data replication and transformation framework built natively for **Google Cloud Platform (GCP)** implementing the **Medallion Architecture (Bronze / Raw → Silver / Conformed)**.

---

## 1. Executive Summary & Platform Capabilities

The Elementl Data Ingestion & Transformation Framework automates enterprise-scale data replication and curation across core corporate and geospatial siting systems:

1. **NetSuite ERP**: Financial transactions, chart of accounts, customers, vendors, and departments extracted via SuiteTalk SuiteQL with HMAC-SHA256 Token-Based Authentication (TBA).
2. **Oracle Primavera P6 (EPPM)**: Project scheduling, activities, work breakdown structures (WBS), and resource assignments extracted directly from the underlying relational database using Oracle JDBC (thin mode).
3. **Elementl Atlas**: Elementl's proprietary clean energy & nuclear siting and environmental intelligence platform, queried over HTTP (ArcGIS REST Feature Services, bulk geospatial file streaming, and tabular REST endpoints).

### Key Architectural Principles

- **True Medallion Architecture**: Strict separation between immutable raw ingestion (Bronze / Raw) and conformed, business-ready models (Silver) via **Google Dataform**.
- **Dual Ingestion Engines**: Pluggable compute routing using **Cloud Run Functions** for lightweight micro-batch tasks (<1M rows, <9 mins) and **Apache Beam on Cloud Dataflow** for distributed high-throughput datasets (≥1M rows).
- **One Composer DAG Per Ingestion Task**: Granular, isolated orchestration preventing pipeline blast radius and allowing independent scheduling and retries.
- **Zero-Downtime Automated Schema Evolution**: Dedicated dynamic schema resolution for NetSuite custom fields and Primavera P6 User-Defined Fields (UDFs).
- **Zero Hardcoded Secrets**: Just-in-time credential resolution from **Google Secret Manager** via IAM Workload Identity.
- **Transactional Idempotency**: Partition purging (`DELETE WHERE ingestion_date = '...'`) before append-loading to guarantee zero duplicate records on retries.

---

## 2. End-to-End Medallion Data Architecture

The architecture implements a multi-tier Google Cloud native Lakehouse pattern transitioning data from source systems through raw bronze landing to analytics-ready silver models.

---

## 3. Dual Ingestion Engines & Selection Matrix

The framework routes tasks between two complementary compute runtimes configured in each task's YAML metadata (`engine.type: 'cloud_run_function' | 'dataflow'`).

| Evaluation Criteria | Cloud Run Functions | Dataflow (Apache Beam) |
| :--- | :--- | :--- |
| **Target Volume** | Small to Medium (< 1,000,000 rows) | High to Massive (≥ 1,000,000 rows) |
| **Execution Window** | Short-lived (≤ 9 minutes max timeout) | Long-running multi-hour batch workflows |
| **Infrastructure Footprint** | Serverless, instant scale-to-zero, lowest cost | Managed autoscaling worker pool in private VPC subnet |
| **Typical Workloads** | NetSuite Accounts/Vendors, P6 Projects/WBS, Atlas Surface Water & Capable Faults | P6 Activity Spreads, Atlas Nationwide Population Grids, Full Historical Backfills |
| **Memory Capacity** | Up to 32 GB RAM per instance | Distributed RAM across multi-worker pool |

---

## 4. End-to-End Ingestion Flows by Source System

### 4.1 NetSuite ERP (SuiteQL & OAuth 1.0a TBA)

NetSuite ingestion leverages SuiteQL queries executed over HTTP POST with Token-Based Authentication (HMAC-SHA256 signature, nonce, timestamp, consumer key, token key).

#### Execution Sequence:
1. **Developer Specification**: Create BigQuery target schema JSON (`config/schemas/netsuite/`) and declarative task YAML (`config/sources/netsuite/`).
2. **DAG Generation**: Run `dag_generator.py` to compile `dag_ingest_netsuite_*.py`.
3. **Cloud Run Function Extraction**: Cloud Composer invokes the multi-source dispatcher with task parameters. The extractor queries SuiteQL with pagination (`LIMIT 1000 OFFSET n`), buffers records, and streams Snappy Parquet to `gs://landing-bucket/netsuite/{table}/`.
4. **Partition Purge**: Airflow deletes any existing landing records for the execution date:
   ```sql
   DELETE FROM `raw_netsuite.transactions` WHERE ingestion_date = '{{ ds }}';
   ```
5. **BigQuery Load**: `GCSToBigQueryOperator` executes with `WRITE_APPEND` and schema evolution options `ALLOW_FIELD_ADDITION` and `ALLOW_FIELD_RELAXATION`.
6. **Dataform Silver Transformation**: Airflow executes `DataformCreateWorkflowInvocationOperator` to curate raw records into `silver_netsuite.*`.

---

### 4.2 Oracle Primavera P6 EPPM (Relational JDBC Ingestion)

Primavera P6 connects directly to the enterprise relational database (Oracle / SQL Server) using JDBC thin drivers, avoiding slow and rate-limited EPPM REST web services.

#### Key Features:
- **Direct Database Extraction**: Connects via Oracle JDBC thin mode (`jdbc:oracle:thin:@//host:port/service`) or SQL Server JDBC.
- **Partitioned Batch Queries**: Parallel split readers partition large tables (e.g. `TASK` activities) by `task_id` or date windows.
- **Snake-Case Column Normalization**: Automatically transforms Oracle uppercase column names (`TASK_CODE`, `TARGET_START_DATE`) into clean BigQuery identifiers (`task_code`, `target_start_date`).

---

### 4.3 Elementl Atlas (Internal Siting Platform via HTTP)

Atlas is Elementl's internal clean energy and nuclear siting platform. Ingestion occurs over HTTP across three distinct data access patterns:

| Siting Access Pattern | Source System Type | Tier-1 Count / Total | Example Siting Criteria | Ingestion Mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **ArcGIS REST Feature Service** | GeoServices REST API | 10 Tier-1 / 63 Total | `S1-02`: Capable Faults Buffer Analysis | Cloud Run Function queries `/FeatureServer/0/query` with spatial envelope & offset pagination |
| **Bulk File Download** | Large Spatial / Census Files | 5 Tier-1 / 23 Total | `S1-01`: Population Density Thresholds | Dataflow streaming Beam pipeline downloads remote files directly to GCS Parquet shards |
| **Tabular Data API** | REST API Endpoints | 1 Tier-1 / 24 Total | `S2-20`: Surface Water Availability (Flowing) | Cloud Run Function calls HTTP JSON endpoint with date-based watermark filtering |

#### Capable Faults Regulatory Siting Example (`S1-02`):
- **Raw Landing**: `raw_atlas.capable_faults` contains fault segments, activity classification (Quaternary / Holocene), slip rate (mm/yr), and GeoJSON geometries.
- **Silver Conformed Model**: `silver_atlas.fct_capable_faults` classifies regulatory exclusionary buffer zones:
  ```sql
  CASE
    WHEN slip_rate_mm_yr >= 5.0 THEN 'TIER_1_RESTRICTED_5KM'
    WHEN slip_rate_mm_yr >= 1.0 THEN 'TIER_2_BUFFER_2KM'
    ELSE 'TIER_3_MONITORED_1KM'
  END AS exclusionary_buffer_tier
  ```

---

## 5. DAG Factory & Self-Healing Orchestration Lifecycle

### One DAG Per Ingestion Task Pattern
Instead of fragile, monolithic DAGs that fail as a single unit, the framework implements **one standalone Airflow DAG per entity** (e.g., `dag_ingest_netsuite_transactions.py`, `dag_ingest_p6_activities.py`, `dag_ingest_atlas_capable_faults.py`).

### CI/CD Code Propagation Lifecycle
1. **Local Authoring**: Engineers modify YAML task configurations, BigQuery JSON schemas, or Jinja templates.
2. **Local Verification**: Run `pytest tests/` (validating schema rules and Python bytecode syntax) and `npx dataform compile`.
3. **DAG Generation**: Run `python composer/dag_factory/dag_generator.py` to regenerate standalone Python DAG files.
4. **Git CI/CD Pipeline**: Cloud Build or GitHub Actions verifies test passing and synchronizes `composer/dags/`, `config/`, and `composer/plugins/` to the Composer environment GCS bucket.
5. **Self-Healing Generator DAG**: Cloud Composer runs `dag_framework_dag_generator.py` on schedule or on-demand, dynamically validating all metadata and re-rendering any missing or drifted DAGs inside the Airflow environment.

---

## 6. Standard TaskGroup Execution Trace

Every generated DAG encapsulates a robust, fault-tolerant execution graph executing the standardized task dependency lifecycle: Ingestion Group (Extract -> Purge Landing Partition -> Load to BigQuery Raw) followed by Dataform Silver Transformation and Audit Logging.

---

## 7. Dataform Raw → Silver Transformation Layer

Dataform transforms raw landing tables into analytics-ready Silver models applying automated cleaning, deduplication, and quality assertions.

### 7.1 Data Cleaning & Deduplication
1. **Windowed Deduplication**: Eliminates duplicate records resulting from pipeline retries:
   ```sql
   QUALIFY ROW_NUMBER() OVER(
     PARTITION BY primary_key
     ORDER BY SAFE_CAST(last_modified_date AS TIMESTAMP) DESC,
              SAFE_CAST(ingestion_timestamp AS TIMESTAMP) DESC
   ) = 1
   ```
2. **Safe Type Casting**: Enforces safe conversions with `SAFE_CAST` (preventing query aborts on malformed records) and default fallbacks (`COALESCE`).
3. **String Sanitization**: Trims leading/trailing whitespace and converts empty strings to `NULL` via `NULLIF(TRIM(column), '')`.
4. **Partitioning & Clustering**: Day-partitioned by transaction/event date and clustered by business keys.

### 7.2 Silver Models Overview

| Source System | Raw Table | Silver Model | Materialization | Partitioning / Clustering |
| :--- | :--- | :--- | :--- | :--- |
| **NetSuite** | `raw_netsuite.transactions` | `silver_netsuite.fct_transactions` | Incremental | `PARTITION BY transaction_date`, `CLUSTER BY transaction_id, transaction_type` |
| **NetSuite** | `raw_netsuite.accounts` | `silver_netsuite.dim_accounts` | Table | `CLUSTER BY account_id, account_type` |
| **NetSuite** | `raw_netsuite.customers` | `silver_netsuite.dim_customers` | Table | `CLUSTER BY customer_id, customer_entity_id` |
| **NetSuite** | `raw_netsuite.vendors` | `silver_netsuite.dim_vendors` | Table | `CLUSTER BY vendor_id, vendor_entity_id` |
| **NetSuite** | `raw_netsuite.departments` | `silver_netsuite.dim_departments` | Table | `CLUSTER BY department_id, department_name` |
| **Primavera P6**| `raw_p6.projects` | `silver_p6.dim_projects` | Table | `CLUSTER BY object_id, project_code` |
| **Primavera P6**| `raw_p6.activities` | `silver_p6.fct_activities` | Incremental | `PARTITION BY DATE(early_start_date)`, `CLUSTER BY project_object_id, object_id` |
| **Primavera P6**| `raw_p6.wbs` | `silver_p6.dim_wbs` | Table | `CLUSTER BY object_id, project_id` |
| **Primavera P6**| `raw_p6.resource_assignments` | `silver_p6.fct_resource_assignments` | Table | `CLUSTER BY object_id, activity_object_id` |
| **Elementl Atlas**| `raw_atlas.capable_faults` | `silver_atlas.fct_capable_faults` | Table | `CLUSTER BY fault_id, state` (S1-02 Fault Buffer) |
| **Elementl Atlas**| `raw_atlas.population_density` | `silver_atlas.dim_population_density` | Table | `CLUSTER BY geoid, state_fips` (S1-01 Density) |
| **Elementl Atlas**| `raw_atlas.surface_water` | `silver_atlas.fct_surface_water_availability` | Incremental | `PARTITION BY observation_date`, `CLUSTER BY station_id, huc_code` (S2-20 Water Flow) |

---

## 8. Automated Schema Evolution Engine (NetSuite & Primavera P6)

Enterprise ERP (**NetSuite**) and Project Portfolio Management (**Oracle Primavera P6**) systems evolve constantly as businesses introduce new attributes, custom fields, and regulatory tracking columns. The framework provides a **zero-downtime, fully automated schema evolution pipeline** engineered specifically for NetSuite and P6.

### Core Schema Evolution Mechanics

1. **BigQuery Dynamic Field Addition (`ALLOW_FIELD_ADDITION`)**:
   Every BigQuery batch load task generated by the DAG factory specifies automated schema evolution flags:
   ```json
   "schemaUpdateOptions": [
     "ALLOW_FIELD_ADDITION",
     "ALLOW_FIELD_RELAXATION"
   ]
   ```
   When upstream NetSuite or P6 schemas evolve, BigQuery dynamically appends the new column to the destination raw table without requiring table recreation or dropping historical data.

2. **Self-Describing Snappy Parquet on GCS**:
   Both Cloud Run Functions and Apache Beam Dataflow write strongly-typed Parquet files with embedded PyArrow schemas. BigQuery natively reads the Parquet file header schema and resolves newly projected attributes.

3. **Defensive Dataform SQLX Models**:
   Downstream Silver SQLX transformations protect analytical models against runtime schema drift:
   - **`SAFE_CAST(column AS TYPE)`**: Never crashes downstream queries if a source field contains unexpected format variations.
   - **`COALESCE(column, default_value)`**: Provides clean defaults for historical partitions ingested prior to the schema evolution event.

### Schema Evolution Compatibility Matrix

| Change Category | Evolution Scenario | Automation Level | Framework Behavior |
| :--- | :--- | :--- | :--- |
| **Additive** | NetSuite adds custom body/line field (`custbody_*`) | **Fully Automated** | Append field to YAML query & JSON schema. BigQuery auto-appends column on next run. |
| **Additive** | Primavera P6 adds UDF in `TASK` or `PROJECT` | **Fully Automated** | Add column to JDBC SQL & JSON schema. BigQuery dynamically creates column. |
| **Relaxation** | Field constraint loosened (REQUIRED → NULLABLE) | **Fully Automated** | `ALLOW_FIELD_RELAXATION` dynamically relaxes column in BigQuery. |
| **Incompatible**| Column Renamed or Dropped | **Managed Migration** | Keep old column in Silver model via `COALESCE(new_col, old_col)` to maintain backward compatibility. |
| **Incompatible**| Data Type Alteration (e.g. STRING to INT) | **Managed Migration** | Use `SAFE_CAST()` in Dataform Silver layer to prevent breaking downstream views. |

---

## 9. Observability, Auditing & Operational Runbook

### BigQuery Audit & Telemetry Schema
Every pipeline execution records end-to-end metrics into `audit_metadata.ingestion_execution_logs`:
- `pipeline_id`, `source_type`, `target_dataset`, `target_table`
- `execution_date`, `status` (`SUCCESS` / `FAILED`)
- `rows_extracted`, `bytes_extracted`, `execution_time_seconds`
- `error_message`, `created_timestamp`

### Developer Quick Start CLI

```bash
# 1. Run complete test suite (syntax, metadata validation, extractors)
PYTHONPATH=. .venv/bin/pytest tests/ -v

# 2. Compile Dataform transformation graph
cd dataform && npx dataform compile && cd ..

# 3. Generate all standalone Airflow DAGs
.venv/bin/python composer/dag_factory/dag_generator.py

# 4. Deploy Cloud Run Functions Dispatcher
./cloud_run_functions/deploy_function.sh liv-arc-dev-shared us-central1

# 5. Build Dataflow Flex Templates
./dataflow/build_templates.sh liv-arc-dev-shared us-central1 elementl-dataflow-templates-dev

# 6. Deploy GCP Infrastructure (Terraform)
cd terraform && terraform init && terraform apply
```

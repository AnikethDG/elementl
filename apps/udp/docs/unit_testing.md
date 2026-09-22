# Unit Testing Guide: Elementl Unified Data Platform (UDP)

This guide documents the unit testing architecture, test suites, mocking methodology, and execution instructions for the Elementl UDP data ingestion framework.

---

## 1. Overview & Testing Philosophy

The UDP unit testing framework is designed around five core principles:

1. **Zero External Dependencies**: Unit tests execute completely offline without requiring Google Cloud Platform access, live database connections (Oracle P6), or SaaS APIs (NetSuite SuiteQL, ArcGIS REST).
2. **Deterministic Speed**: The entire unit suite of 23 tests runs in under **4 seconds**, enabling rapid local iteration and fast CI/CD validation.
3. **Comprehensive Coverage**: Tests isolate and validate critical data contracts, type coercions, security key handoffs, fail-fast configuration validations, and Airflow dynamic DAG generation.
4. **Strict Enforcement of Business Invariants**:
   - **NetSuite Safety Cap**: Strict automated verification that row limits never exceed 10 records during testing/development.
   - **Jinja Placeholder Detection**: Prevention of runtime bugs caused by unrendered Airflow template strings (`{{ ds }}`) being passed to Cloud Run containers.
   - **P6 Schema Owner Fallback**: Validates resolution order across Oracle schemas (`ELEMENTL_PMDB_SBOX_PXRPTUSER` -> `ROADMUSER` -> `ADMUSER`).
5. **Isolation via Pytest Markers**: Unit tests are marked with `@pytest.mark.unit` to clearly differentiate them from live GCP `@pytest.mark.integration` tests.

---

## 2. Directory Structure

All unit tests reside in [`apps/udp/tests/unit/`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit):

```text
apps/udp/tests/
├── pytest.ini                      # Pytest config registering 'unit' and 'integration' markers
├── conftest.py                     # Shared fixtures, mock clients, and dummy test payloads
└── unit/
    ├── test_base_extractor.py      # Base extractor: Parquet write, date coercion, PEM decoding
    ├── test_bulk_extractor.py      # Bulk extractor: Census ACS parsing, USGS NWIS, state FIPS
    ├── test_jdbc_extractor.py      # JDBC extractor: P6 mapping, data sanitization, owner resolution
    ├── test_job_config.py          # JobConfig: Environment parsing, fail-fast rules, redaction
    ├── test_suiteql_extractor.py   # SuiteQL extractor: Account normalisation, <=10 cap, HATEOAS removal
    └── test_udp_dag_factory.py     # DAG Factory: YAML discovery, structured schema, Airflow DAG generation
```

---

## 3. Test Modules & Verification Scope

### Module 1: Base Extractor ([`test_base_extractor.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_base_extractor.py))
Tests the shared base class (`BaseExtractor`) inherited by all Cloud Run ingestion workers.

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_coerce_temporal_dates_and_timestamps` | Ensures Python `datetime.date` and `datetime.datetime` objects are normalized to ISO-8601 strings. | Temporal data consistency across heterogeneous sources. |
| `test_write_records_to_gcs_parquet` | Validates in-memory Snappy-compressed Apache Parquet file generation and mock GCS blob upload. | Correct file structure, column preservation, byte count calculation. |
| `test_write_records_to_gcs_jsonl` | Validates newline-delimited JSON (`JSONL` / `NDJSON`) export formatting. | Correct delimiter handling and string encoding. |
| `test_write_empty_records` | Verifies that an empty record set returns 0 count and 0 bytes without raising an exception. | Resilient pipeline behavior when source queries return empty deltas. |
| `test_resolve_pem_inline_vs_reference` | Verifies resolution precedence of private keys (inline PEM string vs. Secret Manager lookup). | Secure key handling without leaking keys to disk or environment variables. |

---

### Module 2: Bulk & Tabular Extractor ([`test_bulk_extractor.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_bulk_extractor.py))
Tests the Project Atlas bulk download worker (`BulkAtlasExtractor`) handling tabular JSON APIs and Census spatial archives.

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_state_fips_completeness` | Validates that all 52 US state and territory FIPS codes are present for Census TIGER iteration. | Guarantees complete nationwide coverage for bulk GIS block group ingestion. |
| `test_extract_census_json_matrix` | Validates matrix-to-record conversion of US Census Bureau ACS 5-Year demographic API payloads. | First-row headers mapped to dictionary keys with null safety. |
| `test_extract_usgs_nwis` | Validates parsing of USGS NWIS instantaneous streamflow measurements. | GeoLocation coordinate extraction (`latitude`, `longitude`) and value unwrapping. |

---

### Module 3: JDBC Oracle P6 Extractor ([`test_jdbc_extractor.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_jdbc_extractor.py))
Tests the Primavera P6 JDBC database extractor (`jdbc-ingestion`) connecting to Oracle over TCPS port 2484.

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_canonical_p6_table_mapping` | Verifies that all 15 P6 reporting tables map to their canonical BigQuery Bronze table names (`p6_*`). | Data lake table naming consistency. |
| `test_sanitize_value_types` | Tests sanitization of Oracle types: `decimal.Decimal` -> `int`/`float`, `bytes` -> `utf-8`, and LOB/CLOB readers. | Serialization safety for Apache Arrow and Parquet conversion. |
| `test_resolve_table_owner_fallback` | Validates fallback algorithm when querying `ALL_OBJECTS` for schema owner. | Resilient connectivity across different P6 sandbox user privileges. |

---

### Module 4: Job Configuration & Environment Contract ([`test_job_config.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_job_config.py))
Tests the container configuration parser (`JobConfig`) that unpacks Airflow `CloudRunExecuteJobOperator` container overrides.

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_valid_config_from_env` | Tests parsing of all standard container override environment variables. | Correct mapping of `TASK_ID`, `SOURCE_TYPE`, `GCS_BUCKET`, `GCS_PREFIX`, etc. |
| `test_missing_required_env_raises_value_error` | Ensures immediate failure when mandatory fields (`task_id`, `gcs_bucket`) are missing. | Fail-fast execution before container attempts network work. |
| `test_unrendered_jinja_template_rejected` | **Critical**: Rejects any config field containing literal `{{ ... }}` or `{% ... %}` strings. | Prevents silent bugs where Airflow DAG failed to render execution dates. |
| `test_invalid_query_params_json_fallback` | Verifies graceful fallback to empty dict when `QUERY_PARAMS` is malformed JSON. | Crash prevention on optional parameters. |
| `test_redacted_query_truncation` | Validates that `redacted()` truncates long SQL queries for safe logging to Cloud Logging. | Prevents log clutter and accidental credential leakage in execution logs. |

---

### Module 5: NetSuite SuiteQL Extractor ([`test_suiteql_extractor.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_suiteql_extractor.py))
Tests the NetSuite OAuth 2.0 M2M SuiteQL extractor (`suiteql-ingestion`).

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_account_id_normalisation` | Tests conversion of NetSuite sandbox account IDs (`1234567_SB1` -> `1234567-sb1`). | Conformance with SuiteTalk REST API URL host format. |
| `test_max_records_cap_enforcement` | **Mandatory**: Verifies that requested row counts are strictly clamped to <= 10. | Prevents unintended API rate limit exhaustion and sandbox overload. |
| `test_clean_drops_hateoas_links` | Verifies removal of NetSuite HATEOAS REST metadata (`links: [{rel: "self", ...}]`). | Ensures clean columnar schemas without nested navigation objects. |
| `test_query_record_clamping` | Tests SQL rewriting to append or adjust `ROWNUM <= 10` on incoming SuiteQL queries. | Guarantees server-side query bounding before execution. |

---

### Module 6: Dynamic DAG Factory ([`test_udp_dag_factory.py`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/apps/udp/tests/unit/test_udp_dag_factory.py))
Tests dynamic Airflow DAG generation from declarative YAML configurations.

| Test Case | Description | Verification Target |
| :--- | :--- | :--- |
| `test_discover_all_configs_local` | Scans `configs/sources/` and verifies discovery of all 30 source configurations. | 11 NetSuite, 15 P6, and 4 Atlas YAML files loaded successfully. |
| `test_create_udp_dag_structure` | Validates generation of Airflow DAG with `ingestion_group` and `dataform_group`. | Operator hierarchy, DAG ID format, default arguments, and tags. |
| `test_create_udp_dag_structured_schema` | Validates parsing of the standardized multi-section YAML schema (`source`, `engine`, `destination`, `orchestration`). | Seamless translation of nested metadata into Cloud Run container overrides. |

---

## 4. How to Run Unit Tests

### 4.1 Prerequisites
Activate the repository virtual environment:
```bash
cd /usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw
source .venv/bin/activate
```

### 4.2 Run All Unit Tests
```bash
.venv/bin/python3 -m pytest -m unit -v apps/udp/tests/unit
```

### 4.3 Run a Single Test Module
```bash
# Test JobConfig parsing and fail-fast validation
.venv/bin/python3 -m pytest -v apps/udp/tests/unit/test_job_config.py

# Test NetSuite SuiteQL record cap and account normalisation
.venv/bin/python3 -m pytest -v apps/udp/tests/unit/test_suiteql_extractor.py

# Test Dynamic DAG Factory YAML discovery
.venv/bin/python3 -m pytest -v apps/udp/tests/unit/test_udp_dag_factory.py
```

### 4.4 Run with Test Coverage
```bash
.venv/bin/python3 -m pytest -m unit --cov=apps/udp/cloud-run-jobs --cov=apps/udp/composer/dags -v apps/udp/tests/unit
```

---

## 5. Mocking Methodology & Test Fixtures

Unit tests use `unittest.mock` to stub Google Cloud client libraries:

- **Cloud Storage Mock**: Intercepts `storage.Client`, `Bucket`, and `Blob.upload_from_string` to inspect in-memory Parquet payloads without writing to GCS.
- **Secret Manager Mock**: Returns deterministic mock JSON configurations (`{"account_id": "1234567_SB1", "client_id": "test_id", ...}`) without making network calls.
- **Airflow Environment Fallback**: `udp_dag_factory.py` includes a safe fallback (`AIRFLOW_AVAILABLE = False`) allowing configuration discovery and validation to be tested even in environments where Apache Airflow is not locally installed.
- **Isolated Environment Variables**: Tests use `unittest.mock.patch.dict(os.environ, ...)` to guarantee zero pollution across test cases.

---

## 6. Continuous Integration (CI) Guidelines

In Cloud Build or GitHub Actions pipelines, the unit test step must execute prior to container builds or DAG deployments:

```yaml
# Example CI pipeline step
- name: 'python:3.11'
  entrypoint: 'bash'
  args:
    - '-c'
    - |
      pip install -r requirements.txt
      pytest -m unit -v --junitxml=test-results/unit.xml apps/udp/tests/unit
```

A non-zero exit code indicates a breaking schema change, missing configuration invariant, or failed validation rule and will abort the deployment pipeline immediately.

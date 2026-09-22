# Unified Data Platform (UDP) Application Layer (`app/udp/`)

Implements the consolidated Elementl Phase 2 architecture:
- **`environments/`**: Per-environment configuration (`dev.json`, `staging.json`, `production.json`).
- **`configs/sources/` & `configs/schema/`**: Declarative YAMLs and BigQuery schemas for Oracle Primavera P6 (`ELEMENTL_PMDB_SBOX_PXRPTUSER`, 15 tables), Oracle NetSuite (`SuiteQL`, 11 tables), and Atlas (`ArcGIS` & `Bulk`, 4 layers).
- **`composer/dags/`**: Dynamic Airflow DAG factory generating 2-TaskGroup DAGs (Cloud Run Job Ingestion + Dataform Silver Transformation).
- **`cloud-run-jobs/`**: Containerized extraction workers (`arcgis-ingestion`, `bulk-ingestion`, `suiteQL-ingestion`, `jdbc-ingestion`).
- **`tests/`**: Automated Unit ([`app/udp/tests/unit/`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/tests/unit/)) and Live GCP Integration ([`app/udp/tests/integration/`](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/tests/integration/)) test suites.
- **`docs/unit_testing.md`**: Complete [Unit Testing Guide](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/docs/unit_testing.md) covering mock methodology, test suites, execution commands, and CI/CD validation.
- **`docs/integration_testing.md`**: Complete [Integration Testing Guide](file:///usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp/docs/integration_testing.md) detailing live GCP infrastructure verification, Cloud Run execution, and Airflow orchestration on GCP project `elementl-509009`.


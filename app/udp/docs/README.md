# Unified Data Platform (UDP) Application Layer (`app/udp/`)

Implements the consolidated Elementl Phase 2 architecture:
- **`environments/`**: Per-environment configuration (`dev.json`, `staging.json`, `production.json`).
- **`configs/sources/` & `configs/schema/`**: Declarative YAMLs and BigQuery schemas for Oracle Primavera P6 (`ELEMENTL_PMDB_SBOX_PXRPTUSER`, 15 tables), Oracle NetSuite (`SuiteQL`, 11 tables), and Atlas (`ArcGIS` & `Bulk`, 4 layers).
- **`composer/dags/`**: Dynamic Airflow DAG factory generating 2-TaskGroup DAGs (Cloud Run Job Ingestion + Dataform Silver Transformation).
- **`cloud-run-jobs/`**: Containerized extraction workers (`arcgis-ingestion`, `bulk-ingestion`, `suiteQL-ingestion`, `jdbc-ingestion`).

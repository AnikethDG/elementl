# Engine conventions (read before writing any transformation)

These rules apply to every engine folder (`bigquery/`, `dataform/`, `spark/`, `cloud-run-jobs/`).
The orchestrator (`udp_dag_factory.py` + `udp_silver_engines.py` in `elementl-nse-core-apps`) relies on them.

## Datasets and naming

| Layer | Dataset (ani-labs) | Table prefix | Meaning |
|---|---|---|---|
| Bronze (input, read-only) | `elementl_bronze` | `netsuite_<table>` | Raw, all-STRING columns, 'T'/'F' booleans, `M/D/YYYY` dates, may hold several versions of one id. |
| Silver (output) | `elementl_silver` | `stg_` 1:1 conformed · `dim_` dimension · `fct_` fact · `agg_` aggregate · `rej_` quarantined rows | Typed, deduplicated, business-ready. |
| Assertions (Dataform) | `elementl_silver_assertions` | — | Dataform assertion views. |

Bronze audit columns on every table: `ingestion_date DATE`, `ingestion_timestamp TIMESTAMP`.
Silver tables MUST add: `_silver_loaded_at TIMESTAMP` (run time) and `_silver_run_id STRING` (Airflow run_id or job id).

## Parameters every engine receives

| Parameter | BigQuery SQL placeholder | Spark arg | Cloud Run env | Dataform var |
|---|---|---|---|---|
| GCP project | `${project}` | `--project` | `GCP_PROJECT` | `udp_project` |
| Bronze dataset | `${bronze_dataset}` | `--bronze-dataset` | `BRONZE_DATASET` | `bronze_dataset` |
| Silver dataset | `${silver_dataset}` | `--silver-dataset` | `SILVER_DATASET` | `silver_dataset` |
| Logical date (YYYY-MM-DD) | `${ds}` | `--ds` | `EXECUTION_DATE` | — |
| Run id | `${run_id}` | `--run-id` | `RUN_ID` | — |
| Output table (fully qualified) | `${output}` | `--output-table` | `OUTPUT_TABLE` | — |
| Free-form params (`transformation.params` in YAML) | `${params.<key>}` | `--params '<json>'` | `PARAMS` (JSON) | `vars` |

Placeholders use `${...}` (never `{{ }}`: the factory passes strings through Airflow Jinja).

## Type conversion rules (NetSuite bronze → silver)

| Bronze | Silver expression |
|---|---|
| `'T'` / `'F'` | `col = 'T'` → BOOL |
| numeric string | `SAFE_CAST(col AS NUMERIC)` (money) or `INT64` (ids/counts) — keep `id` columns as STRING |
| `M/D/YYYY` | `SAFE.PARSE_DATE('%m/%d/%Y', col)` (BigQuery accepts non-padded) |
| `M/D/YYYY H:MM:SS AM` | `SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', col)` |
| JSON string | `SAFE.PARSE_JSON(col)` then `JSON_VALUE(...)` |

Every transformation that keeps one row per business key MUST dedupe bronze versions first:
latest `ingestion_timestamp` wins (tie-break on parsed `lastmodifieddate`).

## Idempotency

Re-running the same `ds` / `run_id` must not duplicate rows:
- full rebuilds: `CREATE OR REPLACE TABLE` / `mode("overwrite")` / Dataform `type: table`
- incremental: `MERGE` on the business key, or partition overwrite on `ingestion_date = ${ds}`

## Quality

Each silver table ships with at least: row count > 0, and a uniqueness check on its key.
Those are declared in the YAML `quality.checks` block and run by the factory after the engine finishes.

## Repo layout

```
bronze-to-silver/
  docs/DESIGN.md                 design document
  docs/CONVENTIONS.md            this file
  testdata/seed_bronze.sql       synthetic bronze (16 tables)
  bigquery/sql/<name>.sql        one script per silver table (BigQuery scripting allowed)
  dataform/                      Dataform project root (workflow_settings.yaml, definitions/, includes/)
  spark/jobs/<name>.py           one PySpark job per silver table
  spark/lib/udp_silver_common.py shared helpers (arg parsing, typed readers, writer)
  cloud-run-jobs/silver-transform/  one image, TRANSFORM env selects the python module
  deploy/                        scripts that push artifacts to GCS / Artifact Registry / Dataform
  tests/                         offline unit tests per engine
```

## Quality checks (YAML `quality.checks`)

- Derive every check from the **engine payload's documented contract** (SQL header / sqlx `description` / job docstring), never from the table name. Example: `stg_netsuite_item_tags` keeps tag-less items as a row with `tag` NULL by design, so "tag not null" is the wrong check; the right one asserts `(tag IS NULL) = (tag_position IS NULL)`.
- Composite-key uniqueness: use `COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT a, b FROM `${output}`))`. Do **not** use `COUNT(DISTINCT CONCAT(CAST(a AS STRING), CAST(b AS STRING)))` — without a separator `(1,12)` and `(11,2)` collide.
- Each check is a scalar query that must return `expect` (default 0); keep them cheap (single scan of `${output}`).

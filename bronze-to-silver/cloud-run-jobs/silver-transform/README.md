# silver-transform (Cloud Run Jobs engine)

One container image; the `TRANSFORM` env var selects the Python module under `transforms/` that
turns NetSuite-shaped bronze tables into one silver table. Used by the UDP bronze-to-silver
orchestrator for transformations that need procedural Python (date generation, fuzzy matching,
statistics) rather than SQL. Follows [`docs/CONVENTIONS.md`](../../docs/CONVENTIONS.md).

## Layout

```
main.py              env contract -> Context -> transforms.<TRANSFORM>.run(ctx) -> JSON summary
common.py            read_table, latest_version, parse_ns_date/timestamp, tf_to_bool, to_decimal, write_silver
transforms/
  fiscal_calendar.py        -> dim_netsuite_fiscal_calendar      (1 row per day, key: date)
  address_enrichment.py     -> stg_netsuite_address_enriched     (key: address_id)
  customer_domains.py       -> stg_netsuite_customer_domains     (key: customer_id)
  transaction_anomalies.py  -> fct_netsuite_transaction_anomalies (key: line_id)
  budget_vs_actual.py       -> fct_netsuite_budget_vs_actual     (key: account, department, year, month)
Dockerfile, requirements.txt (pinned), .dockerignore
```

## Env contract (set by the orchestrator)

| Var | Meaning | Example |
|---|---|---|
| `TRANSFORM` | module name under `transforms/` | `fiscal_calendar` |
| `GCP_PROJECT` | project | `ani-labs` |
| `BRONZE_DATASET` | bronze dataset (read-only) | `elementl_bronze` |
| `SILVER_DATASET` | silver dataset | `elementl_silver` |
| `OUTPUT_TABLE` | `project.dataset.table`; defaults to `<project>.<silver>.<DEFAULT_TABLE>` | `ani-labs.elementl_silver.dim_netsuite_fiscal_calendar` |
| `EXECUTION_DATE` | logical date `YYYY-MM-DD` | `2026-10-07` |
| `RUN_ID` | Airflow run_id; written to `_silver_run_id` | `manual__2026-10-07T00:00:00+00:00` |
| `PARAMS` | JSON object (default `{}`) available as `ctx.params` | `{"z_threshold": 3}` |

`GCP_PROJECT`, `BRONZE_DATASET`, `SILVER_DATASET` are baked into the job at deploy time; the
orchestrator passes the per-run ones with `--update-env-vars "^|^TRANSFORM=...|OUTPUT_TABLE=...|..."`
(the `^|^` prefix changes the delimiter so `PARAMS` may contain commas).

## Behaviour

* Every transform dedupes bronze versions with `latest_version()` (latest `ingestion_timestamp`,
  tie-break parsed `lastmodifieddate`).
* `write_silver()` adds `_silver_loaded_at TIMESTAMP` + `_silver_run_id STRING`, loads from a
  pandas DataFrame via Parquet with an **explicit schema** (`autodetect=False`) and
  `WRITE_TRUNCATE`, so re-running the same `ds`/`run_id` rebuilds the table (idempotent).
  NUMERIC columns are shipped as `Decimal` (quantised to 9 dp), DATE as `datetime.date`,
  TIMESTAMP tz-aware UTC - pyarrow maps these 1:1 onto BigQuery types.
* Exit code: `0` success, `1` transform failure, `2` configuration error. The last log line is
  `SILVER_SUMMARY {"transform", "output_table", "rows_written", "duration_s", "status"}`.

## Build, deploy, run

```bash
deploy/cloud_run.sh build                      # gcloud builds submit -> :<tag> and :latest
deploy/cloud_run.sh deploy                     # gcloud run jobs deploy silver-transform (create/update)
deploy/cloud_run.sh run fiscal_calendar 2026-10-07   # execute + --wait
deploy/cloud_run.sh logs                       # logs of the last execution
```

Job spec: name `silver-transform`, region `us-central1`, SA `5473845300-compute@developer.gserviceaccount.com`
(BigQuery dataEditor + jobUser), `--max-retries 0`, `--task-timeout 30m`, 1 task, 2 GiB.

## Adding a transform

Create `transforms/<name>.py` with `DEFAULT_TABLE`, a `SCHEMA` list of `common.field(...)` and
`def run(ctx) -> int` that reads with `ctx.read_bronze(...)` / `ctx.read_table(sql)` and returns
`ctx.write_silver(df, SCHEMA)`. Keep the business logic in pure functions that take DataFrames so
the offline tests in `tests/cloud_run/` can exercise them without BigQuery.

## Tests (offline)

```bash
python3 -m venv .venv && .venv/bin/pip install -r cloud-run-jobs/silver-transform/requirements.txt pytest
.venv/bin/python -m pytest tests/cloud_run -q
```

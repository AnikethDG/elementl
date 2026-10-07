# Spark / PySpark engine (Dataproc Serverless)

One PySpark job per silver table, all sharing [`lib/udp_silver_common.py`](lib/udp_silver_common.py).
Jobs read bronze with the spark-bigquery connector (built into Dataproc Serverless runtime 2.2),
dedupe with `latest_version`, apply the NetSuite type rules from
[`docs/CONVENTIONS.md`](../docs/CONVENTIONS.md), append `_silver_loaded_at` / `_silver_run_id`
and `mode("overwrite")` the output table (full rebuild => idempotent).

## Jobs

| Job (`jobs/<name>.py`) | Output table (`elementl_silver.`) | Key | What it does |
|---|---|---|---|
| `fct_account_running_balance` | `fct_netsuite_account_running_balance` | `line_id` | `netsuite_transactionline` (mainline='F') ⋈ latest `netsuite_transaction` (posting='T'); `running_balance` = window sum of `netamount` per `account_id` ordered by `trandate, transaction id, line id` |
| `stg_item_tags` | `stg_netsuite_item_tags` | `(item_id, tag)` | `regexp_extract_all('#(\w+)')` on `description`, `posexplode`; tag-less items keep one row with `tag` NULL; `displayname` trimmed/collapsed/title-cased; `baseprice` NUMERIC, `isinactive` BOOL |
| `stg_employee_masked` | `stg_netsuite_employee` | `employee_id` | `email_sha256`, `phone_masked` (`***-***-1234`), `initials` instead of names, `hiredate` DATE, `tenure_days` as of `--ds`, department/supervisor kept |
| `fct_transaction_usd` | `fct_netsuite_transaction_usd` | `transaction_id` | latest transaction version; as-of join (broadcast + window) to `netsuite_currencyrate` with greatest `effectivedate <= trandate`; USD => rate 1.0; `amount_usd = foreigntotal * rate` |
| `dim_classification_path` | `dim_netsuite_classification_path` | `id` | iterative self-join flatten (max 10 levels): `root_id`, `level` (1 = root), `path` (`Class 1 > Class 4 > Class 13`), `id_path` (`1/4/13`), `is_leaf`, `isinactive` BOOL |

Optional `--params` keys: `fct_transaction_usd`: `base_currency` (default `USD`);
`dim_classification_path`: `max_levels` (default `10`). All other jobs ignore params.

## CLI contract (every job)

```
--project <p> --bronze-dataset <d> --silver-dataset <d> \
--output-table <project.dataset.table> --ds <YYYY-MM-DD> --run-id <string> --params '<json object>'
```

Helpers in `udp_silver_common`: `parse_args()`, `get_spark()`, `read_bronze(spark, table)`,
`latest_version(df, key_cols)` (latest `ingestion_timestamp`, tie-break parsed `lastmodifieddate`),
`parse_ns_date`, `parse_ns_timestamp`, `tf_to_bool`, `to_numeric` (decimal(38,9) => NUMERIC),
`write_silver(df, output_table, mode)` (adds the two audit columns, `writeMethod=direct`).

## GCS layout

```
gs://bkt-ani-labs-udp-configs/silver/spark/lib/udp_silver_common.py   (--py-files)
gs://bkt-ani-labs-udp-configs/silver/spark/jobs/<name>.py              (main python file)
gs://bkt-ani-labs-udp-configs/silver/spark/tmp/                        (only for writeMethod=indirect)
```

## Deploy and run

```bash
deploy/spark.sh                       # py_compile + upload lib and jobs (idempotent)
deploy/spark.sh list                  # job -> output table
deploy/spark.sh run stg_item_tags     # submit + wait; ds defaults to today, run_id manual__<ts>
deploy/spark.sh run fct_transaction_usd --ds 2026-10-07 --run-id manual__x --params '{"base_currency":"USD"}'
deploy/spark.sh run dim_classification_path --no-wait
```

## Exact Dataproc Serverless batch spec (orchestrator should mirror this)

| Setting | Value |
|---|---|
| Project / region | `ani-labs` / `us-central1` |
| Runtime version | `2.2` (Spark 3.5, spark-bigquery connector built in – do **not** add `spark.jars.packages`) |
| Main python file | `gs://bkt-ani-labs-udp-configs/silver/spark/jobs/<name>.py` |
| `--py-files` | `gs://bkt-ani-labs-udp-configs/silver/spark/lib/udp_silver_common.py` |
| Subnetwork | `acai-network` (Private Google Access on) |
| Service account | `5473845300-compute@developer.gserviceaccount.com` (dataproc.worker, bigquery.dataEditor + jobUser, storage.objectAdmin) |
| Args | `--project ani-labs --bronze-dataset elementl_bronze --silver-dataset elementl_silver --output-table ani-labs.elementl_silver.<table> --ds {{ ds }} --run-id {{ run_id }} --params '<json>'` |
| Labels | `datacloud=antigravity,udp-engine=spark,udp-job=<name-with-dashes>` |

gcloud equivalent:

```bash
gcloud dataproc batches submit pyspark gs://bkt-ani-labs-udp-configs/silver/spark/jobs/<name>.py \
  --project ani-labs --region us-central1 --version 2.2 \
  --subnet acai-network --service-account 5473845300-compute@developer.gserviceaccount.com \
  --py-files gs://bkt-ani-labs-udp-configs/silver/spark/lib/udp_silver_common.py \
  -- --project ani-labs --bronze-dataset elementl_bronze --silver-dataset elementl_silver \
     --output-table ani-labs.elementl_silver.<table> --ds 2026-10-07 --run-id <run_id> --params '{}'
```

Airflow: `DataprocCreateBatchOperator(batch={"pyspark_batch": {"main_python_file_uri": ..., "python_file_uris": [lib], "args": [...]}, "runtime_config": {"version": "2.2"}, "environment_config": {"execution_config": {"subnetwork_uri": "acai-network", "service_account": "..."}}}, region="us-central1")`.

## Gotchas

* Batches take ~2–4 min to start (serverless provisioning) before any job log appears; the jobs
  themselves run in well under a minute on the seed data.
* `writeMethod=direct` + `mode("overwrite")` needs no staging bucket and replaces the table
  atomically (BigQuery Storage Write API). Use `write_silver(..., write_method="indirect",
  temporary_gcs_bucket="bkt-ani-labs-udp-configs")` only if direct writes are ever disallowed.
* NUMERIC: cast to `decimal(38,9)` (`to_numeric`); larger precision becomes BIGNUMERIC.
* Sessions run with `spark.sql.ansi.enabled=false` so bad dates / numbers become NULL
  (SAFE_CAST semantics) instead of failing the batch.
* `--params` must be a JSON *object*; the shell quoting in `deploy/spark.sh` passes it as one argv.
* Driver output of a failed batch: `gcloud dataproc batches describe <id> --region us-central1
  --format 'value(runtimeInfo.outputUri)'` then `gcloud storage cat <uri>*`.

## Tests

```bash
python3 -m venv .venv && .venv/bin/pip install pytest   # (pyspark optional)
.venv/bin/python -m pytest tests/spark -q
```

Pure-Python mirrors of the Spark helpers (`*_py`) are tested offline; the Spark parity test runs
only when pyspark is importable, otherwise it is skipped (the Spark expressions are verified by the
Dataproc runs).

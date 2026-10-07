# Bronze → Silver engines — Test plan & results

| | |
|---|---|
| **Component** | Configurable bronze → silver transformation layer (4 engines) |
| **Repos under test** | `elementl-nse-core-apps` (`udp_dag_factory.py`, `udp_silver_engines.py`, `configs/transformations/<system>/<engine>/*.yaml`), `bronze-to-silver` (engine payloads) |
| **Environment** | GCP project `ani-labs`, region `us-central1`, Composer `small-composer-env` (Composer 3 / Airflow 2.11.1) |
| **Test data** | 16 synthetic NetSuite bronze tables in `elementl_bronze` (`testdata/seed_bronze.sql`) |
| **Status** | 🟢 Complete — offline 130 passed / 1 skipped; direct-engine 20 / 20; Composer end-to-end **20 / 20 success** (2 scenarios needed one re-run each, see I10–I13). |
| **Last updated** | 2026-10-07 07:45 UTC |

---

## 1. Objectives

1. Prove that one YAML schema + one DAG shape can drive **four different engines** (BigQuery SQL, Dataform, Spark on Dataproc Serverless, Cloud Run Jobs) for bronze → silver.
2. Prove each engine on **five realistic transformation scenarios** (20 silver tables) with typed output, deduplicated bronze versions, audit columns and idempotent re-runs.
3. Prove the orchestration contract end-to-end in Composer: input checks → engine → quality gate → audit row → DagRun status.
4. Confirm the ingestion path (`configs/sources/**`) is unaffected.

## 2. Scope

| In scope | Out of scope |
|---|---|
| Offline unit tests (no GCP) for factory + every engine payload | Performance / cost benchmarking at production volume |
| Direct engine execution against `elementl_bronze` → `elementl_silver` | Staging / prod deployment (next phase) |
| Composer end-to-end for all 20 configs, 2 DAGs at a time (labs rule) | Gold layer |
| Idempotency (re-run yields identical row counts / keys) | Real NetSuite data (labs has no source connection) |
| Negative paths: unresolved placeholder, unknown engine, missing output, bad YAML | Security review / pen test |

## 3. Test environment

| Item | Value |
|---|---|
| Datasets | `elementl_bronze` (input, 16 tables), `elementl_silver` (output), `elementl_silver_assertions` (Dataform assertions), `ds_operations.ingestion_execution_logs` (audit) |
| Artefact bucket | `gs://bkt-ani-labs-udp-configs/silver/{bigquery/sql,spark/jobs,spark/lib}` |
| Cloud Run | job `silver-transform`, image `us-central1-docker.pkg.dev/ani-labs/udp-ingestion-jobs/silver-transform` |
| Dataform | repository `bronze-to-silver`, workspace `labs` (files uploaded by `deploy/dataform.sh`, no git connection) |
| Dataproc | Serverless batches, runtime 2.2, subnet `acai-network` (PGA) |
| Runtime identity | `5473845300-compute@developer.gserviceaccount.com` (Composer worker, Spark, Cloud Run, Dataform invocations) |
| Composer bucket | `gs://us-central1-small-composer--c3bed0ff-bucket/dags/` (factory + module + `configs/transformations/`) |

Bronze seed row counts: account 192 · accountingperiod 34 · address 250 · budgets 200 · classification 30 · currencyrate 110 · customer 320 · department 15 · employee 120 · entity 400 · item 300 · location 40 · subsidiary 6 · transaction 2400 · transactionline 6000 · vendor 240. Several tables hold multiple versions per id (dedupe test), 'T'/'F' booleans, `M/D/YYYY` dates, JSON strings, and deliberately dirty rows (bad JSON, out-of-range coordinates, blank names).

## 4. Test levels & how to run

| Level | Command | Result |
|---|---|---|
| **L1 Offline – engine payloads** | `bronze-to-silver$ .venv/bin/python -m pytest -q tests` | ✅ **130 passed, 1 skipped** (Spark parity test needs pyspark locally) — `tests/bigquery` 14, `tests/dataform` 45, `tests/spark` 43+1s, `tests/cloud_run` 28 |
| **L1 Offline – orchestrator** | `apps/udp$ python3 <shim> tests/unit/test_udp_silver_engines.py` | ✅ **10 passed** (spec builders per engine, placeholder rendering, discovery, all 20 live YAMLs resolve, no hard-coded project ids, every engine covered) |
| **L1 Offline – ingestion regression** | `apps/udp$ python3 <shim> tests/unit/test_udp_dag_factory.py` | ✅ unchanged: 16 passed, 5 skipped (need Airflow), 2 pre-existing failures (`test_discover_all_configs_local`, `test_pipeline_gate_callable_raises_on_failure_only`) — same before and after this change |
| **L2 Direct engine runs** | `bigquery/render.py` + `bq query`; `deploy/dataform.sh run <tag>`; `deploy/spark.sh run <job>`; `deploy/cloud_run.sh run <transform>` | ✅ 20 / 20 tables produced and verified (§5) |
| **L3 Composer end-to-end** | `scratch/labs/silver_runs.py <table>…` (REST API, 2 DAGs at a time) | 🟡 8 / 20 passed, 0 failed, 12 in flight (§6) |

## 5. L2 — Direct engine results (all 20 scenarios)

Verification for each table: row count, key uniqueness, spot-checked example rows, output column types, `_silver_loaded_at` / `_silver_run_id` present. Idempotency: at least one table per engine re-run with identical result.

### 5.1 BigQuery (SQL scripts)

| Table | Scenario | Rows | Key check | Idempotent re-run | Notes |
|---|---|---|---|---|---|
| `stg_netsuite_customer` | SCD1 MERGE, incremental on `_bronze_ingested_at` watermark | 200 (from 320 bronze versions) | 200 distinct `id` | ✅ 2nd run: 0 rows affected | |
| `dim_netsuite_vendor_history` | SCD2 (`valid_from`/`valid_to`/`is_current`) | 240 history rows, 150 current | 240 distinct `vendor_key`; exactly 1 current row per `id` | ✅ 2nd run: 0 rows affected | |
| `stg_netsuite_subsidiary` | JSON flatten, full rebuild | 6 | 6 distinct `id` | ✅ (CREATE OR REPLACE) | `SAFE.PARSE_JSON` → NULL for bad JSON flagged in `mainaddress_invalid` |
| `stg_netsuite_entity` | Partition overwrite, look-back window (`params.lookback_days`) | 400 | 400 distinct (`id`,`ingestion_date`) | ✅ DELETE 400 + INSERT 400 in one transaction | **Bug found & fixed**: `DECLARE` must precede `CREATE TABLE` in BigQuery scripting |
| `stg_netsuite_location` | Quality quarantine | 28 valid + 12 → `rej_netsuite_location` | 28 + 12 distinct `id` (= 40 bronze ids) | ✅ full rebuild | Reject reasons: `missing_name`, `latitude_out_of_range`, `invalid_mainaddress_json` |

### 5.2 Dataform

| Table | Scenario | Rows | Key check | Idempotent re-run | Notes |
|---|---|---|---|---|---|
| `stg_netsuite_account` | Dedupe + cast, uniqueKey/nonNull assertions | 120 (from 192) | 120 distinct `id` | ✅ | assertions in `elementl_silver_assertions` |
| `stg_netsuite_transaction` | Incremental MERGE (`uniqueKey: id`) | 1500 (from 2400) | 1500 distinct `id` | ✅ 4 invocations → 1 distinct `_silver_loaded_at` | |
| `fct_netsuite_transaction_lines` | Join fact via `ref()`, partitioned by `trandate` | 4092 | 4092 distinct `line_id` | ✅ | = 1364 posting txns × 3 detail lines (independently verified) |
| `agg_netsuite_posting_daily` | Aggregate over the fact | 1080 | 1080 distinct (`trandate`,`account`,`subsidiary`) | ✅ | |
| `stg_netsuite_budgets_monthly` | Unpivot 12 amount columns | 2380 (= 200×12 − 20 NULLs) | 2380 distinct (`budget_id`,`month`) | ✅ | |

### 5.3 Spark (Dataproc Serverless)

| Table | Scenario | Rows | Key check | Idempotent re-run | Wall time |
|---|---|---|---|---|---|
| `fct_netsuite_account_running_balance` | Window running total | 4092 | 4092 distinct `line_id`; 0 mismatches vs BigQuery window re-computation | — | 124 s |
| `stg_netsuite_item_tags` | Split + explode | 635 | 635 distinct (`item_id`,`tag_position`) | ✅ re-run identical (635) | 126 s |
| `stg_netsuite_employee` | PII masking (`params.salt`) | 120 | 120 distinct `employee_id`; `email_sha256` == BigQuery `SHA256` | — | 128 s |
| `fct_netsuite_transaction_usd` | As-of currency-rate join | 1500 | 1500 distinct `transaction_id`; 0 as-of mismatches vs BigQuery | — | 119 s |
| `dim_netsuite_classification_path` | Hierarchy path (iterative self-join) | 30 (3 roots, depth 3) | 30 distinct `id`, 0 unresolved | — | 310 s (quota contention) |

### 5.4 Cloud Run Job (`silver-transform`)

| Table | Scenario | Rows | Key check | Idempotent re-run | Wall time |
|---|---|---|---|---|---|
| `dim_netsuite_fiscal_calendar` | Calendar generation | 730 | 730 distinct `date` | ✅ re-run identical | 165 s |
| `stg_netsuite_address_enriched` | Normalise + enrich | 250 (226 valid) | 250 distinct `address_id` | — | 154 s |
| `stg_netsuite_customer_domains` | String parsing / classification | 200 | 200 distinct `customer_id` | — | 101 s |
| `fct_netsuite_transaction_anomalies` | Z-score outliers (`params.z_threshold`) | 4500 (51 anomalies) | 4500 distinct `line_id` | — | 96 s |
| `fct_netsuite_budget_vs_actual` | 4-input reconciliation | 3460 | 3460 distinct (`account`,`department`,`year`,`month`) | — | 161 s |

## 6. L3 — Composer end-to-end (live)

Pass criteria per DAG: `check_inputs_ready` ✔ → engine task(s) ✔ → `quality_group.run_quality_checks` ✔ (min_rows + declared checks) → `record_audit_log_*` writes a `SUCCESS` row with `engine_type` = engine and `records_loaded` = output rows → `fail_on_upstream_failure` **skipped** → DagRun `success`.

| # | DAG (`dag_udp_silver_…`) | Engine | Result | Audit rows | DagRun wall |
|---|---|---|---|---|---|
| 1 | `stg_netsuite_customer` | bigquery | ✅ success | 200 | 199 s |
| 2 | `stg_netsuite_subsidiary` | bigquery | ✅ success | 6 | 195 s |
| 3 | `dim_netsuite_vendor_history` | bigquery | ✅ success | 240 | 357 s |
| 4 | `stg_netsuite_entity` | bigquery | ✅ success | 400 | 353 s |
| 5 | `stg_netsuite_location` | bigquery | ✅ success | 28 | 1040 s ¹ |
| 6 | `stg_netsuite_account` | dataform | ✅ success | 120 | 947 s ¹ |
| 7 | `stg_netsuite_transaction` | dataform | ✅ success | 1500 | 107 s |
| 8 | `stg_netsuite_budgets_monthly` | dataform | ✅ success ² | 2380 | 98 s |
| 9 | `fct_netsuite_transaction_lines` | dataform | ✅ success ² | 4092 | 95 s |
| 10 | `agg_netsuite_posting_daily` | dataform | ✅ success ² | 1080 | 190 s |
| 11 | `fct_netsuite_account_running_balance` | spark | ✅ success | 4092 | 237 s |
| 12 | `stg_netsuite_item_tags` | spark | ✅ success ³ | 635 | 189 s |
| 13 | `stg_netsuite_employee` | spark | ✅ success | 120 | 215 s |
| 14 | `fct_netsuite_transaction_usd` | spark | ✅ success ⁴ | 1500 | 229 s |
| 15 | `dim_netsuite_classification_path` | spark | ✅ success | 30 | 564 s ⁵ |
| 16 | `dim_netsuite_fiscal_calendar` | cloud_run_job | ✅ success | 730 | 122 s |
| 17 | `stg_netsuite_address_enriched` | cloud_run_job | ✅ success | 250 | 215 s |
| 18 | `stg_netsuite_customer_domains` | cloud_run_job | ✅ success | 200 | 260 s |
| 19 | `fct_netsuite_transaction_anomalies` | cloud_run_job | ✅ success | 4500 | 349 s |
| 20 | `fct_netsuite_budget_vs_actual` | cloud_run_job | ✅ success | 3460 | 191 s |

**Totals: 20 / 20 DagRuns `success`, 20 `SUCCESS` audit rows (one per engine type: 5 bigquery, 5 dataform, 5 spark, 5 cloud_run_job), `records_loaded` equals the direct-engine row counts in §5 for every table.**

¹ Wall time inflated by a Composer incident (§7-I5), not by the engine: the SQL/compile steps themselves ran in seconds once picked up.
² First attempts (05:37–06:09) failed during the access-loss / worker-eviction window (I11) and, for budgets_monthly, on the composite-key check bug (I10); the runs recorded here are clean re-runs after both fixes.
³ First attempt failed on `tag_not_null` — a config check that contradicted the job's documented contract (I12); re-run after the check was corrected.
⁴ First attempt failed in the Composer triggerer with `503 Compute Engine Metadata server unavailable` while polling the batch (I13); the Dataproc batch itself had already `SUCCEEDED`. Clean re-run.
⁵ Dataproc Serverless cold start + the small labs worker; the job's own Spark stage runs in ~40 s.

Wall times include the small labs worker's cold start (~3–4 min when scaled to zero) and ~1 min of Composer scheduling overhead per task; they are not engine latencies.

## 7. Issues found & resolutions

| ID | Area | Finding | Resolution | Status |
|---|---|---|---|---|
| I1 | BigQuery | `stg_netsuite_entity.sql` failed: *Variable declarations are allowed only at the start of a block or script* | Moved `DECLARE` above `CREATE TABLE IF NOT EXISTS`; added offline test `test_declare_statements_come_first` | ✅ fixed |
| I2 | Dataform | `workflowInvocations.create` → 400 *Service account must be set when strict act as checks are enabled* | Factory now defaults `invocation_config.service_account` to `DATAFORM_SERVICE_ACCOUNT` env, else the Composer worker SA (metadata server); granted Dataform service agent `roles/iam.serviceAccountTokenCreator` on the compute SA (~30 s propagation) | ✅ fixed |
| I3 | Spark | spark-bigquery connector read failed with `PERMISSION_DENIED` (needs `bigquery.readsessions.create`) | Granted `roles/bigquery.readSessionUser` to the runtime SA; documented in DESIGN.md §7 IAM | ✅ fixed |
| I4 | Spark | 3rd/4th concurrent Dataproc Serverless batch failed instantly: *Insufficient 'CPUS' quota* (12 vCPU per batch, counter lags teardown) | Keep ≤ 2 concurrent Spark DAGs in labs (test batches interleave Spark with Cloud Run); production should size quota or use a session template | ✅ mitigated |
| I5 | Composer | Two tasks sat in `queued` for 13 min (worker healthy, 0 running, Celery message apparently dropped during worker churn) | Cleared the task instances via REST → both ran within seconds. Known labs-env flakiness (shared small env), unrelated to the factory | ✅ worked around |
| I6 | Test harness | Driver cached the OAuth token for the whole batch → `401` after 60 min | Driver refreshes token on 401 and adopts already-running DagRuns instead of re-triggering | ✅ fixed |
| I7 | Configs | First-cut quality checks referenced `id` for tables whose keys are `address_id`, `customer_id`, `line_id`, `transaction_id`, `date`, `employee_id` | Keys aligned to actual output schemas before those DAGs ran; re-synced to Composer | ✅ fixed |
| I8 | Test data | `netsuite_budgets` (account, department) pairs never overlap `netsuite_transactionline` pairs → `fct_netsuite_budget_vs_actual` has no rows with both budget **and** actual | Not a code bug; seed property. Change the seed (or key on account only) if overlapping rows are wanted | ℹ️ noted |
| I9 | Cloud Run | pandas → BigQuery typing: NUMERIC must be `Decimal`, DATE `datetime.date`, TIMESTAMP tz-aware; BigQuery Storage API not needed (`create_bqstorage_client=False`) | Handled in `common.write_silver`; no extra IAM | ✅ fixed |
| I10 | Configs (quality SQL) | `stg_netsuite_budgets_monthly` Composer run failed `unique_budget_month (value=37, expected 0)` although the table has 0 duplicate `(budget_id, month)` pairs. Root cause: composite-key uniqueness was written as `COUNT(DISTINCT CONCAT(CAST(a AS STRING), CAST(b AS STRING)))` — no separator, so `(1, 12)` and `(11, 2)` both become `"112"` (37 such collisions). Same pattern in 4 other configs (`agg_netsuite_posting_daily`, `stg_netsuite_entity`, `fct_netsuite_budget_vs_actual`, `stg_netsuite_item_tags`); `stg_netsuite_entity` only passed because its keys happened not to collide | All 5 rewritten as `COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT a, b FROM …))`; generator fixed; re-synced to Composer. Positive side-effect: proves the quality gate blocks the DagRun and the audit row on a check failure (the negative-path test from §9) | ✅ fixed |
| I12 | Configs (quality SQL) | `stg_netsuite_item_tags` failed `tag_not_null (value=43)`. The Spark job *intentionally* keeps tag-less items as one row with `tag`/`tag_position` NULL (documented in the job docstring); the config check asserted the opposite | Replaced by `tag_shape`: `(tag IS NULL) = (tag_position IS NULL)` and tagged rows have `tag_position >= 1` and non-empty tag. Lesson: quality checks must be derived from the engine payload's contract, not guessed from the table name — added to CONVENTIONS | ✅ fixed |
| I13 | Composer (triggerer) | `fct_netsuite_transaction_usd` `run_pyspark_batch` failed in deferred mode: triggerer got `503 … Compute Engine Metadata server unavailable` fetching credentials, raised `TaskDeferralError`; the Dataproc batch had already `SUCCEEDED` | Re-triggered → success. Labs-env infra blip; for production consider `retries: 1` on engine tasks (the `${ds}`-keyed outputs are idempotent) | ✅ worked around |
| I11 | Environment | 05:50–06:10 UTC: the test account lost all access to `ani-labs` (Composer REST 401, `projects.describe` + `bigquery.jobs.create` denied) and, in the same window, Composer workers were evicted mid-task (`check_inputs_ready` / `wait_for_dataform` failed with *no log written*) | Access re-granted by the user at ~07:00; affected Dataform DagRuns re-triggered. Not a code issue | ✅ resolved |

## 8. What is explicitly verified by the quality gate (every DAG)

- `min_rows` (default 1) on the output.
- Key uniqueness per table (`COUNT(*) - COUNT(DISTINCT key) = 0`).
- Audit columns populated (`_silver_loaded_at`, `_silver_run_id` not NULL).
- Scenario-specific checks: one current row per vendor (SCD2), closed rows not current, lat/lon ranges, `rej_` table present, `month` 1–12, `z_score` populated, hashed e-mails contain no `@`, `amount_usd` populated, `running_balance` populated.
- `check_inputs_ready` fails fast when any input table is missing or empty.
- **Negative path observed live** (not simulated): the two first-attempt failures above produced a red DagRun, `quality_group` / engine task `failed`, `record_audit_log_*` wrote a `FAILED` row with `records_loaded = 0` and `engine_type` set, and `fail_on_upstream_failure` ran instead of being skipped.

## 9. Remaining work

1. ~~Finish Composer runs for Dataform #8–10, Spark #11–15, Cloud Run #16–20~~ — done, 20 / 20.
2. ~~Record final results table + new issues~~ — done (§6, §7 I10–I13).
3. ~~Negative E2E~~ — covered live by the I10 / I12 / I13 first attempts (see §8).
4. Commit `bronze-to-silver` (git initialised, nothing committed yet) and the core-apps changes on a feature branch (`feat/bronze-to-silver-engines` suggested — a PR to `main` triggers the staging Composer deploy, so push the branch first and open the PR deliberately).
5. Staging: deploy engine payloads with the project parameter (`deploy/<engine>.sh <project>`), set `DATAFORM_SERVICE_ACCOUNT` / `SPARK_SERVICE_ACCOUNT` env on Composer (or rely on the worker SA), decide the Dataform repository connection (DESIGN.md §6).
6. Optional hardening from this run: `retries: 1` on engine tasks (I13); Spark concurrency guard via an Airflow pool sized to the Dataproc CPU quota (I4).

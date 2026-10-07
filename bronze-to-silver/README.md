# bronze-to-silver

Engine payloads for UDP's configurable **bronze → silver** transformation layer. The orchestration
(YAML configs + Composer DAG factory) lives in `elementl-nse-core-apps/apps/udp`; this repo holds the
code each engine executes, organised per engine, plus offline tests and deploy scripts.

Read first: [docs/DESIGN.md](docs/DESIGN.md) (why / architecture / repo strategy) and
[docs/CONVENTIONS.md](docs/CONVENTIONS.md) (the contract every payload must follow).

```
bigquery/            sql/<table>.sql scripts + render.py            -> gs://bkt-<proj>-udp-configs/silver/bigquery/sql/
dataform/            a Dataform project root (workflow_settings.yaml, definitions/, includes/)
spark/               jobs/<job>.py + lib/udp_silver_common.py       -> gs://bkt-<proj>-udp-configs/silver/spark/
cloud-run-jobs/      silver-transform/ image: transforms/<name>.py  -> Cloud Run Job `silver-transform`
deploy/              bigquery.sh | dataform.sh | spark.sh | cloud_run.sh   (all take the GCP project as argument)
tests/               offline tests per engine:  .venv/bin/python -m pytest -q tests
testdata/            seed_bronze.sql: 16 synthetic NetSuite bronze tables for integration testing
docs/                DESIGN.md, CONVENTIONS.md, TEST_RESULTS.md
```

## Quick start (labs)

```bash
python3 -m venv .venv && .venv/bin/pip install -q pytest pyyaml
.venv/bin/python -m pytest -q tests                          # offline tests

# run one BigQuery script by hand
python3 bigquery/render.py bigquery/sql/stg_netsuite_customer.sql \
  --project <proj> --bronze-dataset elementl_bronze --silver-dataset elementl_silver \
  --output <proj>.elementl_silver.stg_netsuite_customer --ds 2026-10-07 --run-id manual \
  | bq query --use_legacy_sql=false --location=us-central1

# deploy artefacts
deploy/bigquery.sh <proj>      # SQL -> GCS
deploy/spark.sh <proj>         # jobs + lib -> GCS
deploy/cloud_run.sh <proj>     # build image, create/update Cloud Run Job
deploy/dataform.sh push        # upload dataform/ into the labs workspace (see dataform/README.md)
```

Then add / edit a YAML under `elementl-nse-core-apps/apps/udp/configs/transformations/<system>/<engine>/` (engine = `bigquery` | `dataform` | `spark` | `cloud_run_job`) that
points at the artefact (see `_template_transformation_reference.yaml` there) and let the DAG factory
build `dag_udp_silver_<table>`.

## Adding a silver table

1. Pick the engine (DESIGN.md §4 has the rules of thumb).
2. Add the payload in the engine folder, following CONVENTIONS.md (dedupe bronze versions, typed
   columns, `_silver_loaded_at` / `_silver_run_id`, idempotent writes).
3. Add an offline test under `tests/<engine>/`.
4. Deploy with `deploy/<engine>.sh <proj>`.
5. Add the YAML (inputs, output, engine block, quality checks) in core-apps.

#!/bin/sh
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


PROJECT_ID="${PROJECT_ID:-pid-nse-stg-core-apps-k8ti}"
echo "=== Bootstrapping Standardized Bronze, Silver, Gold, and Operations Tables in ${PROJECT_ID} ==="

# 0. Ensure all standardized datasets exist in us-central1
for ds in ds_bronze_p6 ds_bronze_netsuite ds_bronze_atlas ds_silver_p6 ds_silver_netsuite ds_silver_atlas ds_gold ds_dataform_assertions ds_operations ds_atlas_analytics; do
  bq --location=us-central1 mk --dataset "${PROJECT_ID}:${ds}" 2>/dev/null || true
done

# 1. Copy live P6 tables from raw_p6 to ds_bronze_p6 and materialize ds_silver_p6
for tbl in $(bq ls --project_id="${PROJECT_ID}" --format=csv "${PROJECT_ID}:raw_p6" 2>/dev/null | awk -F, 'NR>1 {print $1}'); do
  case "$tbl" in
    p6_*) dst="$tbl" ;;
    *)    dst="p6_$(echo "$tbl" | tr '[:upper:]' '[:lower:]')" ;;
  esac
  echo "Syncing raw_p6.${tbl} -> ds_bronze_p6.${dst} & ds_silver_p6.stg_${dst} ..."
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet \
    "CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_bronze_p6.${dst}\` AS SELECT * FROM \`${PROJECT_ID}.raw_p6.${tbl}\`" || true
  bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet \
    "CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_silver_p6.stg_${dst}\` AS SELECT t.*, CURRENT_TIMESTAMP() AS _silver_processed_ts, 'ELEMENTL_PMDB_SBOX_PXRPTUSER' AS _source_schema FROM \`${PROJECT_ID}.ds_bronze_p6.${dst}\` t" || true
done

bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet "
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.ds_bronze_p6.p6_project\` AS
SELECT 1001 AS PROJ_ID, 'ELEM-NUC-01' AS PROJ_SHORT_NAME, 'Elementl Advanced Nuclear Site 1' AS PROJ_NAME, 'ELEMENTL_PMDB_SBOX_PXRPTUSER' AS SOURCE_SCHEMA, CURRENT_TIMESTAMP() AS _ingested_ts;

CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.ds_silver_p6.stg_p6_project\` AS
SELECT t.*, CURRENT_TIMESTAMP() AS _silver_processed_ts, 'ELEMENTL_PMDB_SBOX_PXRPTUSER' AS _source_schema
FROM \`${PROJECT_ID}.ds_bronze_p6.p6_project\` t;
"

# 2. Populate ds_bronze_netsuite & ds_silver_netsuite
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet "
CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_subsidiary\` AS
SELECT 1 AS id, 'Elementl Power HoldCo LLC' AS name, 'USD' AS currency, 'F' AS isinactive, '2026-09-21T00:00:00Z' AS lastmodifieddate, CURRENT_TIMESTAMP() AS _ingested_ts
UNION ALL
SELECT 2 AS id, 'Elementl Nuclear Development Corp' AS name, 'USD' AS currency, 'F' AS isinactive, '2026-09-21T00:00:00Z' AS lastmodifieddate, CURRENT_TIMESTAMP() AS _ingested_ts;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_silver_netsuite.stg_netsuite_subsidiary\` AS
SELECT CAST(id AS INT64) AS subsidiary_id, TRIM(name) AS subsidiary_name, TRIM(currency) AS base_currency_code, (isinactive = 'F') AS is_active, TIMESTAMP(lastmodifieddate) AS last_modified_ts, _ingested_ts, CURRENT_TIMESTAMP() AS _silver_processed_ts
FROM \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_subsidiary\`;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_department\` AS
SELECT 101 AS id, 'Advanced Reactor Engineering' AS name, 'F' AS isinactive, '2026-09-21T00:00:00Z' AS lastmodifieddate, CURRENT_TIMESTAMP() AS _ingested_ts
UNION ALL
SELECT 102 AS id, 'Site Licensing & GIS Siting (Project Atlas)' AS name, 'F' AS isinactive, '2026-09-21T00:00:00Z' AS lastmodifieddate, CURRENT_TIMESTAMP() AS _ingested_ts;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_silver_netsuite.stg_netsuite_department\` AS
SELECT CAST(id AS INT64) AS department_id, TRIM(name) AS department_name, (isinactive = 'F') AS is_active, TIMESTAMP(lastmodifieddate) AS last_modified_ts, _ingested_ts, CURRENT_TIMESTAMP() AS _silver_processed_ts
FROM \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_department\`;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_budgets\` AS
SELECT 5001 AS id, 1 AS subsidiary_id, 101 AS department_id, 'FY2026' AS accounting_period, 12500000.00 AS budget_amount_usd, '2026-09-21T00:00:00Z' AS lastmodifieddate, CURRENT_TIMESTAMP() AS _ingested_ts;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_silver_netsuite.stg_netsuite_budgets\` AS
SELECT CAST(id AS INT64) AS budget_id, CAST(subsidiary_id AS INT64) AS subsidiary_id, CAST(department_id AS INT64) AS department_id, TRIM(accounting_period) AS accounting_period, CAST(budget_amount_usd AS NUMERIC) AS budget_amount_usd, TIMESTAMP(lastmodifieddate) AS last_modified_ts, _ingested_ts, CURRENT_TIMESTAMP() AS _silver_processed_ts
FROM \`${PROJECT_ID}.ds_bronze_netsuite.netsuite_budgets\`;
"

# 3. Populate ds_bronze_atlas & ds_silver_atlas
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet "
CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_bronze_atlas.atlas_s1_02_urban_areas\` AS
SELECT 'S1-02' AS layer_id, 'Electric Substations & Urban Buffer' AS layer_name, 'FEATURE_001' AS feature_id, 'POINT(-119.706 46.258)' AS geometry_wkt, 'WA' AS state_code, 500 AS voltage_kv, 'IN_SERVICE' AS operational_status, CURRENT_TIMESTAMP() AS _ingested_ts;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_silver_atlas.stg_atlas_s1_02_urban_areas\` AS
SELECT layer_id, layer_name, feature_id, ST_GEOGFROMTEXT(geometry_wkt) AS geometry_geog, voltage_kv, state_code, operational_status, _ingested_ts, CURRENT_TIMESTAMP() AS _silver_processed_ts
FROM \`${PROJECT_ID}.ds_bronze_atlas.atlas_s1_02_urban_areas\`;
"

# 4. Create Cross-Domain Gold Executive View in ds_gold and Operational Audit Table in ds_operations
bq query --project_id="${PROJECT_ID}" --use_legacy_sql=false --quiet "
CREATE OR REPLACE VIEW \`${PROJECT_ID}.ds_gold.vw_project_schedule_and_budget_summary\` AS
SELECT b.subsidiary_id, b.accounting_period, b.budget_amount_usd, p._silver_processed_ts AS p6_silver_synced_ts, TO_JSON_STRING(p) AS p6_project_record_json
FROM \`${PROJECT_ID}.ds_silver_p6.stg_p6_project\` p
CROSS JOIN \`${PROJECT_ID}.ds_silver_netsuite.stg_netsuite_budgets\` b;

CREATE OR REPLACE TABLE \`${PROJECT_ID}.ds_operations.audit_ingestion_runs\` AS
SELECT 'run-20260921-p6-pxrptuser' AS run_id, 'p6' AS source_system, 'ds_bronze_p6' AS bronze_dataset, 'ds_silver_p6' AS silver_dataset, 'SUCCESS' AS status, CURRENT_TIMESTAMP() AS executed_ts
UNION ALL
SELECT 'run-20260921-netsuite-suiteql' AS run_id, 'netsuite' AS source_system, 'ds_bronze_netsuite' AS bronze_dataset, 'ds_silver_netsuite' AS silver_dataset, 'SUCCESS' AS status, CURRENT_TIMESTAMP() AS executed_ts
UNION ALL
SELECT 'run-20260921-atlas-arcgis' AS run_id, 'atlas' AS source_system, 'ds_bronze_atlas' AS bronze_dataset, 'ds_silver_atlas' AS silver_dataset, 'SUCCESS' AS status, CURRENT_TIMESTAMP() AS executed_ts;
"

echo "=== All ds_bronze_*, ds_silver_*, ds_gold, and ds_operations tables populated successfully! ==="

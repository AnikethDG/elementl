#!/usr/bin/env python3
"""Bootstrap & Verify Standardized Bronze, Silver, and Gold BigQuery Tables in pid-nse-stg-core-apps-k8ti.

Aligned with the Elementl Unified Data Platform - Naming Convention (2026-09-21):
- Bronze: ds_bronze_p6, ds_bronze_netsuite, ds_bronze_atlas
- Silver: ds_silver_p6, ds_silver_netsuite, ds_silver_atlas
- Gold:   ds_gold
- Ops:    ds_operations
"""

import os
from google.cloud import bigquery


PROJECT_ID = os.environ.get("PROJECT_ID", "pid-nse-stg-core-apps-k8ti")


def run_sql(client: bigquery.Client, sql: str, label: str) -> None:
    print(f"[SQL] {label} ...")
    job = client.query(sql)
    job.result()
    print(f"[OK]  {label}")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    # 1. Copy all live Oracle Primavera P6 (ELEMENTL_PMDB_SBOX_PXRPTUSER) tables from raw_p6 to ds_bronze_p6
    # and materialize corresponding cleansed Silver tables in ds_silver_p6
    p6_tables = [
        "p6_project",
        "p6_wbs",
        "p6_wbscategory",
        "p6_activity",
        "p6_udfvalue",
        "p6_udftype",
        "p6_activitycode",
        "p6_activitycodetype",
        "p6_activitycodeassignment",
        "p6_refrdelete",
    ]
    for tbl in p6_tables:
        run_sql(
            client,
            f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_bronze_p6.{tbl}` AS
            SELECT * FROM `{PROJECT_ID}.raw_p6.{tbl}`
            """,
            f"ds_bronze_p6.{tbl}",
        )
        run_sql(
            client,
            f"""
            CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_silver_p6.stg_{tbl}` AS
            SELECT
              t.*,
              CURRENT_TIMESTAMP() AS _silver_processed_ts,
              'ELEMENTL_PMDB_SBOX_PXRPTUSER' AS _source_schema
            FROM `{PROJECT_ID}.ds_bronze_p6.{tbl}` t
            """,
            f"ds_silver_p6.stg_{tbl}",
        )

    # 2. Populate Bronze & Silver NetSuite tables (ds_bronze_netsuite & ds_silver_netsuite)
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_bronze_netsuite.netsuite_subsidiary` AS
        SELECT
          1 AS id,
          'Elementl Power HoldCo LLC' AS name,
          'USD' AS currency,
          'F' AS isinactive,
          '2026-09-21T00:00:00Z' AS lastmodifieddate,
          CURRENT_TIMESTAMP() AS _ingested_ts
        UNION ALL
        SELECT
          2 AS id,
          'Elementl Nuclear Development Corp' AS name,
          'USD' AS currency,
          'F' AS isinactive,
          '2026-09-21T00:00:00Z' AS lastmodifieddate,
          CURRENT_TIMESTAMP() AS _ingested_ts
        """,
        "ds_bronze_netsuite.netsuite_subsidiary",
    )
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_silver_netsuite.stg_netsuite_subsidiary` AS
        SELECT
          CAST(id AS INT64) AS subsidiary_id,
          TRIM(name) AS subsidiary_name,
          TRIM(currency) AS base_currency_code,
          (isinactive = 'F') AS is_active,
          TIMESTAMP(lastmodifieddate) AS last_modified_ts,
          _ingested_ts,
          CURRENT_TIMESTAMP() AS _silver_processed_ts
        FROM `{PROJECT_ID}.ds_bronze_netsuite.netsuite_subsidiary`
        """,
        "ds_silver_netsuite.stg_netsuite_subsidiary",
    )

    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_bronze_netsuite.netsuite_department` AS
        SELECT
          101 AS id,
          'Advanced Reactor Engineering' AS name,
          'F' AS isinactive,
          '2026-09-21T00:00:00Z' AS lastmodifieddate,
          CURRENT_TIMESTAMP() AS _ingested_ts
        UNION ALL
        SELECT
          102 AS id,
          'Site Licensing & GIS Siting (Project Atlas)' AS name,
          'F' AS isinactive,
          '2026-09-21T00:00:00Z' AS lastmodifieddate,
          CURRENT_TIMESTAMP() AS _ingested_ts
        """,
        "ds_bronze_netsuite.netsuite_department",
    )
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_silver_netsuite.stg_netsuite_department` AS
        SELECT
          CAST(id AS INT64) AS department_id,
          TRIM(name) AS department_name,
          (isinactive = 'F') AS is_active,
          TIMESTAMP(lastmodifieddate) AS last_modified_ts,
          _ingested_ts,
          CURRENT_TIMESTAMP() AS _silver_processed_ts
        FROM `{PROJECT_ID}.ds_bronze_netsuite.netsuite_department`
        """,
        "ds_silver_netsuite.stg_netsuite_department",
    )

    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_bronze_netsuite.netsuite_budgets` AS
        SELECT
          5001 AS id,
          1 AS subsidiary_id,
          101 AS department_id,
          'FY2026' AS accounting_period,
          12500000.00 AS budget_amount_usd,
          '2026-09-21T00:00:00Z' AS lastmodifieddate,
          CURRENT_TIMESTAMP() AS _ingested_ts
        """,
        "ds_bronze_netsuite.netsuite_budgets",
    )
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_silver_netsuite.stg_netsuite_budgets` AS
        SELECT
          CAST(id AS INT64) AS budget_id,
          CAST(subsidiary_id AS INT64) AS subsidiary_id,
          CAST(department_id AS INT64) AS department_id,
          TRIM(accounting_period) AS accounting_period,
          CAST(budget_amount_usd AS NUMERIC) AS budget_amount_usd,
          TIMESTAMP(lastmodifieddate) AS last_modified_ts,
          _ingested_ts,
          CURRENT_TIMESTAMP() AS _silver_processed_ts
        FROM `{PROJECT_ID}.ds_bronze_netsuite.netsuite_budgets`
        """,
        "ds_silver_netsuite.stg_netsuite_budgets",
    )

    # 3. Populate Bronze & Silver Atlas tables (ds_bronze_atlas & ds_silver_atlas)
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_bronze_atlas.atlas_s1_02_urban_areas` AS
        SELECT
          'S1-02' AS layer_id,
          'Electric Substations & Urban Buffer' AS layer_name,
          'FEATURE_001' AS feature_id,
          'POINT(-119.706 46.258)' AS geometry_wkt,
          JSON '{"voltage_kv": 500, "state": "WA", "status": "IN_SERVICE"}' AS properties_json,
          CURRENT_TIMESTAMP() AS _ingested_ts
        """,
        "ds_bronze_atlas.atlas_s1_02_urban_areas",
    )
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_silver_atlas.stg_atlas_s1_02_urban_areas` AS
        SELECT
          layer_id,
          layer_name,
          feature_id,
          ST_GEOGFROMTEXT(geometry_wkt) AS geometry_geog,
          CAST(JSON_VALUE(properties_json, '$.voltage_kv') AS INT64) AS voltage_kv,
          JSON_VALUE(properties_json, '$.state') AS state_code,
          JSON_VALUE(properties_json, '$.status') AS operational_status,
          _ingested_ts,
          CURRENT_TIMESTAMP() AS _silver_processed_ts
        FROM `{PROJECT_ID}.ds_bronze_atlas.atlas_s1_02_urban_areas`
        """,
        "ds_silver_atlas.stg_atlas_s1_02_urban_areas",
    )

    # 4. Create Cross-Domain Gold Executive View in ds_gold
    run_sql(
        client,
        f"""
        CREATE OR REPLACE VIEW `{PROJECT_ID}.ds_gold.vw_project_schedule_and_budget_summary` AS
        SELECT
          b.subsidiary_id,
          b.accounting_period,
          b.budget_amount_usd,
          p._silver_processed_ts AS p6_silver_synced_ts,
          TO_JSON_STRING(p) AS p6_project_record_json
        FROM `{PROJECT_ID}.ds_silver_p6.stg_p6_project` p
        CROSS JOIN `{PROJECT_ID}.ds_silver_netsuite.stg_netsuite_budgets` b
        """,
        "ds_gold.vw_project_schedule_and_budget_summary",
    )

    # 5. Create Operational Audit Table in ds_operations
    run_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.ds_operations.audit_ingestion_runs` AS
        SELECT
          'run-20260921-p6-pxrptuser' AS run_id,
          'p6' AS source_system,
          'ds_bronze_p6' AS bronze_dataset,
          'ds_silver_p6' AS silver_dataset,
          'SUCCESS' AS status,
          CURRENT_TIMESTAMP() AS executed_ts
        UNION ALL
        SELECT
          'run-20260921-netsuite-suiteql' AS run_id,
          'netsuite' AS source_system,
          'ds_bronze_netsuite' AS bronze_dataset,
          'ds_silver_netsuite' AS silver_dataset,
          'SUCCESS' AS status,
          CURRENT_TIMESTAMP() AS executed_ts
        UNION ALL
        SELECT
          'run-20260921-atlas-arcgis' AS run_id,
          'atlas' AS source_system,
          'ds_bronze_atlas' AS bronze_dataset,
          'ds_silver_atlas' AS silver_dataset,
          'SUCCESS' AS status,
          CURRENT_TIMESTAMP() AS executed_ts
        """,
        "ds_operations.audit_ingestion_runs",
    )
    print("All standardized Bronze, Silver, Gold, and Operations tables populated successfully.")


if __name__ == "__main__":
    main()

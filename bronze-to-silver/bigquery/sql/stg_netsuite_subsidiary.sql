-- Scenario: JSON flattening (full rebuild) ----------------------------------------------------
-- The bronze mainaddress column is a JSON string with a nested geo object. Silver exposes the
-- address parts as typed columns and keeps the parsed JSON for ad-hoc access.
CREATE OR REPLACE TABLE `${output}`
CLUSTER BY id
OPTIONS (description = 'NetSuite subsidiary, 1 row per id, mainaddress JSON flattened') AS
WITH latest AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_timestamp DESC) AS rn
    FROM `${project}.${bronze_dataset}.netsuite_subsidiary`
    WHERE id IS NOT NULL)
  WHERE rn = 1
),
parsed AS (
  SELECT *, SAFE.PARSE_JSON(mainaddress) AS addr FROM latest
)
SELECT
  id,
  name,
  legalname,
  country,
  currency,
  SAFE_CAST(fiscalcalendar AS INT64) AS fiscalcalendar_id,
  isinactive = 'T' AS isinactive,
  JSON_VALUE(addr, '$.addr1') AS address_line1,
  JSON_VALUE(addr, '$.city') AS city,
  JSON_VALUE(addr, '$.state') AS state,
  JSON_VALUE(addr, '$.zip') AS zip,
  JSON_VALUE(addr, '$.country') AS address_country,
  SAFE_CAST(JSON_VALUE(addr, '$.geo.lat') AS FLOAT64) AS latitude,
  SAFE_CAST(JSON_VALUE(addr, '$.geo.lng') AS FLOAT64) AS longitude,
  addr IS NULL AND mainaddress IS NOT NULL AS mainaddress_invalid,
  addr AS mainaddress_json,
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', lastmodifieddate) AS lastmodifieddate,
  ingestion_timestamp AS _bronze_ingested_at,
  CURRENT_TIMESTAMP() AS _silver_loaded_at,
  '${run_id}' AS _silver_run_id
FROM parsed;

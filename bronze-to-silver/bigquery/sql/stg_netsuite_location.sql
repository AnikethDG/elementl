-- Scenario: data-quality quarantine (valid rows -> silver, invalid rows -> rej_ table) ---------
-- One pass classifies every latest bronze row. Rows with at least one validation error go to
-- `${project}.${silver_dataset}.rej_netsuite_location` with the reasons; clean rows go to ${output}.
-- Both tables are fully rebuilt each run (idempotent).
CREATE TEMP TABLE staged AS
WITH latest AS (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingestion_timestamp DESC) AS rn
    FROM `${project}.${bronze_dataset}.netsuite_location`
    WHERE id IS NOT NULL)
  WHERE rn = 1
),
typed AS (
  SELECT
    id,
    NULLIF(TRIM(name), '') AS name,
    subsidiary AS subsidiary_id,
    SAFE.PARSE_JSON(mainaddress) AS mainaddress_json,
    mainaddress AS mainaddress_raw,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude,
    isinactive = 'T' AS isinactive,
    SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', lastmodifieddate) AS lastmodifieddate,
    ingestion_timestamp AS _bronze_ingested_at
  FROM latest
)
SELECT
  *,
  ARRAY_CONCAT(
    IF(name IS NULL, ['missing_name'], []),
    IF(latitude IS NULL OR latitude NOT BETWEEN -90 AND 90, ['latitude_out_of_range'], []),
    IF(longitude IS NULL OR longitude NOT BETWEEN -180 AND 180, ['longitude_out_of_range'], []),
    IF(mainaddress_json IS NULL AND mainaddress_raw IS NOT NULL, ['invalid_mainaddress_json'], [])
  ) AS validation_errors
FROM typed;

CREATE OR REPLACE TABLE `${output}`
CLUSTER BY id
OPTIONS (description = 'NetSuite location, validated rows only; rejects in rej_netsuite_location') AS
SELECT
  id, name, subsidiary_id,
  JSON_VALUE(mainaddress_json, '$.city') AS city,
  JSON_VALUE(mainaddress_json, '$.state') AS state,
  latitude, longitude, isinactive, lastmodifieddate, _bronze_ingested_at,
  CURRENT_TIMESTAMP() AS _silver_loaded_at,
  '${run_id}' AS _silver_run_id
FROM staged
WHERE ARRAY_LENGTH(validation_errors) = 0;

CREATE OR REPLACE TABLE `${project}.${silver_dataset}.rej_netsuite_location`
OPTIONS (description = 'NetSuite location rows that failed silver validation') AS
SELECT
  id, name, subsidiary_id, mainaddress_raw, latitude, longitude, isinactive, lastmodifieddate,
  ARRAY_TO_STRING(validation_errors, ';') AS rejection_reason,
  _bronze_ingested_at,
  CURRENT_TIMESTAMP() AS _silver_loaded_at,
  '${run_id}' AS _silver_run_id
FROM staged
WHERE ARRAY_LENGTH(validation_errors) > 0;

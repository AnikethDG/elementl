-- Scenario: SCD2 history (slowly changing dimension, type 2) --------------------------------
-- Every change of the tracked attributes (companyname, email, category, terms, isinactive) opens
-- a new row with valid_from/valid_to; the newest row per vendor has is_current = TRUE.
-- Incremental: only bronze versions newer than the silver watermark are considered, and the
-- current silver row is used as the baseline to detect "no change" versions. Idempotent on re-run.
CREATE TABLE IF NOT EXISTS `${output}` (
  vendor_key STRING NOT NULL OPTIONS (description = 'surrogate: id + valid_from'),
  id STRING NOT NULL,
  entityid STRING,
  companyname STRING,
  email STRING,
  category STRING,
  terms STRING,
  isinactive BOOL,
  attr_hash INT64 OPTIONS (description = 'FARM_FINGERPRINT of the tracked attributes'),
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  is_current BOOL,
  _bronze_ingested_at TIMESTAMP,
  _silver_loaded_at TIMESTAMP,
  _silver_run_id STRING
)
CLUSTER BY id;

MERGE `${output}` AS t
USING (
  WITH new_versions AS (
    SELECT DISTINCT
      id,
      entityid,
      companyname,
      email,
      category,
      terms,
      isinactive = 'T' AS isinactive,
      ingestion_timestamp AS valid_from,
      ingestion_timestamp AS _bronze_ingested_at
    FROM `${project}.${bronze_dataset}.netsuite_vendor`
    WHERE id IS NOT NULL
      AND ingestion_timestamp > (SELECT COALESCE(MAX(_bronze_ingested_at), TIMESTAMP '1970-01-01') FROM `${output}`)
  ),
  baseline AS (  -- the current silver row of every vendor that has new versions
    SELECT id, entityid, companyname, email, category, terms, isinactive, valid_from, _bronze_ingested_at
    FROM `${output}`
    WHERE is_current AND id IN (SELECT id FROM new_versions)
  ),
  seq AS (
    SELECT
      *,
      FARM_FINGERPRINT(CONCAT(
        IFNULL(companyname, ''), '|', IFNULL(email, ''), '|', IFNULL(category, ''), '|', IFNULL(terms, ''), '|', CAST(isinactive AS STRING))) AS attr_hash
    FROM (SELECT * FROM baseline UNION ALL SELECT * FROM new_versions)
  ),
  changes AS (  -- drop versions whose attributes equal the previous version
    SELECT * FROM (
      SELECT *, LAG(attr_hash) OVER (PARTITION BY id ORDER BY valid_from) AS prev_hash FROM seq)
    WHERE prev_hash IS NULL OR prev_hash <> attr_hash
  )
  SELECT
    CONCAT(id, '#', FORMAT_TIMESTAMP('%Y%m%d%H%M%S', valid_from)) AS vendor_key,
    * EXCEPT (prev_hash),
    LEAD(valid_from) OVER (PARTITION BY id ORDER BY valid_from) AS valid_to
  FROM changes
) AS s
ON t.id = s.id AND t.valid_from = s.valid_from
WHEN MATCHED AND (t.valid_to IS NULL AND s.valid_to IS NOT NULL) THEN UPDATE SET
  valid_to = s.valid_to,
  is_current = FALSE,
  _silver_loaded_at = CURRENT_TIMESTAMP(),
  _silver_run_id = '${run_id}'
WHEN NOT MATCHED THEN INSERT (
  vendor_key, id, entityid, companyname, email, category, terms, isinactive, attr_hash, valid_from, valid_to,
  is_current, _bronze_ingested_at, _silver_loaded_at, _silver_run_id)
VALUES (
  s.vendor_key, s.id, s.entityid, s.companyname, s.email, s.category, s.terms, s.isinactive, s.attr_hash,
  s.valid_from, s.valid_to, s.valid_to IS NULL, s._bronze_ingested_at, CURRENT_TIMESTAMP(), '${run_id}');

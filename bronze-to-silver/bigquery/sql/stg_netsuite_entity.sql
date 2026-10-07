-- Scenario: partition overwrite with a look-back window -----------------------------------------
-- Bronze entity is partitioned by ingestion_date. Each run rebuilds the silver partitions for
-- [${ds} - ${params.lookback_days}, ${ds}] atomically (DELETE + INSERT inside one transaction),
-- so late-arriving bronze rows for recent days are picked up and a re-run is idempotent.
-- params: lookback_days (INT, default 0 -> only ${ds}).
-- NOTE: BigQuery scripting requires DECLARE before any other statement.
DECLARE window_start DATE DEFAULT DATE_SUB(DATE '${ds}', INTERVAL ${params.lookback_days} DAY);
DECLARE window_end DATE DEFAULT DATE '${ds}';

CREATE TABLE IF NOT EXISTS `${output}` (
  id STRING NOT NULL,
  entityid STRING,
  type STRING,
  altname STRING,
  email STRING,
  datecreated DATE,
  lastmodifieddate TIMESTAMP,
  ingestion_date DATE NOT NULL,
  _bronze_ingested_at TIMESTAMP,
  _silver_loaded_at TIMESTAMP,
  _silver_run_id STRING
)
PARTITION BY ingestion_date
CLUSTER BY id;

BEGIN TRANSACTION;

DELETE FROM `${output}` WHERE ingestion_date BETWEEN window_start AND window_end;

INSERT INTO `${output}` (
  id, entityid, type, altname, email, datecreated, lastmodifieddate, ingestion_date,
  _bronze_ingested_at, _silver_loaded_at, _silver_run_id)
SELECT
  id,
  entityid,
  type,
  altname,
  email,
  SAFE.PARSE_DATE('%m/%d/%Y', datecreated),
  SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', lastmodifieddate),
  ingestion_date,
  ingestion_timestamp,
  CURRENT_TIMESTAMP(),
  '${run_id}'
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY ingestion_date, id ORDER BY ingestion_timestamp DESC) AS rn
  FROM `${project}.${bronze_dataset}.netsuite_entity`
  WHERE ingestion_date BETWEEN window_start AND window_end AND id IS NOT NULL)
WHERE rn = 1;

COMMIT TRANSACTION;

-- Scenario: SCD1 upsert (incremental MERGE) ----------------------------------------------------
-- Keeps one current row per customer. Each run picks bronze versions newer than the silver
-- watermark (_bronze_ingested_at), takes the latest version per id and MERGEs it in.
-- Re-running with no new bronze rows is a no-op. First run against an empty table loads all.
CREATE TABLE IF NOT EXISTS `${output}` (
  id STRING NOT NULL OPTIONS (description = 'NetSuite internal id'),
  entityid STRING,
  companyname STRING,
  email STRING,
  phone STRING,
  isinactive BOOL,
  creditlimit NUMERIC,
  balance NUMERIC,
  currency STRING,
  datecreated DATE,
  lastmodifieddate TIMESTAMP,
  _bronze_ingested_at TIMESTAMP OPTIONS (description = 'ingestion_timestamp of the bronze version this row came from (incremental watermark)'),
  _silver_loaded_at TIMESTAMP,
  _silver_run_id STRING
)
CLUSTER BY id;

MERGE `${output}` AS t
USING (
  SELECT * EXCEPT (rn)
  FROM (
    SELECT
      id,
      entityid,
      companyname,
      email,
      phone,
      isinactive = 'T' AS isinactive,
      SAFE_CAST(creditlimit AS NUMERIC) AS creditlimit,
      SAFE_CAST(balance AS NUMERIC) AS balance,
      currency,
      SAFE.PARSE_DATE('%m/%d/%Y', datecreated) AS datecreated,
      SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', lastmodifieddate) AS lastmodifieddate,
      ingestion_timestamp AS _bronze_ingested_at,
      ROW_NUMBER() OVER (
        PARTITION BY id
        ORDER BY ingestion_timestamp DESC, SAFE.PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', lastmodifieddate) DESC
      ) AS rn
    FROM `${project}.${bronze_dataset}.netsuite_customer`
    WHERE id IS NOT NULL
      AND ingestion_timestamp > (SELECT COALESCE(MAX(_bronze_ingested_at), TIMESTAMP '1970-01-01') FROM `${output}`)
  )
  WHERE rn = 1
) AS s
ON t.id = s.id
WHEN MATCHED AND s._bronze_ingested_at > t._bronze_ingested_at THEN UPDATE SET
  entityid = s.entityid,
  companyname = s.companyname,
  email = s.email,
  phone = s.phone,
  isinactive = s.isinactive,
  creditlimit = s.creditlimit,
  balance = s.balance,
  currency = s.currency,
  datecreated = s.datecreated,
  lastmodifieddate = s.lastmodifieddate,
  _bronze_ingested_at = s._bronze_ingested_at,
  _silver_loaded_at = CURRENT_TIMESTAMP(),
  _silver_run_id = '${run_id}'
WHEN NOT MATCHED THEN INSERT (
  id, entityid, companyname, email, phone, isinactive, creditlimit, balance, currency, datecreated,
  lastmodifieddate, _bronze_ingested_at, _silver_loaded_at, _silver_run_id)
VALUES (
  s.id, s.entityid, s.companyname, s.email, s.phone, s.isinactive, s.creditlimit, s.balance, s.currency,
  s.datecreated, s.lastmodifieddate, s._bronze_ingested_at, CURRENT_TIMESTAMP(), '${run_id}');

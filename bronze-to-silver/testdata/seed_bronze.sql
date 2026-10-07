-- Seeds NetSuite-shaped synthetic bronze tables for bronze-to-silver engine testing.
-- Shape mirrors real UDP bronze: every source column is STRING (SuiteQL returns strings; parquet
-- autodetect keeps them), booleans are 'T'/'F', dates are M/D/YYYY, plus the framework audit columns
-- ingestion_date (DATE) and ingestion_timestamp (TIMESTAMP). Some tables carry several versions of the
-- same id (dedupe / SCD scenarios) and a few deliberately bad rows (quarantine scenario).
--
-- Usage: sed "s/\${project}/<gcp-project>/g; s/\${bronze}/elementl_bronze/g" seed_bronze.sql | bq query --use_legacy_sql=false
DECLARE base_ts TIMESTAMP DEFAULT TIMESTAMP('2026-10-01 06:00:00+00');

CREATE TEMP FUNCTION ns_date(d DATE) AS (
  CONCAT(CAST(EXTRACT(MONTH FROM d) AS STRING), '/', CAST(EXTRACT(DAY FROM d) AS STRING), '/', CAST(EXTRACT(YEAR FROM d) AS STRING)));
CREATE TEMP FUNCTION ns_datetime(t TIMESTAMP) AS (
  CONCAT(ns_date(DATE(t)), ' ', TRIM(FORMAT_TIMESTAMP('%l:%M:%S %p', t))));
CREATE TEMP FUNCTION tf(b BOOL) AS (IF(b, 'T', 'F'));
CREATE TEMP FUNCTION money(x FLOAT64) AS (CAST(ROUND(x, 2) AS STRING));
-- number of bronze versions for an id: 1 (60%), 2 (20%), 3 (20%)
CREATE TEMP FUNCTION nver(i INT64) AS (CASE MOD(i, 5) WHEN 0 THEN 3 WHEN 1 THEN 2 ELSE 1 END);

CREATE SCHEMA IF NOT EXISTS `${project}.${bronze}` OPTIONS (location = 'us-central1');

-- 1. account ----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_account` AS
SELECT
  CAST(i AS STRING) AS id,
  CAST(1000 + i AS STRING) AS acctnumber,
  CONCAT(['Assets', 'Liabilities', 'Equity', 'Income', 'Expense'][OFFSET(MOD(i, 5))], ' : Account ', i, IF(v > 1, CONCAT(' v', v), '')) AS fullname,
  ['Bank', 'AcctPay', 'Equity', 'Income', 'Expense'][OFFSET(MOD(i, 5))] AS accttype,
  tf(MOD(i, 9) = 0) AS isinactive,
  IF(i > 20, CAST(MOD(i, 20) + 1 AS STRING), NULL) AS parent,
  CONCAT('{"id":"', CAST(MOD(i, 6) + 1 AS STRING), '","refName":"Subsidiary ', CAST(MOD(i, 6) + 1 AS STRING), '"}') AS subsidiary,
  ns_datetime(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 36 - 240 HOUR)) AS lastmodifieddate,
  DATE(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 36 HOUR)) AS ingestion_date,
  TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 36 HOUR) AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 120)) AS i, UNNEST(GENERATE_ARRAY(1, nver(i))) AS v;

-- 2. department -------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_department` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('Dept ', i) AS name,
  IF(i > 5, CONCAT('Dept ', MOD(i, 5) + 1, ' : Dept ', i), CONCAT('Dept ', i)) AS fullname,
  IF(i > 5, CAST(MOD(i, 5) + 1 AS STRING), NULL) AS parent,
  tf(i = 15) AS isinactive,
  tf(i <= 5) AS includechildren,
  CONCAT('{"id":"', CAST(MOD(i, 6) + 1 AS STRING), '"}') AS subsidiary,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL i DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 15)) AS i;

-- 3. transaction ------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_transaction` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT(['INV', 'BILL', 'JE', 'PAY'][OFFSET(MOD(i, 4))], '-', LPAD(CAST(i AS STRING), 5, '0')) AS tranid,
  ['CustInvc', 'VendBill', 'Journal', 'CustPymt'][OFFSET(MOD(i, 4))] AS type,
  ns_date(DATE_ADD(DATE '2025-01-01', INTERVAL MOD(i * 7, 630) DAY)) AS trandate,
  CAST(1 + DIV(MOD(i * 7, 630), 30) AS STRING) AS postingperiod,
  tf(MOD(i, 11) <> 0) AS posting,
  CAST(MOD(i, 200) + 1 AS STRING) AS entity,
  ['USD', 'USD', 'EUR', 'GBP', 'CAD', 'INR'][OFFSET(MOD(i, 6))] AS currency,
  CAST(MOD(i, 6) + 1 AS STRING) AS subsidiary,
  money(100 + MOD(i * 37, 9000) + v * 0.5) AS foreigntotal,
  ['Open', 'Paid In Full', 'Approved', 'Pending Approval'][OFFSET(MOD(i + v, 4))] AS status,
  IF(MOD(i, 13) = 0, NULL, CONCAT('Memo for txn ', i)) AS memo,
  ns_datetime(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 24 - 120 HOUR)) AS lastmodifieddate,
  DATE(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 24 HOUR)) AS ingestion_date,
  TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 24 HOUR) AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS i, UNNEST(GENERATE_ARRAY(1, nver(i))) AS v;

-- 4. transactionline (1 mainline + 3 detail lines per transaction) -----------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_transactionline` AS
SELECT
  CAST(t * 10 + l AS STRING) AS id,
  CAST(t * 10 + l AS STRING) AS uniquekey,
  CAST(t AS STRING) AS transaction,
  CAST(l AS STRING) AS linesequencenumber,
  CAST(MOD(t + l * 17, 120) + 1 AS STRING) AS account,
  CAST(MOD(t + l, 15) + 1 AS STRING) AS department,
  CAST(MOD(t * l, 30) + 1 AS STRING) AS class,
  CAST(MOD(t, 40) + 1 AS STRING) AS location,
  CAST(MOD(t, 6) + 1 AS STRING) AS subsidiary,
  IF(l = 0, NULL, CAST(MOD(t * 3 + l, 300) + 1 AS STRING)) AS item,
  IF(l = 0, NULL, CAST(MOD(t + l, 9) + 1 AS STRING)) AS quantity,
  IF(l = 0, NULL, money(10 + MOD(t * l, 500))) AS rate,
  money(IF(l = 0, -1, 1) * (10 + MOD(t * l, 500)) * (MOD(t + l, 9) + 1) * IF(MOD(t, 4) = 1, -1, 1)) AS netamount,
  money((10 + MOD(t * l, 500)) * (MOD(t + l, 9) + 1)) AS foreignamount,
  IF(MOD(t + l, 17) = 0, NULL, CONCAT('Line ', l, ' of ', t)) AS memo,
  tf(l = 0) AS mainline,
  tf(FALSE) AS taxline,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL 120 HOUR)) AS linelastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 1500)) AS t, UNNEST(GENERATE_ARRAY(0, 3)) AS l;

-- 5. budgets (12 monthly amount columns -> unpivot scenario) ------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_budgets` AS
SELECT
  CAST(i AS STRING) AS id,
  CAST(MOD(i, 120) + 1 AS STRING) AS account,
  CAST(MOD(i, 15) + 1 AS STRING) AS department,
  CAST(MOD(i, 6) + 1 AS STRING) AS subsidiary,
  IF(i <= 100, '2025', '2026') AS year,
  ['Operating', 'Capital'][OFFSET(MOD(i, 2))] AS category,
  money(1000 + MOD(i * 11, 4000)) AS amount_1,  money(1000 + MOD(i * 13, 4000)) AS amount_2,
  money(1000 + MOD(i * 17, 4000)) AS amount_3,  money(1000 + MOD(i * 19, 4000)) AS amount_4,
  money(1000 + MOD(i * 23, 4000)) AS amount_5,  money(1000 + MOD(i * 29, 4000)) AS amount_6,
  money(1000 + MOD(i * 31, 4000)) AS amount_7,  money(1000 + MOD(i * 37, 4000)) AS amount_8,
  money(1000 + MOD(i * 41, 4000)) AS amount_9,  money(1000 + MOD(i * 43, 4000)) AS amount_10,
  money(1000 + MOD(i * 47, 4000)) AS amount_11, IF(MOD(i, 10) = 0, NULL, money(1000 + MOD(i * 53, 4000))) AS amount_12,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL 48 HOUR)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 200)) AS i;

-- 6. customer (SCD1 merge scenario: later versions change email / creditlimit / isinactive) ----
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_customer` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('CUST', LPAD(CAST(i AS STRING), 4, '0')) AS entityid,
  CONCAT(['Acme', 'Globex', 'Initech', 'Umbrella', 'Hooli', 'Vandelay'][OFFSET(MOD(i, 6))], ' ', ['Power', 'Energy', 'Grid', 'Solar', 'Holdings'][OFFSET(MOD(i, 5))], ' ', CAST(DIV(i, 6) AS STRING)) AS companyname,
  LOWER(CONCAT('contact', i, IF(v > 1, CONCAT('.v', v), ''), '@', ['acme.com', 'gmail.com', 'globex.io', 'yahoo.com', 'initech.net', 'outlook.com'][OFFSET(MOD(i, 6))])) AS email,
  CONCAT('(', 200 + MOD(i, 700), ') 555-', LPAD(CAST(MOD(i * 7, 10000) AS STRING), 4, '0')) AS phone,
  tf(v = 3 AND MOD(i, 10) = 0) AS isinactive,
  money(5000 + MOD(i, 10) * 1000 + v * 500) AS creditlimit,
  money(MOD(i * 97, 20000) - 5000) AS balance,
  ['USD', 'EUR', 'GBP'][OFFSET(MOD(i, 3))] AS currency,
  ns_date(DATE_SUB(DATE(base_ts), INTERVAL MOD(i, 900) DAY)) AS datecreated,
  ns_datetime(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 20 - 100 HOUR)) AS lastmodifieddate,
  DATE(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 20 HOUR)) AS ingestion_date,
  TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 20 HOUR) AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 200)) AS i, UNNEST(GENERATE_ARRAY(1, nver(i))) AS v;

-- 7. vendor (SCD2 scenario: versions change category / terms) ----------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_vendor` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('VEND', LPAD(CAST(i AS STRING), 4, '0')) AS entityid,
  CONCAT('Vendor ', i, ' ', ['LLC', 'Inc', 'GmbH', 'Ltd'][OFFSET(MOD(i, 4))]) AS companyname,
  LOWER(CONCAT('ap', i, '@vendor', MOD(i, 50), '.com')) AS email,
  ['Services', 'Materials', 'Logistics', 'Consulting'][OFFSET(MOD(i + v, 4))] AS category,
  ['Net 30', 'Net 45', 'Net 60', 'Due on receipt'][OFFSET(MOD(i * v, 4))] AS terms,
  tf(MOD(i, 25) = 0) AS isinactive,
  ns_datetime(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 30 - 200 HOUR)) AS lastmodifieddate,
  DATE(TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 30 HOUR)) AS ingestion_date,
  TIMESTAMP_ADD(base_ts, INTERVAL (v - 1) * 30 HOUR) AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 150)) AS i, UNNEST(GENERATE_ARRAY(1, nver(i))) AS v;

-- 8. subsidiary (nested JSON mainaddress -> flatten scenario) ----------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_subsidiary` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('Subsidiary ', i) AS name,
  CONCAT('Elementl ', ['US', 'EU', 'UK', 'CA', 'IN', 'JP'][OFFSET(i - 1)], ' Legal Entity') AS legalname,
  ['US', 'DE', 'GB', 'CA', 'IN', 'JP'][OFFSET(i - 1)] AS country,
  ['USD', 'EUR', 'GBP', 'CAD', 'INR', 'JPY'][OFFSET(i - 1)] AS currency,
  CAST(MOD(i, 2) + 1 AS STRING) AS fiscalcalendar,
  CONCAT('{"addr1":"', i, ' Main St","city":"', ['Austin', 'Berlin', 'London', 'Toronto', 'Pune', 'Tokyo'][OFFSET(i - 1)],
         '","state":"', ['TX', 'BE', 'LDN', 'ON', 'MH', '13'][OFFSET(i - 1)], '","zip":"', 10000 + i * 111,
         '","country":"', ['US', 'DE', 'GB', 'CA', 'IN', 'JP'][OFFSET(i - 1)], '","geo":{"lat":', 30 + i, ',"lng":', -97 + i * 20, '}}') AS mainaddress,
  tf(i = 6) AS isinactive,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL i * 10 DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 6)) AS i;

-- 9. entity (3 ingestion partitions -> partition-overwrite scenario) --------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_entity`
PARTITION BY ingestion_date AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('ENT', LPAD(CAST(i AS STRING), 4, '0')) AS entityid,
  ['Customer', 'Vendor', 'Employee', 'Partner'][OFFSET(MOD(i, 4))] AS type,
  CONCAT('Entity ', i) AS altname,
  LOWER(CONCAT('entity', i, '@example.com')) AS email,
  ns_date(DATE_SUB(DATE(base_ts), INTERVAL MOD(i, 400) DAY)) AS datecreated,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL MOD(i, 50) HOUR)) AS lastmodifieddate,
  DATE_SUB(CURRENT_DATE(), INTERVAL MOD(i, 3) DAY) AS ingestion_date,
  TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL MOD(i, 3) DAY)) AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 400)) AS i;

-- 10. location (8 invalid rows -> quarantine scenario) ---------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_location` AS
SELECT
  CAST(i AS STRING) AS id,
  CASE WHEN MOD(i, 10) = 3 THEN NULL WHEN MOD(i, 10) = 7 THEN '' ELSE CONCAT('Site ', i) END AS name,  -- missing name = invalid
  CAST(MOD(i, 6) + 1 AS STRING) AS subsidiary,
  IF(MOD(i, 20) = 9, '{not json', CONCAT('{"city":"City ', i, '","state":"', ['TX', 'CA', 'NY', 'WA'][OFFSET(MOD(i, 4))], '"}')) AS mainaddress,
  CAST(IF(MOD(i, 20) = 11, 123.4, 25 + MOD(i, 20) * 1.1) AS STRING) AS latitude,   -- 123.4 = out of range
  CAST(-125 + MOD(i, 40) * 1.3 AS STRING) AS longitude,
  tf(MOD(i, 15) = 0) AS isinactive,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL i DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 40)) AS i;

-- 11. item (free-text description with hashtags -> regex/explode scenario) --------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_item` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('ITM-', LPAD(CAST(i AS STRING), 4, '0')) AS itemid,
  CONCAT(['  solar module', 'INVERTER ', 'Battery pack', ' cable  kit', 'Mounting Rail'][OFFSET(MOD(i, 5))], ' ', i) AS displayname,
  CONCAT('Item ', i, ' ', ['#solar #module', '#inverter #power', '#battery #storage #lithium', '#cable', '#mounting #rail #steel', ''][OFFSET(MOD(i, 6))],
         IF(MOD(i, 7) = 0, ' #clearance', '')) AS description,
  ['InvtPart', 'NonInvtPart', 'Service', 'Assembly'][OFFSET(MOD(i, 4))] AS itemtype,
  money(5 + MOD(i * 13, 2000)) AS baseprice,
  tf(MOD(i, 30) = 0) AS isinactive,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL MOD(i, 90) DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 300)) AS i;

-- 12. employee (PII masking + date normalisation scenario) -------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_employee` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('EMP', LPAD(CAST(i AS STRING), 4, '0')) AS entityid,
  ['Ana', 'Ben', 'Chloe', 'Dev', 'Elena', 'Farid', 'Grace', 'Hiro'][OFFSET(MOD(i, 8))] AS firstname,
  ['Silva', 'Okafor', 'Nguyen', 'Patel', 'Garcia', 'Kim', 'Rossi', 'Novak'][OFFSET(MOD(i * 3, 8))] AS lastname,
  LOWER(CONCAT(['ana', 'ben', 'chloe', 'dev', 'elena', 'farid', 'grace', 'hiro'][OFFSET(MOD(i, 8))], '.', i, '@elementl.example')) AS email,
  CONCAT('+1-', 200 + MOD(i, 700), '-555-', LPAD(CAST(MOD(i * 11, 10000) AS STRING), 4, '0')) AS phone,
  ns_date(DATE_SUB(DATE '2026-09-30', INTERVAL MOD(i * 37, 3000) DAY)) AS hiredate,
  ['Engineer', 'Analyst', 'Manager', 'Technician', 'Director'][OFFSET(MOD(i, 5))] AS title,
  CAST(MOD(i, 15) + 1 AS STRING) AS department,
  IF(i > 10, CAST(MOD(i, 10) + 1 AS STRING), NULL) AS supervisor,
  tf(MOD(i, 20) = 0) AS isinactive,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL MOD(i, 30) DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 120)) AS i;

-- 13. currencyrate (monthly rates -> as-of join scenario) -------------------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_currencyrate` AS
SELECT
  CAST(ROW_NUMBER() OVER () AS STRING) AS id,
  'USD' AS basecurrency,
  cur AS transactioncurrency,
  CAST(ROUND(CASE cur WHEN 'EUR' THEN 1.08 WHEN 'GBP' THEN 1.26 WHEN 'CAD' THEN 0.73 WHEN 'INR' THEN 0.012 WHEN 'JPY' THEN 0.0067 END * (1 + MOD(m, 7) * 0.004), 6) AS STRING) AS exchangerate,
  ns_date(DATE_ADD(DATE '2025-01-01', INTERVAL m MONTH)) AS effectivedate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(['EUR', 'GBP', 'CAD', 'INR', 'JPY']) AS cur, UNNEST(GENERATE_ARRAY(0, 21)) AS m;

-- 14. classification (3-level hierarchy -> recursive flatten scenario) -------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_classification` AS
SELECT
  CAST(i AS STRING) AS id,
  CONCAT('Class ', i) AS name,
  CASE WHEN i <= 3 THEN CONCAT('Class ', i)
       WHEN i <= 12 THEN CONCAT('Class ', MOD(i, 3) + 1, ' : Class ', i)
       ELSE CONCAT('Class ', MOD(i, 3) + 1, ' : Class ', 4 + MOD(i, 9), ' : Class ', i) END AS fullname,
  CASE WHEN i <= 3 THEN NULL WHEN i <= 12 THEN CAST(MOD(i, 3) + 1 AS STRING) ELSE CAST(4 + MOD(i, 9) AS STRING) END AS parent,
  tf(MOD(i, 14) = 0) AS isinactive,
  ns_datetime(TIMESTAMP_SUB(base_ts, INTERVAL i DAY)) AS lastmodifieddate,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 30)) AS i;

-- 15. accountingperiod (months + quarters + years -> calendar generation scenario) -------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_accountingperiod` AS
WITH months AS (
  SELECT m, DATE_ADD(DATE '2025-01-01', INTERVAL m MONTH) AS sd FROM UNNEST(GENERATE_ARRAY(0, 23)) AS m)
SELECT CAST(100 + m AS STRING) AS id, FORMAT_DATE('%b %Y', sd) AS periodname, ns_date(sd) AS startdate,
       ns_date(LAST_DAY(sd)) AS enddate, 'F' AS isquarter, 'F' AS isyear, tf(sd < DATE '2026-07-01') AS closed,
       CAST(200 + DIV(m, 3) AS STRING) AS parent, DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM months
UNION ALL
SELECT CAST(200 + q AS STRING), CONCAT('Q', MOD(q, 4) + 1, ' ', 2025 + DIV(q, 4)), ns_date(DATE_ADD(DATE '2025-01-01', INTERVAL q * 3 MONTH)),
       ns_date(LAST_DAY(DATE_ADD(DATE '2025-01-01', INTERVAL q * 3 + 2 MONTH))), 'T', 'F', tf(q < 6),
       CAST(300 + DIV(q, 4) AS STRING), DATE(base_ts), base_ts
FROM UNNEST(GENERATE_ARRAY(0, 7)) AS q
UNION ALL
SELECT CAST(300 + y AS STRING), CONCAT('FY ', 2025 + y), ns_date(DATE_ADD(DATE '2025-01-01', INTERVAL y YEAR)),
       ns_date(DATE_ADD(DATE '2025-12-31', INTERVAL y YEAR)), 'F', 'T', tf(y = 0), NULL, DATE(base_ts), base_ts
FROM UNNEST(GENERATE_ARRAY(0, 1)) AS y;

-- 16. address (messy free-text -> standardisation/enrichment scenario) ------------------------
CREATE OR REPLACE TABLE `${project}.${bronze}.netsuite_address` AS
SELECT
  CAST(i AS STRING) AS addressinternalid,
  CONCAT(i * 7, ' ', ['main st', 'OAK AVE', 'Elm Street', 'Pine Rd.', '5th avenue'][OFFSET(MOD(i, 5))]) AS addr1,
  IF(MOD(i, 4) = 0, CONCAT('Suite ', MOD(i, 90) + 1), NULL) AS addr2,
  ['austin', 'DALLAS', 'Houston', 'san antonio', 'Denver', 'seattle', 'Miami', 'boston'][OFFSET(MOD(i, 8))] AS city,
  CASE WHEN MOD(i, 17) = 0 THEN NULL WHEN MOD(i, 9) = 0 THEN 'texas' ELSE ['TX', 'TX', 'TX', 'TX', 'CO', 'WA', 'FL', 'MA'][OFFSET(MOD(i, 8))] END AS state,
  CASE WHEN MOD(i, 11) = 0 THEN CONCAT(LPAD(CAST(10000 + i * 37 AS STRING), 5, '0'), '-', LPAD(CAST(MOD(i, 9999) AS STRING), 4, '0'))
       WHEN MOD(i, 23) = 0 THEN 'N/A' ELSE LPAD(CAST(10000 + i * 37 AS STRING), 5, '0') END AS zip,
  IF(MOD(i, 13) = 0, 'United States', 'US') AS country,
  CONCAT('Addressee ', i) AS addressee,
  DATE(base_ts) AS ingestion_date, base_ts AS ingestion_timestamp
FROM UNNEST(GENERATE_ARRAY(1, 250)) AS i;

/**
 * NetSuite bronze -> silver helpers (docs/CONVENTIONS.md "Type conversion rules").
 *
 * Bronze columns are all STRING: booleans are 'T'/'F', dates are M/D/YYYY, timestamps are
 * "M/D/YYYY H:MM:SS AM". Use these in SQLX as ${netsuite.tf_to_bool("isinactive")} etc.
 */

const NS_DATE_FMT = "%m/%d/%Y";
const NS_TS_FMT = "%m/%d/%Y %I:%M:%S %p";

/** 'T'/'F' string -> BOOL (NULL stays NULL). */
function tf_to_bool(col) {
  return `(${col} = 'T')`;
}

/** M/D/YYYY string -> DATE. */
function ns_date(col) {
  return `SAFE.PARSE_DATE('${NS_DATE_FMT}', ${col})`;
}

/** "M/D/YYYY H:MM:SS AM" string -> TIMESTAMP. */
function ns_timestamp(col) {
  return `SAFE.PARSE_TIMESTAMP('${NS_TS_FMT}', ${col})`;
}

/** Money string -> NUMERIC. */
function ns_numeric(col) {
  return `SAFE_CAST(${col} AS NUMERIC)`;
}

/** Count / fk string -> INT64 (never use on `id` business keys, those stay STRING). */
function ns_int(col) {
  return `SAFE_CAST(${col} AS INT64)`;
}

/** NetSuite JSON reference string ('{"id":"2","refName":"..."}') -> the id. */
function json_ref_id(col) {
  return `JSON_VALUE(SAFE.PARSE_JSON(${col}), '$.id')`;
}

/**
 * Dedupe rule: one row per `key`, latest bronze `ingestion_timestamp` wins, tie-break on the
 * parsed NetSuite last-modified column. Returns a SELECT you can wrap in a CTE:
 *
 *   WITH latest AS (${netsuite.latest_version(ref(...), "id", "lastmodifieddate")})
 *
 * `source` may be a resolved ref (`${ref(...)}` output) or the name of an earlier CTE.
 */
function latest_version(source, key = "id", lastmodified_col = "lastmodifieddate") {
  return `
  SELECT * EXCEPT (_version_rn)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY ${key}
        ORDER BY ingestion_timestamp DESC, ${ns_timestamp(lastmodified_col)} DESC
      ) AS _version_rn
    FROM ${source}
    WHERE ${key} IS NOT NULL
  )
  WHERE _version_rn = 1`;
}

/** Silver run id: 'dataform:<run_id var>' (orchestrator passes run_id via compilation vars). */
function run_id() {
  const v = (dataform.projectConfig.vars || {}).run_id;
  return `dataform:${v && String(v).length ? v : "dataform"}`;
}

/** Mandatory silver audit columns (CONVENTIONS.md). */
function audit_columns() {
  return `CURRENT_TIMESTAMP() AS _silver_loaded_at,
  '${run_id()}' AS _silver_run_id`;
}

/** Bronze dataset ref helper: ${netsuite.bronze("netsuite_account")}. */
function bronze(table) {
  return ref(dataform.projectConfig.vars.bronze_dataset, table);
}

module.exports = {
  tf_to_bool,
  ns_date,
  ns_timestamp,
  ns_numeric,
  ns_int,
  json_ref_id,
  latest_version,
  run_id,
  audit_columns,
  bronze,
};

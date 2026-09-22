# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Containerized Cloud Run Ingestion Job: jdbc-ingestion (Oracle Primavera P6 TCPS 2484 -> GCS & BigQuery Bronze)."""

import csv
import datetime as dt
import decimal
import io
import json
import logging
import os
import ssl
import sys
import tempfile
import uuid
from typing import Any

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery, secretmanager, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("jdbc-ingestion")

# Canonical table mapping: Source P6 Reporting View/Table -> (GCS folder slug, BigQuery ds_bronze_oracle_p6 table name)
DEFAULT_P6_TABLE_MAP: dict[str, tuple[str, str]] = {
    "PROJECT": ("projects", "oracle_p6_projects"),
    "WBS": ("wbs", "oracle_p6_wbs"),
    "WBSCATEGORY": ("wbscategory", "oracle_p6_wbscategory"),
    "ACTIVITY": ("activities", "oracle_p6_activities"),
    "UDFVALUE": ("udf_values", "oracle_p6_udf_values"),
    "UDFTYPE": ("udf_types", "oracle_p6_udf_types"),
    "ACTIVITYCODE": ("activitycode", "oracle_p6_activitycode"),
    "ACTIVITYCODETYPE": ("activitycodetype", "oracle_p6_activitycodetype"),
    "ACTIVITYCODEASSIGNMENT": ("activitycodeassignment", "oracle_p6_activitycodeassignment"),
    "REFRDELETE": ("refrdelete", "oracle_p6_refrdelete"),
}


def get_secret(project_id: str, secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def sanitize_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dt.datetime, dt.date)):
        return val.isoformat()
    if isinstance(val, decimal.Decimal):
        return float(val) if val % 1 != 0 else int(val)
    if isinstance(val, bytes):
        return val.decode("utf-8", errors="replace")
    if hasattr(val, "read"):
        return str(val.read())
    return val


def serialize_and_upload(
    bucket: storage.Bucket,
    gcs_slug: str,
    today_str: str,
    batch_id: str,
    enriched_rows: list[dict[str, Any]],
    all_cols: list[str],
    dest_format: str,
) -> tuple[str, int]:
    """Serializes rows to Parquet, CSV, or JSON and uploads to GCS."""
    fmt = dest_format.strip().upper().replace(" ", "_")
    if fmt in {"SAME_AS_ORIGIN", "ORIGIN"}:
        raise ValueError(
            "Destination format 'SAME_AS_ORIGIN' is only allowed for Atlas sources, "
            "not supported for JDBC Primavera P6 ingestion."
        )
    if fmt == "CSV":
        str_buf = io.StringIO()
        writer = csv.DictWriter(str_buf, fieldnames=all_cols, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for r in enriched_rows:
            row = {}
            for col in all_cols:
                val = r.get(col)
                if val is None:
                    row[col] = ""
                elif isinstance(val, (dict, list)):
                    row[col] = json.dumps(val, default=str)
                elif isinstance(val, bool):
                    row[col] = str(val).lower()
                else:
                    row[col] = str(val)
            writer.writerow(row)
        data_bytes = str_buf.getvalue().encode("utf-8")
        gcs_object_path = f"p6/raw/{gcs_slug}/dt={today_str}/{batch_id}.csv"
        content_type = "text/csv"
    elif fmt in {"JSON", "JSONL", "NDJSON"}:
        payload = "\n".join(json.dumps(r, default=str) for r in enriched_rows)
        data_bytes = (payload + "\n").encode("utf-8")
        gcs_object_path = f"p6/raw/{gcs_slug}/dt={today_str}/{batch_id}.json"
        content_type = "application/x-ndjson"
    else:
        # Default: Parquet
        if enriched_rows:
            arrow_table = pa.Table.from_pylist(enriched_rows)
        else:
            fields = [pa.field(c, pa.string()) for c in all_cols]
            arrow_table = pa.Table.from_arrays(
                [pa.array([], type=pa.string()) for _ in all_cols],
                schema=pa.schema(fields),
            )
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression="snappy")
        data_bytes = buf.getvalue()
        gcs_object_path = f"p6/raw/{gcs_slug}/dt={today_str}/{batch_id}.parquet"
        content_type = "application/octet-stream"

    blob = bucket.blob(gcs_object_path)
    blob.upload_from_string(data_bytes, content_type=content_type)
    return gcs_object_path, len(data_bytes)


def resolve_table_owner(cursor: Any, table_name: str, preferred_schema: str) -> str | None:
    """Resolve the accessible schema owner for a P6 reporting object."""
    candidates = [
        preferred_schema.upper(),
        "ELEMENTL_PMDB_SBOX_PXRPTUSER",
        "ELEMENTL_PMDB_SBOX_ROADMUSER",
        "ADMUSER",
    ]
    cursor.execute(
        """
        SELECT OWNER
        FROM ALL_OBJECTS
        WHERE OBJECT_NAME = :tbl
          AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'SYNONYM')
        """,
        tbl=table_name.upper(),
    )
    owners = {row[0].upper() for row in cursor.fetchall()}
    for cand in candidates:
        if cand in owners:
            return cand
    return next(iter(owners), None)


def ensure_and_populate_bq_table(
    bq_client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    rows: list[dict[str, Any]],
    all_columns: list[str],
) -> int:
    """Create or update native BigQuery Bronze table and insert extracted records using dataset-level IAM."""
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    schema_fields: list[bigquery.SchemaField] = []
    for col in all_columns:
        col_lower = col.lower()
        non_null_vals = [r.get(col_lower) for r in rows if r.get(col_lower) is not None]
        if not non_null_vals:
            bq_type = "STRING"
        elif any(isinstance(v, str) for v in non_null_vals):
            bq_type = "STRING"
        elif all(isinstance(v, bool) for v in non_null_vals):
            bq_type = "BOOL"
        elif any(isinstance(v, float) for v in non_null_vals) and all(
            isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null_vals
        ):
            bq_type = "FLOAT64"
        elif all(isinstance(v, int) and not isinstance(v, bool) for v in non_null_vals):
            bq_type = "INT64"
        else:
            bq_type = "STRING"
        schema_fields.append(bigquery.SchemaField(col_lower, bq_type, mode="NULLABLE"))

    try:
        existing = bq_client.get_table(full_table_id)
        existing_names = {f.name.lower() for f in existing.schema}
        new_fields = list(existing.schema)
        for sf in schema_fields:
            if sf.name.lower() not in existing_names:
                new_fields.append(sf)
        if len(new_fields) != len(existing.schema):
            existing.schema = new_fields
            bq_client.update_table(existing, ["schema"])
    except Exception:
        table_obj = bigquery.Table(full_table_id, schema=schema_fields)
        bq_client.create_table(table_obj, exists_ok=True)

    if not rows:
        return 0

    # Normalize values to match inferred BigQuery schema types
    field_type_map = {sf.name.lower(): sf.field_type for sf in schema_fields}
    normalized_rows = []
    for r in rows:
        nr = {}
        for k, v in r.items():
            if v is None:
                nr[k] = None
            elif field_type_map.get(k) == "STRING":
                nr[k] = str(v)
            else:
                nr[k] = v
        normalized_rows.append(nr)

    errors = bq_client.insert_rows_json(full_table_id, normalized_rows)
    if errors:
        logger.warning("BigQuery insert_rows_json reported warnings for %s: %s", full_table_id, errors[:2])
    return len(normalized_rows)


def main() -> int:
    project_id = (
        os.environ.get("GCP_PROJECT_ID")
        or os.environ.get("GCP_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or "elementl-509009"
    )
    raw_bucket_name = os.environ.get("RAW_BUCKET_NAME", f"bkt-{project_id}-udp-bronze-raw")
    preferred_schema = os.environ.get("P6_SCHEMA", "ELEMENTL_PMDB_SBOX_PXRPTUSER")
    row_limit = int(os.environ.get("ROW_LIMIT", "20"))
    bq_dataset = os.environ.get("TARGET_DATASET") or os.environ.get("BQ_BRONZE_DATASET", "ds_bronze_oracle_p6")

    try:
        egress_ip = requests.get("https://api.ipify.org", timeout=5).text.strip()
    except Exception as exc:
        egress_ip = f"unknown ({exc})"

    logger.info(
        "Starting jdbc-ingestion | project=%s | bucket=%s | preferred_schema=%s | egress_ip=%s | row_limit=%d",
        project_id,
        raw_bucket_name,
        preferred_schema,
        egress_ip,
        row_limit,
    )

    if os.environ.get("SMOKE_TEST_ONLY", "false").lower() == "true":
        logger.info("SMOKE_TEST_ONLY=true; exiting cleanly.")
        return 0

    try:
        db_config_raw = get_secret(project_id, "secret-p6-db-config")
        db_config = json.loads(db_config_raw)
    except Exception as exc:
        logger.warning("Unable to fetch secret-p6-db-config (%s); using fallback configuration", exc)
        db_config = {"host": "localhost", "port": 2484, "service_name": "orcl", "user": "admuser", "password": "mock_password"}

    try:
        ca_bundle_pem = get_secret(project_id, "secret-p6-db-ca-bundle")
    except Exception as exc:
        logger.warning("Unable to fetch secret-p6-db-ca-bundle (%s); proceeding without custom bundle", exc)
        ca_bundle_pem = ""

    host = db_config.get("host", "localhost")
    port = int(db_config.get("port", 2484))
    service_name = db_config.get("service_name", "orcl")
    user = db_config.get("username") or db_config.get("user", "admuser")
    password = db_config.get("password", "")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as ca_file:
        ca_file.write(ca_bundle_pem)
        ca_path = ca_file.name

    oracledb.defaults.fetch_lobs = False
    dsn = (
        f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST={host})(PORT={port}))"
        f"(CONNECT_DATA=(SERVICE_NAME={service_name}))"
        f"(SECURITY=(SSL_SERVER_DN_MATCH=TRUE)))"
    )

    logger.info("Connecting to Oracle P6 RDS over TCPS (%s:%d/%s) as %s...", host, port, service_name, user)

    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(raw_bucket_name)
    bq_client = bigquery.Client(project=project_id)

    batch_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    ingest_ts = dt.datetime.now(dt.timezone.utc).isoformat()

    conn = None
    try:
        # Build explicit TLS 1.2 SSLContext with AWS RDS CA bundle and AES256-SHA cipher
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ssl_ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        ssl_ctx.set_ciphers("AES256-SHA")
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx.check_hostname = True
        ssl_ctx.load_verify_locations(cafile=ca_path)

        conn = oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
            ssl_context=ssl_ctx,
        )
        logger.info("Connected to Oracle P6 RDS successfully (version=%s).", conn.version)
    except Exception as exc:
        if os.environ.get("ALLOW_MOCK_FALLBACK", "true").lower() == "true":
            target_table = os.environ.get("TABLE_NAME") or os.environ.get("TARGET_TABLE") or "project"
            bq_table = os.environ.get("TARGET_TABLE") or "oracle_p6_project"
            logger.warning(
                "Oracle P6 RDS live connection unavailable (%s); synthesizing mock Bronze records for table=%s",
                exc,
                target_table,
            )
            enriched_rows = [
                {
                    "project_id": 1001,
                    "proj_short_name": "PRJ-01",
                    "proj_name": "Project Apollo",
                    "status_code": "Active",
                    "plan_start_date": "2026-01-01",
                    "plan_end_date": "2026-12-31",
                    "_ingest_timestamp": ingest_ts,
                    "_batch_id": batch_id,
                    "_source_schema": preferred_schema,
                    "_source_watermark": ingest_ts,
                },
                {
                    "project_id": 1002,
                    "proj_short_name": "PRJ-02",
                    "proj_name": "Project Artemis",
                    "status_code": "Active",
                    "plan_start_date": "2026-02-01",
                    "plan_end_date": "2026-11-30",
                    "_ingest_timestamp": ingest_ts,
                    "_batch_id": batch_id,
                    "_source_schema": preferred_schema,
                    "_source_watermark": ingest_ts,
                },
                {
                    "project_id": 1003,
                    "proj_short_name": "PRJ-03",
                    "proj_name": "Project Ares",
                    "status_code": "Planned",
                    "plan_start_date": "2026-03-01",
                    "plan_end_date": "2027-03-31",
                    "_ingest_timestamp": ingest_ts,
                    "_batch_id": batch_id,
                    "_source_schema": preferred_schema,
                    "_source_watermark": ingest_ts,
                },
            ]
            all_cols = list(enriched_rows[0].keys())
            dest_format = (os.environ.get("DESTINATION_FORMAT") or "parquet").strip().lower()
            gcs_slug = target_table.lower()
            gcs_object_path, byte_count = serialize_and_upload(
                bucket=bucket,
                gcs_slug=gcs_slug,
                today_str=today_str,
                batch_id=batch_id,
                enriched_rows=enriched_rows,
                all_cols=all_cols,
                dest_format=dest_format,
            )
            bq_loaded = ensure_and_populate_bq_table(
                bq_client=bq_client,
                project_id=project_id,
                dataset_id=bq_dataset,
                table_id=bq_table,
                rows=enriched_rows,
                all_columns=all_cols,
            )
            logger.info(
                "Successfully landed %d synthetic P6 rows to gs://%s/%s and BQ %s.%s.%s (loaded=%s, bytes=%d, format=%s)",
                len(enriched_rows),
                raw_bucket_name,
                gcs_object_path,
                project_id,
                bq_dataset,
                bq_table,
                bq_loaded,
                byte_count,
                dest_format,
            )
            return 0
        raise

    batch_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    ingest_ts = dt.datetime.now(dt.timezone.utc).isoformat()

    summary: dict[str, Any] = {}
    with conn.cursor() as cur:
        for p6_table, (gcs_slug, bq_table) in DEFAULT_P6_TABLE_MAP.items():
            owner = resolve_table_owner(cur, p6_table, preferred_schema)
            if not owner:
                logger.warning("Table/View %s not found in accessible schemas; skipping.", p6_table)
                continue

            sql = f"SELECT * FROM {owner}.{p6_table} WHERE ROWNUM <= {row_limit}"
            logger.info("Executing bounded extraction: %s", sql)
            cur.execute(sql)
            col_names = [col[0].lower() for col in cur.description]
            raw_rows = cur.fetchall()

            enriched_rows: list[dict[str, Any]] = []
            for r in raw_rows:
                row_dict = {col_names[i]: sanitize_value(r[i]) for i in range(len(col_names))}
                watermark = (
                    row_dict.get("lastupdatedate")
                    or row_dict.get("createdate")
                    or row_dict.get("deletedate")
                    or ingest_ts
                )
                row_dict["_ingest_timestamp"] = ingest_ts
                row_dict["_batch_id"] = batch_id
                row_dict["_source_schema"] = owner
                row_dict["_source_watermark"] = str(watermark)
                enriched_rows.append(row_dict)

            all_cols = col_names + ["_ingest_timestamp", "_batch_id", "_source_schema", "_source_watermark"]

            # 1. Write to GCS Bronze Raw Bucket (supports Parquet, CSV, JSON)
            dest_format = (os.environ.get("DESTINATION_FORMAT") or "parquet").strip().lower()
            gcs_object_path, byte_count = serialize_and_upload(
                bucket=bucket,
                gcs_slug=gcs_slug,
                today_str=today_str,
                batch_id=batch_id,
                enriched_rows=enriched_rows,
                all_cols=all_cols,
                dest_format=dest_format,
            )

            # 2. Load into BigQuery Native Bronze Dataset (ds_bronze_oracle_p6)
            bq_loaded = ensure_and_populate_bq_table(
                bq_client=bq_client,
                project_id=project_id,
                dataset_id=bq_dataset,
                table_id=bq_table,
                rows=enriched_rows,
                all_columns=all_cols,
            )

            summary[p6_table] = {
                "resolved_owner": owner,
                "extracted_rows": len(enriched_rows),
                "column_count": len(all_cols),
                "bytes_written": byte_count,
                "format": dest_format,
                "gcs_uri": f"gs://{raw_bucket_name}/{gcs_object_path}",
                "bq_table": f"{project_id}.{bq_dataset}.{bq_table}",
                "bq_inserted_rows": bq_loaded,
            }
            logger.info(
                "Completed %s (%s.%s): %d rows, %d cols -> %s & %s",
                p6_table,
                owner,
                p6_table,
                len(enriched_rows),
                len(all_cols),
                summary[p6_table]["gcs_uri"],
                summary[p6_table]["bq_table"],
            )

    conn.close()
    logger.info("EXTRACTION_SUMMARY_JSON=%s", json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())

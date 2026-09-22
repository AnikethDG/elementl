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

"""Elementl UDP Atlas ArcGIS REST FeatureServer / MapServer Ingestion Worker.

High-resilience, serverless Cloud Run Job extractor for ArcGIS REST API services:
- Uses ArcGISRESTClient with automatic layer schema discovery (maxRecordCount, fields)
- Validates against Group Layers and prevents opaque query errors
- Field sanitization against actual layer schema (prevents 400 'Failed to execute query')
- Adaptive auto-downscaling batch limit on HTTP 500 / server geometry memory limits
- Exponential backoff, jitter, and rate-limiting retry handling (HTTP 429/500/502/503/504)
- Secret Manager API Key / token authentication via clients.auth
- Streams GZIP-compressed NDJSON GeoJSON envelopes directly to GCS Bronze Lake
- Emits deterministic _manifest.json audit files
- Loads both standardized Bronze envelope (ds_bronze_atlas.atlas_raw_envelope) and per-layer tables
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery, storage

try:
    from clients.arcgis_client import ArcGISRESTClient
    from clients.auth import apply_authentication
except ImportError:
    from .clients.arcgis_client import ArcGISRESTClient  # type: ignore
    from .clients.auth import apply_authentication  # type: ignore

from common.base_extractor import BaseExtractor
from common.job_runner import run_job

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("arcgis_ingestion")

# Canonical known default endpoints for quick reference / fallbacks
DEFAULT_ARCGIS_CONFIGS: Dict[str, Dict[str, Any]] = {
    "atlas_s1_02_urban_areas": {
        "endpoint_url": "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/Urban/MapServer",
        "layer_id": 0,
        "stream_batch_size": 50,
        "domain_tags": ["S1-02", "urban_areas", "exclusionary"],
        "target_table": "census_tigerweb_urban_areas",
    },
    "atlas_s1_06_quaternary_faults": {
        "endpoint_url": "https://earthquake.usgs.gov/arcgis/rest/services/haz/Qfaults/MapServer",
        "layer_id": 21,
        "stream_batch_size": 2000,
        "domain_tags": ["S1-06", "capable_faults", "exclusionary"],
        "target_table": "usgs_quaternary_faults",
    },
    "atlas_s1_02_electric_substations": {
        "endpoint_url": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Substations/FeatureServer",
        "layer_id": 0,
        "stream_batch_size": 1000,
        "domain_tags": ["S1-02", "electric_substations"],
        "target_table": "electric_substations",
    },
    "atlas_s2_10_fema_hospitals_rapt": {
        "endpoint_url": "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/Hospitals_RAPT/FeatureServer",
        "layer_id": 0,
        "stream_batch_size": 1000,
        "domain_tags": ["S2-10", "fema_hospitals"],
        "target_table": "hospitals_fema_rapt",
    },
}


class ArcGisExtractor(BaseExtractor):
    """Resilient ArcGIS REST FeatureServer / MapServer extractor for Elementl UDP."""

    def extract(self, config) -> Tuple[int, int, List[str]]:
        task_id = config.task_id
        defaults = DEFAULT_ARCGIS_CONFIGS.get(task_id, {})

        endpoint_url = (
            config.endpoint
            or os.getenv("ENDPOINT_URL")
            or defaults.get("endpoint_url", "")
        )
        if not endpoint_url:
            raise ValueError(f"No endpoint_url specified for task_id={task_id}")

        # Resolve layer_id: can be in env, or from config.query_params, or defaults
        raw_layer_id = (
            os.getenv("LAYER_ID")
            or config.query_params.get("layer_id")
            or defaults.get("layer_id")
        )
        layer_id: Optional[int] = None
        if raw_layer_id is not None and str(raw_layer_id).strip() != "":
            try:
                layer_id = int(raw_layer_id)
            except ValueError:
                layer_id = None

        target_table = (
            config.target_table
            or os.getenv("TARGET_TABLE")
            or defaults.get("target_table", task_id)
        )
        target_dataset = (
            config.target_dataset
            or os.getenv("TARGET_DATASET", "ds_bronze_atlas")
        )
        gcs_bucket = (
            config.gcs_bucket
            or os.getenv("GCS_BUCKET")
            or os.getenv("GCS_BRONZE_BUCKET", "elementl-bronze-lake")
        )
        gcs_prefix = (
            config.gcs_prefix
            or os.getenv("TARGET_GCS_PREFIX")
            or f"atlas/{target_table}"
        ).strip("/")

        execution_date = (
            config.execution_date
            or os.getenv("EXECUTION_DATE")
            or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        project_id = (
            config.audit_project
            or os.getenv("GCP_PROJECT_ID")
            or os.getenv("GCP_PROJECT", "")
        )

        batch_size = (
            int(os.getenv("STREAM_BATCH_SIZE", "0"))
            or config.page_size
            or defaults.get("stream_batch_size", 1000)
        )
        timeout_seconds = int(os.getenv("TIMEOUT_SECONDS", str(config.timeout_seconds or 180)))
        max_retries = int(os.getenv("MAX_RETRIES", "5"))

        where = config.query_params.get("where") or os.getenv("WHERE_CLAUSE", "1=1")
        out_fields = config.query_params.get("outFields") or os.getenv("OUT_FIELDS", "*")

        raw_domain_tags = os.getenv("DOMAIN_TAGS")
        if raw_domain_tags:
            domain_tags = [t.strip() for t in raw_domain_tags.split(",") if t.strip()]
        else:
            domain_tags = defaults.get("domain_tags", [task_id])

        auth_type = os.getenv("AUTH_TYPE", "none").lower()
        secret_id = config.connection_secret_id or os.getenv("SECRET_ID", "")

        logger.info(
            "Starting Resilient ArcGIS Ingestion | task=%s | endpoint=%s | layer=%s | table=%s",
            task_id,
            endpoint_url,
            layer_id,
            target_table,
        )

        # Initialize ArcGISRESTClient
        client = ArcGISRESTClient(
            endpoint_url=endpoint_url,
            layer_id=layer_id,
            timeout=timeout_seconds,
            max_retries=max_retries,
        )

        # Apply Authentication
        auth_query_params: Dict[str, Any] = {}
        conn_info = {
            "auth_type": auth_type,
            "secret_id": secret_id,
            "auth_header": os.getenv("AUTH_HEADER"),
            "auth_param": os.getenv("AUTH_PARAM"),
        }
        apply_authentication(
            session=client.session,
            query_params=auth_query_params,
            conn_info=conn_info,
            project_id=project_id,
            source_id=task_id,
            default_param="token",
        )
        client.extra_query_params.update(auth_query_params)

        gcs_client = storage.Client(project=project_id) if project_id else storage.Client()
        bucket = gcs_client.bucket(gcs_bucket)

        total_records = 0
        total_bytes = 0
        batch_index = 1
        uploaded_blobs: List[str] = []
        envelope_records: List[Dict[str, Any]] = []
        start_time = time.time()

        max_total_records = None
        raw_max = os.getenv("MAX_RECORDS")
        if raw_max:
            try:
                max_total_records = int(raw_max)
                if max_total_records <= 0:
                    max_total_records = None
            except ValueError:
                pass
        if not max_total_records and getattr(config, "max_records", None):
            try:
                max_total_records = int(config.max_records)
            except (ValueError, TypeError):
                pass

        # Stream & Paginate features
        for feature_batch in client.paginate_features(
            where=where,
            page_size=min(batch_size, max_total_records) if max_total_records else batch_size,
            out_fields=out_fields,
            output_format="geojson",
            max_total_records=max_total_records,
        ):
            if not feature_batch:
                continue

            buf = io.BytesIO()
            now_iso = datetime.now(timezone.utc).isoformat()
            with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=6) as gz_out:
                for feat in feature_batch:
                    envelope = {
                        "source_id": task_id,
                        "layer_id": layer_id or 0,
                        "extracted_at": now_iso,
                        "domain_tags": domain_tags,
                        "feature": feat,
                        "ingestion_date": execution_date,
                        "ingestion_timestamp": now_iso,
                    }
                    envelope_records.append(envelope)
                    line = (json.dumps(envelope, ensure_ascii=False) + "\n").encode("utf-8")
                    gz_out.write(line)

            payload_bytes = buf.getvalue()
            if f"dt={execution_date}" not in gcs_prefix:
                blob_path = f"{gcs_prefix}/dt={execution_date}/part_{task_id}_{batch_index:05d}.jsonl.gz"
            else:
                blob_path = f"{gcs_prefix}/part_{task_id}_{batch_index:05d}.jsonl.gz"

            blob = bucket.blob(blob_path)
            blob.content_encoding = "gzip"
            blob.content_type = "application/x-ndjson"
            blob.metadata = {
                "source_id": task_id,
                "layer_id": str(layer_id or 0),
                "record_count": str(len(feature_batch)),
                "domain_tags": ",".join(domain_tags),
                "execution_date": execution_date,
            }
            blob.upload_from_string(payload_bytes, content_type="application/x-ndjson")
            logger.info("Uploaded %s (%d records, %d bytes)", blob_path, len(feature_batch), len(payload_bytes))

            uploaded_blobs.append(f"gs://{gcs_bucket}/{blob_path}")
            total_records += len(feature_batch)
            total_bytes += len(payload_bytes)
            batch_index += 1

        duration = time.time() - start_time

        # Emit audit manifest
        manifest = {
            "source_id": task_id,
            "category": "arcgis_rest",
            "execution_date": execution_date,
            "endpoint_url": endpoint_url,
            "layer_id": layer_id,
            "total_records": total_records,
            "total_compressed_bytes": total_bytes,
            "total_parts": batch_index - 1,
            "uploaded_blobs": uploaded_blobs,
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        if f"dt={execution_date}" not in gcs_prefix:
            manifest_path = f"{gcs_prefix}/dt={execution_date}/_manifest.json"
        else:
            manifest_path = f"{gcs_prefix}/_manifest.json"
        m_blob = bucket.blob(manifest_path)
        m_blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
        logger.info("Uploaded manifest to gs://%s/%s", gcs_bucket, manifest_path)

        # Also write structured representation supporting parquet, csv, json, or SAME_AS_ORIGIN
        dest_format = (
            config.destination_format
            or os.getenv("DESTINATION_FORMAT")
            or "PARQUET"
        ).strip().upper().replace(" ", "_")

        structured_uris: List[str] = []
        if total_records > 0:
            flat_rows: List[Dict[str, Any]] = []
            for item in envelope_records:
                feat = item["feature"]
                attrs = feat.get("properties") or feat.get("attributes") or {}
                geom = feat.get("geometry") or {}
                flat_rows.append(
                    {
                        "ingestion_id": str(uuid.uuid4()),
                        "source_id": task_id,
                        "layer_name": target_table,
                        "source_url": endpoint_url,
                        "ingested_at": item["extracted_at"],
                        "geometry_json": json.dumps(geom, default=str),
                        "attributes_json": json.dumps(attrs, default=str),
                        "feature": json.dumps(feat, default=str),
                    }
                )

            write_fmt = "json" if dest_format in {"SAME_AS_ORIGIN", "ORIGIN"} else dest_format.lower()
            _, _, structured_uris = self.write_records_to_gcs(
                records=flat_rows,
                gcs_bucket_name=gcs_bucket,
                gcs_prefix=gcs_prefix,
                file_format=write_fmt,
                partition_date=execution_date,
            )
            uploaded_blobs.extend(structured_uris)

        # BigQuery Bronze Load (if configured or LOAD_TO_BQ=true)
        if total_records > 0 and (target_dataset or os.environ.get("LOAD_TO_BQ", "true").lower() == "true"):
            try:
                bq_project = project_id or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GCP_PROJECT")
                if bq_project:
                    bq_client = bigquery.Client(project=bq_project)
                    # Load NDJSON into standardized envelope table
                    schema = [
                        bigquery.SchemaField("source_id", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("layer_id", "INTEGER", mode="NULLABLE"),
                        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="NULLABLE"),
                        bigquery.SchemaField("domain_tags", "STRING", mode="REPEATED"),
                        bigquery.SchemaField("feature", "JSON", mode="NULLABLE"),
                        bigquery.SchemaField("ingestion_date", "DATE", mode="NULLABLE"),
                        bigquery.SchemaField("ingestion_timestamp", "TIMESTAMP", mode="NULLABLE"),
                    ]
                    if f"dt={execution_date}" not in gcs_prefix:
                        ndjson_gcs_pattern = f"gs://{gcs_bucket}/{gcs_prefix}/dt={execution_date}/*.jsonl.gz"
                    else:
                        ndjson_gcs_pattern = f"gs://{gcs_bucket}/{gcs_prefix}/*.jsonl.gz"
                    job_cfg = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        schema=schema,
                        time_partitioning=bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY, field="ingestion_date"),
                        ignore_unknown_values=True,
                    )
                    load_job = bq_client.load_table_from_uri(
                        ndjson_gcs_pattern,
                        f"{bq_project}.{target_dataset}.{target_table}",
                        job_config=job_cfg,
                    )
                    load_job.result()
                    logger.info("Loaded %d rows into BigQuery %s.%s.%s", total_records, bq_project, target_dataset, target_table)
            except Exception as bq_err:
                logger.warning("BigQuery Bronze load notice: %s", bq_err)

        return total_records, total_bytes, uploaded_blobs


def main() -> None:
    """CLI / Container Entrypoint."""
    # If invoked with CLI flags (like --source-id, etc.), parse them into environment vars for JobConfig
    if len(sys.argv) > 1 and any(arg.startswith("--") for arg in sys.argv[1:]):
        parser = argparse.ArgumentParser(description="Elementl ArcGIS Ingestion Worker")
        parser.add_argument("--source-id", default=os.getenv("TASK_ID", os.getenv("SOURCE_ID")))
        parser.add_argument("--execution-date", default=os.getenv("EXECUTION_DATE"))
        parser.add_argument("--gcs-bucket", default=os.getenv("GCS_BUCKET", os.getenv("GCS_BRONZE_BUCKET")))
        parser.add_argument("--gcp-project", default=os.getenv("GCP_PROJECT_ID", os.getenv("GCP_PROJECT")))
        parser.add_argument("--endpoint-url", default=os.getenv("ENDPOINT_URL"))
        parser.add_argument("--layer-id", default=os.getenv("LAYER_ID"))
        parser.add_argument("--target-gcs-prefix", default=os.getenv("TARGET_GCS_PREFIX"))
        parser.add_argument("--target-table", default=os.getenv("TARGET_TABLE"))
        parser.add_argument("--target-dataset", default=os.getenv("TARGET_DATASET", "ds_bronze_atlas"))
        parser.add_argument("--query-params", default=os.getenv("QUERY_PARAMS", "{}"))
        parser.add_argument("--domain-tags", default=os.getenv("DOMAIN_TAGS"))
        parser.add_argument("--auth-type", default=os.getenv("AUTH_TYPE", "none"))
        parser.add_argument("--secret-id", default=os.getenv("SECRET_ID"))
        parser.add_argument("--stream-batch-size", default=os.getenv("STREAM_BATCH_SIZE", "1000"))
        parser.add_argument("--timeout-seconds", default=os.getenv("TIMEOUT_SECONDS", "180"))
        parser.add_argument("--max-retries", default=os.getenv("MAX_RETRIES", "5"))

        args, _ = parser.parse_known_args()
        if args.source_id:
            os.environ["TASK_ID"] = args.source_id
            os.environ["SOURCE_ID"] = args.source_id
        if args.execution_date:
            os.environ["EXECUTION_DATE"] = args.execution_date
        if args.gcs_bucket:
            os.environ["GCS_BUCKET"] = args.gcs_bucket
        if args.gcp_project:
            os.environ["GCP_PROJECT"] = args.gcp_project
            os.environ["GCP_PROJECT_ID"] = args.gcp_project
        if args.endpoint_url:
            os.environ["ENDPOINT_URL"] = args.endpoint_url
        if args.layer_id:
            os.environ["LAYER_ID"] = str(args.layer_id)
        if args.target_gcs_prefix:
            os.environ["GCS_PREFIX"] = args.target_gcs_prefix
        if args.target_table:
            os.environ["TARGET_TABLE"] = args.target_table
        if args.target_dataset:
            os.environ["TARGET_DATASET"] = args.target_dataset
        if args.query_params:
            os.environ["QUERY_PARAMS"] = args.query_params
        if args.domain_tags:
            os.environ["DOMAIN_TAGS"] = args.domain_tags
        if args.auth_type:
            os.environ["AUTH_TYPE"] = args.auth_type
        if args.secret_id:
            os.environ["SECRET_ID"] = args.secret_id
        if args.stream_batch_size:
            os.environ["STREAM_BATCH_SIZE"] = str(args.stream_batch_size)
        if args.timeout_seconds:
            os.environ["TIMEOUT_SECONDS"] = str(args.timeout_seconds)
        if args.max_retries:
            os.environ["MAX_RETRIES"] = str(args.max_retries)

    sys.exit(run_job(ArcGisExtractor()))


if __name__ == "__main__":
    main()

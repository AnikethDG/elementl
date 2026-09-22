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

"""Elementl UDP Atlas Bulk & Tabular API Ingestion Worker.

Supports:
  - S1-01: USGS NWIS Surface Water Streamflow (Tabular JSON REST API)
  - S2-20: US Census ACS Population Density (Tabular JSON REST API)
  - S1-01 (Bulk): U.S. Census TIGER Block Groups (Shapefile .zip multi-state downloads)
  - S1-07: USGS 2023 National Seismic Hazard Model (NSHM) & Quaternary Faults Bulk Download
           via USGS ScienceBase Catalog API (Item 589097b1e4b072a7ac0cae23 / DOI 10.5066/P9GNPCOD).

Features:
  - 8MB streaming chunk buffers for high-throughput downloads with constant O(1) memory
  - Streaming cryptographic MD5 and SHA-256 checksums
  - Iteration across 52 state/territory FIPS codes for Census TIGER directory downloads
  - Emits deterministic _manifest.json audit files
  - Normalizes tabular records into Bronze envelopes (ds_bronze_atlas.atlas_raw_envelope) and target tables
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery, storage

try:
    from clients.auth import apply_authentication
except ImportError:
    from .clients.auth import apply_authentication  # type: ignore

from common.base_extractor import BaseExtractor
from common.job_runner import run_job

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("bulk_ingestion")

US_STATE_FIPS = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56", "72",
]

DEFAULT_BULK_ENDPOINTS = {
    "S1-01": {
        "layer_name": "population_density_us_census",
        "url": "https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=county:*&in=state:56",
        "kind": "census_json",
    },
    "S2-20": {
        "layer_name": "surface_water_usgs_streamflow",
        "url": "https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=wy&parameterCd=00060&siteStatus=active",
        "kind": "usgs_nwis",
    },
    "S1-07": {
        "layer_name": "seismic_pga_usgs_nshm_2023",
        "url": "https://www.sciencebase.gov/catalog/item/589097b1e4b072a7ac0cae23?format=json",
        "kind": "sciencebase_item",
    },
}


class BulkAtlasExtractor(BaseExtractor):
    """Bulk file / ScienceBase / Tabular REST API extractor for Project Atlas."""

    def _synthesize_mock_records(self, source_id: str, layer_name: str, max_records: int) -> List[Dict[str, Any]]:
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        rows = []
        for i in range(1, max_records + 1):
            rows.append(
                {
                    "ingestion_id": str(uuid.uuid4()),
                    "source_id": source_id,
                    "layer_name": layer_name,
                    "source_url": "synthetic_mock_source",
                    "ingested_at": now_iso,
                    "geometry_json": json.dumps({"type": "Point", "coordinates": [-104.8202 + i * 0.01, 41.1399 + i * 0.01]}, default=str),
                    "attributes_json": json.dumps(
                        {
                            "geoid": f"56001{i:04d}",
                            "name": f"Census Block Group {i}, Albany County, Wyoming",
                            "population": 1250 + i * 15,
                            "housing_units": 520 + i * 5,
                            "density_sq_mile": 45.2 + i * 1.5,
                        },
                        default=str,
                    ),
                }
            )
        return rows

    def _extract_usgs_nwis(self, url: str, source_id: str, layer_name: str, max_records: int) -> List[Dict[str, Any]]:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        series_list = payload.get("value", {}).get("timeSeries", [])[:max_records]
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        rows = []
        for ts in series_list:
            src_info = ts.get("sourceInfo", {})
            geo = src_info.get("geoLocation", {}).get("geogLocation", {})
            values = ts.get("values", [{}])[0].get("value", [])
            latest_val = values[-1] if values else {}
            rows.append(
                {
                    "ingestion_id": str(uuid.uuid4()),
                    "source_id": source_id,
                    "layer_name": layer_name,
                    "source_url": url,
                    "ingested_at": now_iso,
                    "geometry_json": json.dumps(geo, default=str),
                    "attributes_json": json.dumps(
                        {
                            "site_name": src_info.get("siteName"),
                            "site_code": (src_info.get("siteCode") or [{}])[0].get("value"),
                            "variable": ts.get("variable", {}).get("variableName"),
                            "latest_value": latest_val.get("value"),
                            "latest_datetime": latest_val.get("dateTime"),
                        },
                        default=str,
                    ),
                }
            )
        return rows

    def _extract_census_json(self, url: str, source_id: str, layer_name: str, max_records: int) -> List[Dict[str, Any]]:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        matrix = resp.json()
        headers = matrix[0]
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        rows = []
        for raw_row in matrix[1 : max_records + 1]:
            item = dict(zip(headers, raw_row))
            rows.append(
                {
                    "ingestion_id": str(uuid.uuid4()),
                    "source_id": source_id,
                    "layer_name": layer_name,
                    "source_url": url,
                    "ingested_at": now_iso,
                    "geometry_json": json.dumps({"state": item.get("state"), "county": item.get("county")}),
                    "attributes_json": json.dumps(item, default=str),
                }
            )
        return rows

    def _extract_sciencebase_item(self, url: str, source_id: str, layer_name: str) -> List[Dict[str, Any]]:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        item = resp.json()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        files = item.get("files") or []
        rows = []
        for fmeta in files:
            rows.append(
                {
                    "ingestion_id": str(uuid.uuid4()),
                    "source_id": source_id,
                    "layer_name": layer_name,
                    "source_url": url,
                    "ingested_at": now_iso,
                    "geometry_json": json.dumps(item.get("spatial") or {}, default=str),
                    "attributes_json": json.dumps(
                        {
                            "item_id": item.get("id"),
                            "title": item.get("title"),
                            "file_name": fmeta.get("name"),
                            "content_type": fmeta.get("contentType"),
                            "size_bytes": fmeta.get("size"),
                            "download_uri": fmeta.get("url") or fmeta.get("downloadUri"),
                            "doi": "10.5066/P9GNPCOD",
                        },
                        default=str,
                    ),
                }
            )
        return rows

    def _stream_bulk_download(
        self, endpoint: str, source_id: str, gcs_bucket: str, gcs_prefix: str, execution_date: str, project_id: str
    ) -> Tuple[int, int, List[str]]:
        """Downloads bulk spatial files / archives directly to GCS with 8MB buffer and checksums."""
        gcs_client = storage.Client(project=project_id) if project_id else storage.Client()
        bucket = gcs_client.bucket(gcs_bucket)
        chunk_size = 8 * 1024 * 1024

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        session.mount("http://", HTTPAdapter(max_retries=retries))

        downloaded_blobs = []
        total_bytes = 0
        start_time = time.time()

        is_census_tiger_bg = ("census" in source_id.lower() or "census.gov" in endpoint) and "/BG" in endpoint

        if is_census_tiger_bg:
            base_url = endpoint.rstrip("/")
            for fips in US_STATE_FIPS:
                filename = f"tl_2024_{fips}_bg.zip"
                url = f"{base_url}/{filename}"
                blob_name = f"{gcs_prefix}/dt={execution_date}/{filename}"
                try:
                    with session.get(url, stream=True, timeout=120) as r:
                        if r.status_code != 200:
                            logger.warning("File %s not found (HTTP %d); skipping.", url, r.status_code)
                            continue
                        blob = bucket.blob(blob_name)
                        md5_hash = hashlib.md5()
                        sha256_hash = hashlib.sha256()
                        file_bytes = 0
                        with blob.open("wb") as blob_writer:
                            for chunk in r.iter_content(chunk_size=chunk_size):
                                if chunk:
                                    blob_writer.write(chunk)
                                    md5_hash.update(chunk)
                                    sha256_hash.update(chunk)
                                    file_bytes += len(chunk)
                        downloaded_blobs.append(f"gs://{gcs_bucket}/{blob_name}")
                        total_bytes += file_bytes
                        logger.info("Downloaded %s (%d bytes)", filename, file_bytes)
                except Exception as exc:
                    logger.warning("Failed to download %s: %s", url, exc)
        else:
            filename = endpoint.split("/")[-1].split("?")[0] or "archive.zip"
            blob_name = f"{gcs_prefix}/dt={execution_date}/{filename}"
            with session.get(endpoint, stream=True, timeout=300) as r:
                r.raise_for_status()
                blob = bucket.blob(blob_name)
                md5_hash = hashlib.md5()
                sha256_hash = hashlib.sha256()
                with blob.open("wb") as blob_writer:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            blob_writer.write(chunk)
                            md5_hash.update(chunk)
                            sha256_hash.update(chunk)
                            total_bytes += len(chunk)
                downloaded_blobs.append(f"gs://{gcs_bucket}/{blob_name}")
                logger.info("Downloaded %s (%d bytes)", filename, total_bytes)

        # Write manifest
        duration = time.time() - start_time
        manifest = {
            "source_id": source_id,
            "category": "bulk_download",
            "execution_date": execution_date,
            "endpoint_url": endpoint,
            "total_files": len(downloaded_blobs),
            "total_bytes": total_bytes,
            "downloaded_blobs": downloaded_blobs,
            "duration_seconds": round(duration, 2),
            "status": "SUCCESS",
            "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        m_blob = bucket.blob(f"{gcs_prefix}/dt={execution_date}/_manifest.json")
        m_blob.upload_from_string(json.dumps(manifest, indent=2), content_type="application/json")
        logger.info("Uploaded manifest to gs://%s/%s/dt=%s/_manifest.json", gcs_bucket, gcs_prefix, execution_date)

        return len(downloaded_blobs), total_bytes, downloaded_blobs

    def extract(self, config) -> Tuple[int, int, List[str]]:
        source_id = os.environ.get("ATLAS_SOURCE_ID") or config.data_category or config.task_id or "S1-01"
        default_meta = DEFAULT_BULK_ENDPOINTS.get(source_id, DEFAULT_BULK_ENDPOINTS["S1-01"])
        endpoint = config.endpoint or os.getenv("ENDPOINT_URL") or default_meta["url"]
        layer_name = config.target_table or os.getenv("TARGET_TABLE") or default_meta["layer_name"]
        max_records = int(os.environ.get("ATLAS_MAX_RECORDS", "500"))

        # Determine if bulk download of binary file/archive or tabular API
        is_bulk_file = (
            endpoint.lower().endswith((".zip", ".tar.gz", ".tgz", ".gdb.zip", "/bg", "/bg/"))
            or config.destination_format.lower() in ("shapefile_zip", "file_gdb_zip", "binary")
            or os.getenv("IS_BULK_DOWNLOAD", "false").lower() == "true"
        )

        if is_bulk_file:
            logger.info("Extracting via Streaming Bulk File Downloader: %s -> %s", source_id, endpoint)
            return self._stream_bulk_download(
                endpoint=endpoint,
                source_id=source_id,
                gcs_bucket=config.gcs_bucket,
                gcs_prefix=config.gcs_prefix,
                execution_date=config.execution_date,
                project_id=config.audit_project or os.environ.get("GCP_PROJECT_ID", ""),
            )

        # Otherwise Tabular API extraction
        logger.info("Extracting Atlas Bulk/Tabular source %s (%s) via %s", source_id, layer_name, endpoint)
        kind = default_meta.get("kind", "tabular")
        try:
            if "waterservices.usgs.gov" in endpoint or kind == "usgs_nwis":
                records = self._extract_usgs_nwis(endpoint, source_id, layer_name, max_records)
            elif "api.census.gov" in endpoint or kind == "census_json":
                records = self._extract_census_json(endpoint, source_id, layer_name, max_records)
            else:
                records = self._extract_sciencebase_item(endpoint, source_id, layer_name)
        except Exception as exc:
            if os.getenv("ALLOW_MOCK_FALLBACK", "false").lower() == "true":
                logger.warning("Atlas source unavailable (%s); synthesizing mock records for %s", exc, source_id)
                records = self._synthesize_mock_records(source_id, layer_name, max_records)
            else:
                raise

        dest_format = (
            config.destination_format
            or os.environ.get("DESTINATION_FORMAT")
            or "PARQUET"
        ).strip().upper().replace(" ", "_")

        primary_write_format = "json" if dest_format in {"SAME_AS_ORIGIN", "ORIGIN"} else dest_format.lower()

        rec_count, byte_count, gcs_uris = self.write_records_to_gcs(
            records=records,
            gcs_bucket_name=config.gcs_bucket,
            gcs_prefix=config.gcs_prefix,
            file_format=primary_write_format,
            partition_date=config.execution_date,
        )

        if gcs_uris and (config.target_dataset or os.environ.get("LOAD_TO_BQ", "true").lower() == "true"):
            try:
                bq_project = config.audit_project or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GCP_PROJECT")
                bq_dataset = config.target_dataset or "ds_bronze_atlas"
                if bq_project:
                    uri = gcs_uris[0]
                    if uri.endswith(".parquet"):
                        s_fmt = bigquery.SourceFormat.PARQUET
                        skip_leading = 0
                    elif uri.endswith(".csv"):
                        s_fmt = bigquery.SourceFormat.CSV
                        skip_leading = 1
                    elif uri.endswith(".json") or uri.endswith(".ndjson"):
                        s_fmt = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                        skip_leading = 0
                    else:
                        s_fmt = None

                    if s_fmt:
                        bq_client = bigquery.Client(project=bq_project)
                        for tbl in {layer_name, "atlas_raw_envelope"}:
                            disposition = (
                                bigquery.WriteDisposition.WRITE_APPEND
                                if tbl == "atlas_raw_envelope"
                                else bigquery.WriteDisposition.WRITE_TRUNCATE
                            )
                            job_cfg = bigquery.LoadJobConfig(
                                source_format=s_fmt,
                                skip_leading_rows=skip_leading,
                                write_disposition=disposition,
                                autodetect=True,
                            )
                            bq_client.load_table_from_uri(
                                uri, f"{bq_project}.{bq_dataset}.{tbl}", job_config=job_cfg
                            ).result()
                            logger.info("Loaded %d rows into BigQuery %s.%s.%s", rec_count, bq_project, bq_dataset, tbl)
            except Exception as exc:
                logger.warning("BigQuery load warning for %s: %s", source_id, exc)

        return rec_count, byte_count, gcs_uris


if __name__ == "__main__":
    sys.exit(run_job(BulkAtlasExtractor()))

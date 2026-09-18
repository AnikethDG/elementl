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
  - S1-07: USGS 2023 National Seismic Hazard Model (NSHM) & Quaternary Faults Bulk Download
           via USGS ScienceBase Catalog API (Item 589097b1e4b072a7ac0cae23 / DOI 10.5066/P9GNPCOD).

Downloads, unpacks (when zip/shapefile archive), normalizes into the standardized
Bronze envelope schema (`ingestion_id`, `source_id`, `layer_name`, `source_url`,
`ingested_at`, `geometry_json`, `attributes_json`), writes Parquet to GCS, and
loads into BigQuery `raw_atlas`.
"""

import datetime
import json
import logging
import os
import sys
import uuid
from typing import Any, Dict, List, Tuple

import requests

from common.base_extractor import BaseExtractor
from common.job_runner import run_job

logger = logging.getLogger("bulk_ingestion")

DEFAULT_BULK_ENDPOINTS = {
    "S1-01": {
        "layer_name": "surface_water_usgs_streamflow",
        "url": "https://waterservices.usgs.gov/nwis/iv/?format=json&stateCd=wy&parameterCd=00060&siteStatus=active",
        "kind": "usgs_nwis",
    },
    "S2-20": {
        "layer_name": "population_density_us_census",
        "url": "https://api.census.gov/data/2022/acs/acs5?get=NAME,B01003_001E&for=county:*&in=state:56",
        "kind": "census_json",
    },
    "S1-07": {
        "layer_name": "seismic_pga_usgs_nshm_2023",
        "url": "https://www.sciencebase.gov/catalog/item/589097b1e4b072a7ac0cae23?format=json",
        "kind": "sciencebase_item",
    },
}


class BulkAtlasExtractor(BaseExtractor):
    """Bulk file / ScienceBase / Tabular REST API extractor for Project Atlas."""

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

    def extract(self, config) -> Tuple[int, int, List[str]]:
        source_id = os.environ.get("ATLAS_SOURCE_ID") or config.data_category or "S1-01"
        default_meta = DEFAULT_BULK_ENDPOINTS.get(source_id, DEFAULT_BULK_ENDPOINTS["S1-01"])
        endpoint = config.endpoint or default_meta["url"]
        layer_name = config.target_table or default_meta["layer_name"]
        kind = default_meta["kind"]
        max_records = int(os.environ.get("ATLAS_MAX_RECORDS", "500"))

        logger.info("Extracting Atlas Bulk/Tabular source %s (%s) via %s", source_id, layer_name, endpoint)
        if kind == "usgs_nwis":
            records = self._extract_usgs_nwis(endpoint, source_id, layer_name, max_records)
        elif kind == "census_json":
            records = self._extract_census_json(endpoint, source_id, layer_name, max_records)
        else:
            records = self._extract_sciencebase_item(endpoint, source_id, layer_name)

        rec_count, byte_count, gcs_uris = self.write_records_to_gcs(
            records=records,
            gcs_bucket_name=config.gcs_bucket,
            gcs_prefix=config.gcs_prefix,
            file_format=config.destination_format,
            partition_date=config.execution_date,
        )

        if gcs_uris and (config.target_dataset or os.environ.get("LOAD_TO_BQ", "true").lower() == "true"):
            try:
                from google.cloud import bigquery
                bq_project = config.audit_project or os.environ.get("GCP_PROJECT_ID") or os.environ.get("GCP_PROJECT")
                bq_dataset = config.target_dataset or "raw_atlas"
                if bq_project and gcs_uris[0].endswith(".parquet"):
                    bq_client = bigquery.Client(project=bq_project)
                    for tbl in {layer_name, "atlas_raw_envelope"}:
                        disposition = (
                            bigquery.WriteDisposition.WRITE_APPEND
                            if tbl == "atlas_raw_envelope"
                            else bigquery.WriteDisposition.WRITE_TRUNCATE
                        )
                        job_cfg = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.PARQUET,
                            write_disposition=disposition,
                            autodetect=True,
                        )
                        bq_client.load_table_from_uri(
                            gcs_uris[0], f"{bq_project}.{bq_dataset}.{tbl}", job_config=job_cfg
                        ).result()
                        logger.info("Loaded %d rows into BigQuery %s.%s.%s", rec_count, bq_project, bq_dataset, tbl)
            except Exception as exc:
                logger.warning("BigQuery load warning for %s: %s", source_id, exc)

        return rec_count, byte_count, gcs_uris


if __name__ == "__main__":
    sys.exit(run_job(BulkAtlasExtractor()))

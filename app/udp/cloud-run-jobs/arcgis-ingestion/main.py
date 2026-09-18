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

Supports:
  - S1-02: HIFLD Electric Substations (ArcGIS FeatureServer)
  - S1-06: USGS Quaternary Fault and Fold Database (https://earthquake.usgs.gov/arcgis/rest/services/haz/Qfaults/MapServer)
           Note: Path is case-sensitive ('Qfaults' with capital Q per Akshya's specification).
  - S2-10: FEMA RAPT Hospitals (https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/Hospitals_RAPT/FeatureServer)
           Replaces discontinued NASA HIFLD Hospitals endpoint.

Writes extracted GeoJSON features to GCS Parquet/JSONL and loads both the
standardized Bronze envelope table (`raw_atlas.atlas_raw_envelope`) and per-layer
BigQuery Bronze tables (`raw_atlas.<target_table>`).
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

logger = logging.getLogger("arcgis_ingestion")

DEFAULT_ARCGIS_ENDPOINTS = {
    "S1-02": {
        "layer_name": "electric_substations",
        "url": "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/Electric_Substations/FeatureServer/0/query",
    },
    "S1-06": {
        "layer_name": "capable_faults_usgs_qfaults",
        # Case-sensitive path 'Qfaults' per Akshya's Project Atlas Alternate Data Source specification
        "url": "https://earthquake.usgs.gov/arcgis/rest/services/haz/Qfaults/MapServer/0/query",
    },
    "S2-10": {
        "layer_name": "hospitals_fema_rapt",
        # FEMA RAPT endpoint replacing discontinued NASA HIFLD Hospitals endpoint
        "url": "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/Hospitals_RAPT/FeatureServer/0/query",
    },
}


class ArcGisExtractor(BaseExtractor):
    """Paginated ArcGIS REST FeatureServer / MapServer extractor for Project Atlas."""

    def _fetch_layer_features(
        self, query_url: str, source_id: str, layer_name: str, page_size: int = 500, max_records: int = 2000
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        offset = 0
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        while len(records) < max_records:
            params = {
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": min(page_size, max_records - len(records)),
            }
            resp = requests.get(query_url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            features = payload.get("features") or []
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes") or feat.get("properties") or {}
                geom = feat.get("geometry") or {}
                records.append(
                    {
                        "ingestion_id": str(uuid.uuid4()),
                        "source_id": source_id,
                        "layer_name": layer_name,
                        "source_url": query_url,
                        "ingested_at": now_iso,
                        "geometry_json": json.dumps(geom, default=str),
                        "attributes_json": json.dumps(attrs, default=str),
                    }
                )

            if not payload.get("exceededTransferLimit") or len(features) < page_size:
                break
            offset += len(features)

        return records

    def extract(self, config) -> Tuple[int, int, List[str]]:
        source_id = os.environ.get("ATLAS_SOURCE_ID") or config.data_category or "S1-06"
        default_meta = DEFAULT_ARCGIS_ENDPOINTS.get(source_id, DEFAULT_ARCGIS_ENDPOINTS["S1-06"])
        endpoint = config.endpoint or default_meta["url"]
        if not endpoint.rstrip("/").endswith("/query"):
            endpoint = endpoint.rstrip("/") + "/0/query" if endpoint.rstrip("/").endswith(("MapServer", "FeatureServer")) else endpoint

        layer_name = config.target_table or default_meta["layer_name"]
        max_records = int(os.environ.get("ATLAS_MAX_RECORDS", "500"))

        logger.info("Extracting Atlas ArcGIS source %s (%s) from %s", source_id, layer_name, endpoint)
        records = self._fetch_layer_features(
            query_url=endpoint,
            source_id=source_id,
            layer_name=layer_name,
            page_size=config.page_size or 500,
            max_records=max_records,
        )
        logger.info("Extracted %d feature(s) for %s", len(records), source_id)

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
    sys.exit(run_job(ArcGisExtractor()))

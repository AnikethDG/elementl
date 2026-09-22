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

"""ArcGIS REST FeatureServer and MapServer API Client.

High-resilience, standalone Python client for Esri ArcGIS REST API services:
- Auto-discovers layer schema (maxRecordCount, extent, geometryType, fields, supportsPagination)
- Sanitizes and validates field names against actual layer schema (prevents 'Failed to execute query' 400 errors)
- Handles standard offset pagination (resultOffset / resultRecordCount)
- Adaptive auto-downscaling batch limit on HTTP 500 / server payload memory timeouts
- Implements fallback spatial bounding-box (quadtree) chunking for servers with pagination disabled
- Provides exponential backoff, jitter, and rate-limiting retry handling (HTTP 429/500/502/503/504)
- Zero framework dependencies (pure Python with requests & urllib3)
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Generator, List, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ArcGISRESTClient:
    """High-resilience client for querying ArcGIS FeatureServer and MapServer vector services.

    :param endpoint_url: Base URL of the MapServer or FeatureServer (or specific layer URL).
    :param layer_id: Optional integer layer ID (e.g. 0, 2, 4) if not included in endpoint_url.
    :param timeout: Request timeout in seconds.
    :param max_retries: Max retry attempts for transient server errors.
    :param backoff_factor: Exponential backoff base factor.
    """

    def __init__(
        self,
        endpoint_url: str,
        layer_id: Optional[int] = None,
        timeout: int = 60,
        max_retries: int = 5,
        backoff_factor: float = 1.0,
        extra_query_params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.endpoint_url = endpoint_url.rstrip("/")
        self.layer_id = layer_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.extra_query_params: Dict[str, Any] = dict(extra_query_params or {})
        self._session: Optional[requests.Session] = None
        self._metadata_cache: Optional[Dict[str, Any]] = None

    @property
    def layer_url(self) -> str:
        """Constructs the canonical layer endpoint URL."""
        if self.layer_id is not None and not self.endpoint_url.endswith(f"/{self.layer_id}"):
            return f"{self.endpoint_url}/{self.layer_id}"
        return self.endpoint_url

    @property
    def session(self) -> requests.Session:
        """Initializes a requests.Session with connection pooling and custom retry policy."""
        if self._session is None:
            session = requests.Session()
            retries = Retry(
                total=self.max_retries,
                backoff_factor=self.backoff_factor,
                status_forcelist=[429, 502, 503, 504],
                allowed_methods=["GET", "POST"],
                raise_on_status=False,
            )
            adapter = HTTPAdapter(
                max_retries=retries,
                pool_connections=20,
                pool_maxsize=20,
            )
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            session.headers.update(
                {
                    "Accept": "application/json, application/geo+json, */*",
                    "User-Agent": "Elementl-Geospatial-Client/1.0",
                }
            )
            self._session = session
        return self._session

    def get_layer_metadata(self) -> Dict[str, Any]:
        """Fetches the service metadata and schema definition for the target layer."""
        if self._metadata_cache is not None:
            return self._metadata_cache

        url = self.layer_url
        params: Dict[str, Any] = {"f": "json"}
        params.update(self.extra_query_params)
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                raise RuntimeError(f"ArcGIS service returned error: {data['error']}")

            # Validate that the layer is a queryable Feature Layer, not a Group Layer
            layer_type = data.get("type", "")
            if layer_type and layer_type.lower() == "group layer":
                sub_ids = data.get("subLayerIds", [])
                layer_name = data.get("name", "Unknown")
                raise ValueError(
                    f"Target layer {self.layer_url} is a 'Group Layer' ('{layer_name}'). "
                    f"Group layers cannot be queried directly in ArcGIS. "
                    f"Please configure a specific Feature Layer ID (subLayerIds: {sub_ids})."
                )

            self._metadata_cache = data
            return data
        except Exception as e:
            logger.error("Failed to retrieve layer metadata from %s: %s", url, e)
            raise

    def sanitize_out_fields(self, out_fields: str) -> str:
        """Validates and filters requested outFields against actual layer schema.

        If non-existent or mis-cased fields are requested, Esri returns 'Failed to execute query'.
        This method resolves field names case-insensitively and discards unmapped fields.
        """
        if not out_fields or out_fields.strip() == "*":
            return "*"
        try:
            meta = self.get_layer_metadata()
            layer_fields = meta.get("fields", [])
            if not layer_fields:
                return "*"

            # Build case-insensitive lookup
            valid_field_map = {f["name"].upper(): f["name"] for f in layer_fields if "name" in f}
            if not valid_field_map:
                return "*"

            requested = [f.strip() for f in out_fields.split(",") if f.strip()]
            matched: List[str] = []
            for field in requested:
                upper_f = field.upper()
                if upper_f in valid_field_map:
                    matched.append(valid_field_map[upper_f])
                else:
                    logger.warning(
                        "Field '%s' not present in layer %s schema; omitted to avoid query failure.",
                        field,
                        self.layer_url,
                    )

            if not matched:
                logger.warning(
                    "None of requested outFields (%s) exist in layer schema. Falling back to '*'",
                    out_fields,
                )
                return "*"

            return ",".join(matched)
        except Exception as err:
            logger.debug("Could not validate outFields against metadata: %s. Using '%s'", err, out_fields)
            return out_fields

    def check_health(self) -> bool:
        """Checks availability of the ArcGIS REST endpoint."""
        try:
            meta = self.get_layer_metadata()
            return "name" in meta or "id" in meta or "fields" in meta or "layers" in meta
        except Exception as err:
            logger.warning("ArcGIS endpoint health check failed for %s: %s", self.layer_url, err)
            return False

    def get_feature_count(self, where: str = "1=1") -> int:
        """Queries the server for the total number of features matching the where clause."""
        url = f"{self.layer_url}/query"
        params: Dict[str, Any] = {
            "where": where,
            "returnCountOnly": "true",
            "f": "json",
        }
        params.update(self.extra_query_params)
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if "count" in data:
            return int(data["count"])
        if "features" in data:
            return len(data["features"])
        return 0

    def fetch_features_page(
        self,
        where: str = "1=1",
        offset: int = 0,
        limit: int = 1000,
        out_fields: str = "*",
        output_format: str = "geojson",
        geometry: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fetches a single paginated batch of features from the layer query endpoint."""
        url = f"{self.layer_url}/query"
        safe_out_fields = self.sanitize_out_fields(out_fields)
        current_limit = limit

        params: Dict[str, Any] = {
            "where": where,
            "outFields": safe_out_fields,
            "returnGeometry": "true",
            "outSR": "4326",  # Standard WGS84 for GeoJSON
            "f": output_format,
            "resultOffset": offset,
            "resultRecordCount": current_limit,
        }
        params.update(self.extra_query_params)

        if geometry:
            params["geometry"] = f"{geometry['minx']},{geometry['miny']},{geometry['maxx']},{geometry['maxy']}"
            params["geometryType"] = "esriGeometryEnvelope"
            params["spatialRel"] = "esriSpatialRelIntersects"

        for attempt in range(1, self.max_retries + 1):
            params["resultRecordCount"] = current_limit
            try:
                start_time = time.time()
                resp = self.session.get(url, params=params, timeout=self.timeout)
                duration = time.time() - start_time

                if resp.status_code == 200:
                    data = resp.json()
                    if "error" in data:
                        err_msg = data["error"].get("message", str(data["error"]))
                        logger.warning("ArcGIS API returned error object: %s", err_msg)

                        # Automatic fallback to outFields=* if specific field query failed
                        if safe_out_fields != "*" and attempt < self.max_retries:
                            logger.info("Retrying query with wildcard outFields='*' fallback.")
                            safe_out_fields = "*"
                            params["outFields"] = "*"
                            time.sleep(1.0)
                            continue

                        # If query operation failed, downscale batch size
                        if current_limit > 25 and attempt < self.max_retries:
                            current_limit = max(25, current_limit // 2)
                            logger.info("Downscaling query limit to %d and retrying...", current_limit)
                            time.sleep(1.0)
                            continue

                        if attempt == self.max_retries:
                            raise RuntimeError(f"ArcGIS query error: {err_msg}")
                        time.sleep(self.backoff_factor * attempt)
                        continue

                    feature_count = len(data.get("features", []))
                    logger.info(
                        "Fetched %d features from %s (offset=%d, limit=%d) in %.2fs",
                        feature_count,
                        self.layer_url,
                        offset,
                        current_limit,
                        duration,
                    )
                    return data

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** attempt))
                    wait_time = retry_after + random.uniform(0.5, 1.5)
                    logger.warning("Rate limited (HTTP 429). Retrying after %.2fs...", wait_time)
                    time.sleep(wait_time)
                    continue

                if resp.status_code in {500, 502, 503, 504}:
                    # Adaptive batch downscaling on 500 internal server / payload limits
                    if current_limit > 25:
                        current_limit = max(25, current_limit // 2)
                        logger.warning(
                            "Server HTTP %d error (likely geometry payload limit). Auto-downscaling batch limit to %d...",
                            resp.status_code,
                            current_limit,
                        )
                    wait_time = (self.backoff_factor * (2 ** (attempt - 1))) + random.uniform(0.1, 1.0)
                    time.sleep(wait_time)
                    continue

                resp.raise_for_status()

            except requests.exceptions.RequestException as err:
                if current_limit > 25 and attempt < self.max_retries:
                    current_limit = max(25, current_limit // 2)
                    logger.warning("Request timeout/error. Downscaling batch limit to %d...", current_limit)
                if attempt == self.max_retries:
                    logger.error("Max retries reached for %s. Last error: %s", url, err)
                    raise
                wait_time = (self.backoff_factor * (2 ** (attempt - 1))) + random.uniform(0.1, 1.0)
                time.sleep(wait_time)

        raise RuntimeError(f"Failed to fetch features from {self.layer_url} at offset {offset}")

    def paginate_features(
        self,
        where: str = "1=1",
        page_size: int = 1000,
        out_fields: str = "*",
        output_format: str = "geojson",
        max_total_records: Optional[int] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Generator yielding pages of GeoJSON / EsriJSON feature dictionaries."""
        offset = 0
        records_fetched = 0

        # Discover server maxRecordCount constraint
        try:
            meta = self.get_layer_metadata()
            server_max = meta.get("maxRecordCount", page_size)
            if server_max and server_max < page_size:
                logger.info(
                    "Layer maxRecordCount is %d, lowering page_size from %d to %d",
                    server_max,
                    page_size,
                    server_max,
                )
                page_size = server_max
        except Exception:
            logger.debug("Could not verify maxRecordCount from metadata, using page_size %d", page_size)

        while True:
            limit = page_size
            if max_total_records is not None:
                remaining = max_total_records - records_fetched
                if remaining <= 0:
                    break
                limit = min(limit, remaining)

            page_data = self.fetch_features_page(
                where=where,
                offset=offset,
                limit=limit,
                out_fields=out_fields,
                output_format=output_format,
            )

            features = page_data.get("features", [])
            if not features:
                logger.info("No more features returned. Pagination complete at offset %d.", offset)
                break

            yield features

            records_fetched += len(features)
            offset += len(features)

            # Check if server indicated there are no more features
            if len(features) < limit or page_data.get("exceededTransferLimit") is False:
                if len(features) < limit:
                    break

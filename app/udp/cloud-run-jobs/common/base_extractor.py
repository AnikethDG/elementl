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

"""
Base extractor for the Elementl Cloud Run ingestion jobs.

Provides Secret Manager access, record normalisation and GCS writes in
Parquet / JSONL.
"""

import io
import os
import json
import logging
import datetime
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage, secretmanager

logger = logging.getLogger("base_extractor")


class BaseExtractor(ABC):
    """Abstract base class for all source extractors."""

    def __init__(self) -> None:
        self.storage_client = storage.Client()
        self._secret_client: Optional[secretmanager.SecretManagerServiceClient] = None

    # ------------------------------------------------------------------ secrets

    @property
    def secret_client(self) -> secretmanager.SecretManagerServiceClient:
        if self._secret_client is None:
            self._secret_client = secretmanager.SecretManagerServiceClient()
        return self._secret_client

    def _qualify(self, secret_resource_id: str) -> str:
        """Expands a bare secret name into a fully qualified resource id."""
        if secret_resource_id.startswith("projects/"):
            return secret_resource_id
        project_id = os.environ.get("GCP_PROJECT") or os.environ.get(
            "GOOGLE_CLOUD_PROJECT", ""
        )
        if not project_id:
            raise ValueError(
                "Short secret id supplied but neither GCP_PROJECT nor "
                "GOOGLE_CLOUD_PROJECT is set."
            )
        return f"projects/{project_id}/secrets/{secret_resource_id}/versions/latest"

    def get_secret_bytes(self, secret_resource_id: str) -> bytes:
        """Retrieves a secret payload as raw bytes.

        Used for private keys and CA bundles: the payload is handed straight to
        the crypto/ssl layer, so it never needs to touch the filesystem.
        """
        name = self._qualify(secret_resource_id)
        logger.info("Retrieving secret %s", name)
        return self.secret_client.access_secret_version(name=name).payload.data

    def get_secret(self, secret_resource_id: str) -> str:
        """Retrieves a secret payload from Google Secret Manager."""
        return self.get_secret_bytes(secret_resource_id).decode("UTF-8")

    def parse_secret_json(self, secret_resource_id: str) -> Dict[str, Any]:
        """Retrieves and parses a JSON secret payload."""
        if not secret_resource_id:
            return {}
        return json.loads(self.get_secret(secret_resource_id))

    def resolve_pem(
        self,
        creds: Dict[str, Any],
        inline_key: str,
        secret_ref_key: str,
        what: str,
    ) -> str:
        """Resolves a PEM that may be supplied inline or by secret reference.

        Two shapes are supported so that a key can be rotated independently of
        the rest of the credentials blob:

          {"<inline_key>": "-----BEGIN ...-----\\n..."}
          {"<secret_ref_key>": "projects/p/secrets/s/versions/latest"}

        Returns PEM text. Never writes anything to disk.
        """
        inline = (creds.get(inline_key) or "").strip()
        if inline:
            logger.info("Using inline %s from the credentials secret.", what)
            return inline

        ref = (creds.get(secret_ref_key) or "").strip()
        if ref:
            logger.info("Using %s from referenced secret.", what)
            return self.get_secret(ref).strip()

        raise ValueError(
            f"No {what} available: set either '{inline_key}' or "
            f"'{secret_ref_key}' in the credentials secret."
        )

    # ------------------------------------------------------------------- writes

    @staticmethod
    def _coerce_temporal(item: Dict[str, Any]) -> Dict[str, Any]:
        """Best-effort conversion of date/time-looking strings to real types."""
        for key, value in list(item.items()):
            if not isinstance(value, str) or not value:
                continue
            lowered = key.lower()
            looks_temporal = (
                "date" in lowered or "time" in lowered or lowered.endswith("_at")
            )
            if not looks_temporal:
                continue
            try:
                candidate = value.replace("Z", "+00:00")
                if len(candidate) == 10 and "T" not in candidate and " " not in candidate:
                    item[key] = datetime.date.fromisoformat(candidate)
                else:
                    item[key] = datetime.datetime.fromisoformat(candidate)
            except ValueError:
                pass
        return item

    def write_records_to_gcs(
        self,
        records: List[Dict[str, Any]],
        gcs_bucket_name: str,
        gcs_prefix: str,
        file_format: str = "parquet",
        partition_date: Optional[str] = None,
    ) -> Tuple[int, int, List[str]]:
        """Writes records to GCS in Parquet or JSONL, returning (rows, bytes, uris)."""
        now_utc = datetime.datetime.now(datetime.timezone.utc)

        if partition_date:
            part_date_obj = datetime.date.fromisoformat(str(partition_date)[:10])
        else:
            part_date_obj = now_utc.date()

        if not records:
            logger.warning("No records to write to GCS for prefix %s", gcs_prefix)
            return 0, 0, []

        cleaned_records = []
        for record in records:
            item = self._coerce_temporal(dict(record))
            item["ingestion_date"] = part_date_obj
            item["ingestion_timestamp"] = now_utc
            cleaned_records.append(item)

        bucket = self.storage_client.bucket(gcs_bucket_name)
        prefix = gcs_prefix.strip("/")
        timestamp_str = now_utc.strftime("%Y%m%d_%H%M%S")
        fmt = file_format.lower()

        if fmt in {"parquet", "pq"}:
            filename = f"{prefix}/data_{timestamp_str}.parquet"
            table = pa.Table.from_pylist(cleaned_records)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")
            data_bytes = buffer.getvalue()
            content_type = "application/octet-stream"
        elif fmt in {"json", "jsonl", "ndjson"}:
            filename = f"{prefix}/data_{timestamp_str}.json"
            payload = "\n".join(json.dumps(r, default=str) for r in cleaned_records)
            data_bytes = (payload + "\n").encode("utf-8")
            content_type = "application/x-ndjson"
        else:
            raise ValueError(f"Unsupported destination file format: {file_format}")

        bucket.blob(filename).upload_from_string(data_bytes, content_type=content_type)
        gcs_uri = f"gs://{gcs_bucket_name}/{filename}"
        logger.info(
            "Uploaded %s records (%s bytes) to %s",
            len(cleaned_records),
            len(data_bytes),
            gcs_uri,
        )
        return len(cleaned_records), len(data_bytes), [gcs_uri]

    # -------------------------------------------------------------------- hooks

    @abstractmethod
    def extract(self, config) -> Tuple[int, int, List[str]]:
        """Runs the extraction, returning (records, bytes_written, gcs_uris)."""
        raise NotImplementedError

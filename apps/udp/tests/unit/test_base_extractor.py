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

"""Unit tests for BaseExtractor record transformation, temporal coercion, and GCS serialization."""

import io
import datetime
from unittest.mock import MagicMock, patch
import pytest
import pyarrow.parquet as pq

from common.base_extractor import BaseExtractor


class DummyExtractor(BaseExtractor):
    """Concrete subclass for testing BaseExtractor core methods."""

    def extract(self, config):
        return 0, 0, []


@pytest.mark.unit
class TestBaseExtractor:
    """Unit tests for BaseExtractor methods."""

    @pytest.fixture
    def extractor(self):
        with patch("google.cloud.storage.Client"):
            return DummyExtractor()

    def test_coerce_temporal_dates_and_timestamps(self, extractor):
        record = {
            "name": "Acme Corp",
            "created_date": "2026-03-10",
            "updated_at": "2026-03-10T14:20:00Z",
            "description": "2026-not-a-date-column",
        }
        coerced = extractor._coerce_temporal(dict(record))

        assert isinstance(coerced["created_date"], datetime.date)
        assert coerced["created_date"] == datetime.date(2026, 3, 10)
        assert isinstance(coerced["updated_at"], datetime.datetime)
        assert coerced["name"] == "Acme Corp"
        # Non-temporal column name shouldn't be parsed
        assert coerced["description"] == "2026-not-a-date-column"

    def test_write_records_to_gcs_parquet(self, extractor, sample_records):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        extractor.storage_client.bucket.return_value = mock_bucket

        rec_count, byte_count, uris = extractor.write_records_to_gcs(
            records=sample_records,
            gcs_bucket_name="bkt-elementl-509009-udp-bronze-raw",
            gcs_prefix="netsuite/raw/department",
            file_format="parquet",
            partition_date="2026-09-18",
        )

        assert rec_count == 2
        assert byte_count > 0
        assert len(uris) == 1
        assert uris[0].startswith("gs://bkt-elementl-509009-udp-bronze-raw/netsuite/raw/department/data_")
        assert uris[0].endswith(".parquet")

        # Verify that mock_blob received valid Snappy Parquet bytes
        mock_blob.upload_from_string.assert_called_once()
        uploaded_bytes = mock_blob.upload_from_string.call_args[0][0]
        assert isinstance(uploaded_bytes, bytes)

        # Inspect table structure from the in-memory parquet bytes
        table = pq.read_table(io.BytesIO(uploaded_bytes))
        assert table.num_rows == 2
        assert "ingestion_date" in table.column_names
        assert "ingestion_timestamp" in table.column_names

    def test_write_records_to_gcs_jsonl(self, extractor, sample_records):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        extractor.storage_client.bucket.return_value = mock_bucket

        rec_count, byte_count, uris = extractor.write_records_to_gcs(
            records=sample_records,
            gcs_bucket_name="bkt-elementl-509009-udp-bronze-raw",
            gcs_prefix="atlas/raw/water",
            file_format="jsonl",
            partition_date="2026-09-18",
        )

        assert rec_count == 2
        assert byte_count > 0
        assert uris[0].endswith(".json")
        mock_blob.upload_from_string.assert_called_once()
        content_type = mock_blob.upload_from_string.call_args[1].get("content_type")
        assert content_type == "application/x-ndjson"

    def test_write_records_to_gcs_csv(self, extractor, sample_records):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        extractor.storage_client.bucket.return_value = mock_bucket

        rec_count, byte_count, uris = extractor.write_records_to_gcs(
            records=sample_records,
            gcs_bucket_name="bkt-elementl-509009-udp-bronze-raw",
            gcs_prefix="netsuite/raw/account",
            file_format="csv",
            partition_date="2026-09-18",
        )

        assert rec_count == 2
        assert byte_count > 0
        assert len(uris) == 1
        assert uris[0].endswith(".csv")
        mock_blob.upload_from_string.assert_called_once()
        uploaded_bytes = mock_blob.upload_from_string.call_args[0][0]
        content_type = mock_blob.upload_from_string.call_args[1].get("content_type")
        assert content_type == "text/csv"

        csv_text = uploaded_bytes.decode("utf-8")
        lines = [line for line in csv_text.splitlines() if line.strip()]
        assert len(lines) == 3  # Header + 2 data rows
        assert "ingestion_date" in lines[0]
        assert "ingestion_timestamp" in lines[0]

    def test_write_records_to_gcs_same_as_origin(self, extractor, sample_records):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        extractor.storage_client.bucket.return_value = mock_bucket

        rec_count, byte_count, uris = extractor.write_records_to_gcs(
            records=sample_records,
            gcs_bucket_name="bkt-elementl-509009-udp-bronze-raw",
            gcs_prefix="atlas/raw/environment",
            file_format="SAME_AS_ORIGIN",
            partition_date="2026-09-18",
        )

        assert rec_count == 2
        assert byte_count > 0
        assert len(uris) == 1
        assert uris[0].endswith(".json")
        mock_blob.upload_from_string.assert_called_once()
        content_type = mock_blob.upload_from_string.call_args[1].get("content_type")
        assert content_type == "application/x-ndjson"

    def test_write_empty_records(self, extractor):
        rec_count, byte_count, uris = extractor.write_records_to_gcs(
            records=[],
            gcs_bucket_name="bkt-elementl-509009-udp-bronze-raw",
            gcs_prefix="empty/prefix",
        )
        assert rec_count == 0
        assert byte_count == 0
        assert uris == []

    def test_resolve_pem_inline_vs_reference(self, extractor):
        creds_inline = {"private_key_pem": "-----BEGIN PRIVATE KEY-----\nMIIB..."}
        pem = extractor.resolve_pem(creds_inline, "private_key_pem", "private_key_secret_id", "key")
        assert pem == "-----BEGIN PRIVATE KEY-----\nMIIB..."

        with patch.object(extractor, "get_secret", return_value="-----FROM SECRET-----"):
            creds_ref = {"private_key_secret_id": "projects/p/secrets/s/versions/1"}
            pem_ref = extractor.resolve_pem(creds_ref, "private_key_pem", "private_key_secret_id", "key")
            assert pem_ref == "-----FROM SECRET-----"

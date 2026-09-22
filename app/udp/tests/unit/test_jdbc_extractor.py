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

"""Unit tests for Primavera P6 JDBC ingestion worker and Oracle TCPS sanitization."""

import datetime
import decimal
import importlib
from unittest.mock import MagicMock
import pytest

jdbc_mod = importlib.import_module("jdbc-ingestion.main")
DEFAULT_P6_TABLE_MAP = jdbc_mod.DEFAULT_P6_TABLE_MAP
sanitize_value = jdbc_mod.sanitize_value
resolve_table_owner = jdbc_mod.resolve_table_owner
serialize_and_upload = jdbc_mod.serialize_and_upload


@pytest.mark.unit
class TestJDBCExtractor:
    """Unit tests for JDBC / Primavera P6 extraction helpers."""

    def test_canonical_p6_table_mapping(self):
        expected_keys = [
            "PROJECT",
            "WBS",
            "WBSCATEGORY",
            "ACTIVITY",
            "UDFVALUE",
            "UDFTYPE",
            "ACTIVITYCODE",
            "REFRDELETE",
        ]
        for key in expected_keys:
            assert key in DEFAULT_P6_TABLE_MAP
            folder_slug, bq_table_name = DEFAULT_P6_TABLE_MAP[key]
            assert isinstance(folder_slug, str)
            assert bq_table_name.startswith("p6_")

    def test_sanitize_value_types(self):
        assert sanitize_value(None) is None

        d = datetime.date(2026, 9, 18)
        assert sanitize_value(d) == "2026-09-18"
        dt_val = datetime.datetime(2026, 9, 18, 14, 30, 0)
        assert sanitize_value(dt_val) == "2026-09-18T14:30:00"

        assert sanitize_value(decimal.Decimal("123.45")) == 123.45
        assert sanitize_value(decimal.Decimal("100.00")) == 100

        assert sanitize_value(b"test_string") == "test_string"

        mock_reader = MagicMock()
        mock_reader.read.return_value = "stream_content"
        assert sanitize_value(mock_reader) == "stream_content"

    def test_resolve_table_owner_fallback(self):
        mock_cursor = MagicMock()
        # Mock cursor returning the preferred schema owner via fetchall()
        mock_cursor.fetchall.return_value = [("ELEMENTL_PMDB_SBOX_PXRPTUSER",)]

        owner = resolve_table_owner(mock_cursor, "PROJECT", "ELEMENTL_PMDB_SBOX_PXRPTUSER")
        assert owner == "ELEMENTL_PMDB_SBOX_PXRPTUSER"
        mock_cursor.execute.assert_called_once()

    def test_serialize_and_upload_formats(self):
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        sample_rows = [
            {"project_id": 1, "proj_name": "Apollo", "active": True},
            {"project_id": 2, "proj_name": "Artemis", "active": False},
        ]
        all_cols = ["project_id", "proj_name", "active"]

        # 1. Parquet
        path, byte_count = serialize_and_upload(
            bucket=mock_bucket,
            gcs_slug="projects",
            today_str="2026-09-22",
            batch_id="batch123",
            enriched_rows=sample_rows,
            all_cols=all_cols,
            dest_format="parquet",
        )
        assert path.endswith(".parquet")
        assert byte_count > 0
        assert mock_blob.upload_from_string.call_args[1]["content_type"] == "application/octet-stream"

        # 2. CSV
        path, byte_count = serialize_and_upload(
            bucket=mock_bucket,
            gcs_slug="projects",
            today_str="2026-09-22",
            batch_id="batch123",
            enriched_rows=sample_rows,
            all_cols=all_cols,
            dest_format="csv",
        )
        assert path.endswith(".csv")
        assert byte_count > 0
        assert mock_blob.upload_from_string.call_args[1]["content_type"] == "text/csv"

        # 3. JSON
        path, byte_count = serialize_and_upload(
            bucket=mock_bucket,
            gcs_slug="projects",
            today_str="2026-09-22",
            batch_id="batch123",
            enriched_rows=sample_rows,
            all_cols=all_cols,
            dest_format="json",
        )
        assert path.endswith(".json")
        assert byte_count > 0
        assert mock_blob.upload_from_string.call_args[1]["content_type"] == "application/x-ndjson"

        # 4. SAME_AS_ORIGIN should raise ValueError for JDBC P6
        with pytest.raises(ValueError, match="only allowed for Atlas"):
            serialize_and_upload(
                bucket=mock_bucket,
                gcs_slug="projects",
                today_str="2026-09-22",
                batch_id="batch123",
                enriched_rows=sample_rows,
                all_cols=all_cols,
                dest_format="SAME_AS_ORIGIN",
            )

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

"""Unit tests for Atlas Bulk and Tabular REST API extraction and normalization."""

import json
import importlib
from unittest.mock import MagicMock, patch
import pytest

bulk_mod = importlib.import_module("bulk-ingestion.main")
BulkAtlasExtractor = bulk_mod.BulkAtlasExtractor
US_STATE_FIPS = bulk_mod.US_STATE_FIPS


@pytest.mark.unit
class TestBulkExtractor:
    """Unit tests for Atlas Bulk/Tabular extraction."""

    def test_state_fips_completeness(self):
        # Must contain 52 FIPS entries (50 states + DC "11" + PR "72")
        assert len(US_STATE_FIPS) == 52
        assert "06" in US_STATE_FIPS  # California
        assert "48" in US_STATE_FIPS  # Texas
        assert "11" in US_STATE_FIPS  # District of Columbia
        assert "72" in US_STATE_FIPS  # Puerto Rico

    def test_extract_census_json_matrix(self):
        with patch("google.cloud.storage.Client"):
            extractor = BulkAtlasExtractor()

        # Census API returns matrix: row 0 is header, subsequent rows are data
        mock_matrix = [
            ["B01001_001E", "NAME", "state", "county"],
            ["10540", "Autauga County, Alabama", "01", "001"],
            ["22310", "Baldwin County, Alabama", "01", "003"],
        ]

        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_matrix
        mock_resp.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_resp):
            rows = extractor._extract_census_json(
                url="https://api.census.gov/data/test",
                source_id="S1-01",
                layer_name="population_density",
                max_records=10,
            )

        assert len(rows) == 2
        first_row = rows[0]
        assert first_row["source_id"] == "S1-01"
        assert first_row["layer_name"] == "population_density"
        assert "geometry_json" in first_row
        geo = json.loads(first_row["geometry_json"])
        assert geo["state"] == "01"
        assert geo["county"] == "001"

        attrs = json.loads(first_row["attributes_json"])
        assert attrs["B01001_001E"] == "10540"
        assert attrs["NAME"] == "Autauga County, Alabama"

    def test_extract_usgs_nwis(self):
        with patch("google.cloud.storage.Client"):
            extractor = BulkAtlasExtractor()

        mock_payload = {
            "value": {
                "timeSeries": [
                    {
                        "sourceInfo": {
                            "siteName": "PAWTUXET RIVER AT CRANSTON, RI",
                            "siteCode": [{"value": "01116500"}],
                            "geoLocation": {
                                "geogLocation": {"latitude": 41.750379, "longitude": -71.442284}
                            },
                        },
                        "variable": {"variableName": "Streamflow, ft3/s"},
                        "values": [{"value": [{"value": "45.2", "dateTime": "2026-09-18T10:00:00"}]}],
                    }
                ]
            }
        }

        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_payload
        mock_resp.raise_for_status.return_value = None

        with patch("requests.get", return_value=mock_resp):
            rows = extractor._extract_usgs_nwis(
                url="https://waterservices.usgs.gov/test",
                source_id="S2-20",
                layer_name="cooling_water_supply",
                max_records=5,
            )

        assert len(rows) == 1
        assert rows[0]["source_id"] == "S2-20"
        geo = json.loads(rows[0]["geometry_json"])
        assert geo["latitude"] == 41.750379
        attrs = json.loads(rows[0]["attributes_json"])
        assert attrs["site_code"] == "01116500"
        assert attrs["latest_value"] == "45.2"

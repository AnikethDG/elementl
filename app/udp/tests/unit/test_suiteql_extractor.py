"""Unit tests for NetSuite SuiteQL extractor, M2M JWT handling, and record cap enforcement."""

import os
import importlib
import pytest
from unittest.mock import MagicMock, patch

suiteql_mod = importlib.import_module("suiteql-ingestion.main")
_normalise_account_id = suiteql_mod._normalise_account_id
_max_records = suiteql_mod._max_records
HARD_MAX_RECORDS_CEILING = suiteql_mod.HARD_MAX_RECORDS_CEILING
NetSuiteExtractor = suiteql_mod.NetSuiteExtractor


@pytest.mark.unit
class TestSuiteQLExtractor:
    """Unit tests for NetSuite SuiteQL ingestion worker."""

    def test_account_id_normalisation(self):
        assert _normalise_account_id("1234567_SB1") == "1234567-sb1"
        assert _normalise_account_id("  9876543  ") == "9876543"
        assert _normalise_account_id("TSTDRV_12345") == "tstdrv-12345"

    def test_max_records_cap_enforcement(self):
        with patch.dict(os.environ, {}, clear=True):
            assert _max_records() == 10

        with patch.dict(os.environ, {"NETSUITE_MAX_RECORDS": "500"}, clear=True):
            assert _max_records() == HARD_MAX_RECORDS_CEILING

        with patch.dict(os.environ, {"NETSUITE_MAX_RECORDS": "0"}, clear=True):
            assert _max_records() == 1
        with patch.dict(os.environ, {"NETSUITE_MAX_RECORDS": "-10"}, clear=True):
            assert _max_records() == 1

        with patch.dict(os.environ, {"NETSUITE_MAX_RECORDS": "5"}, clear=True):
            assert _max_records() == 5

    def test_clean_drops_hateoas_links(self):
        extractor = NetSuiteExtractor()
        raw_row = {
            "id": "10",
            "name": "Engineering",
            "links": [{"rel": "self", "href": "https://..."}],
        }
        cleaned = extractor._clean(raw_row)
        assert "links" not in cleaned
        assert cleaned["id"] == "10"
        assert cleaned["name"] == "Engineering"

    def test_query_record_clamping(self):
        extractor = NetSuiteExtractor()

        # Mock requests.post returning 20 items
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "items": [{"id": str(i), "name": f"dept_{i}", "links": []} for i in range(20)],
            "hasMore": False,
        }

        with patch("requests.post", return_value=mock_response):
            rows = extractor._query(
                suiteql_url="https://test.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql",
                token="test_token",
                suiteql="SELECT id, name FROM department",
                max_records=10,
            )
            assert len(rows) == 10
            assert all("links" not in r for r in rows)

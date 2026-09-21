"""Unit tests for JobConfig parsing, validation, and serialization."""

import os
import pytest
from unittest.mock import patch

from common.job_config import JobConfig


@pytest.mark.unit
class TestJobConfig:
    """Test suite for JobConfig validation and parsing."""

    def test_valid_config_from_env(self):
        env = {
            "TASK_ID": "netsuite_department",
            "SOURCE_TYPE": "netsuite",
            "GCS_BUCKET": "test-bronze-bucket",
            "GCS_PREFIX": "netsuite/raw/department/dt=2026-09-18",
            "DESTINATION_FORMAT": "parquet",
            "AUDIT_PROJECT": "pid-nse-stg-core-apps-k8ti",
            "AUDIT_DATASET": "ds_operations",
            "AUDIT_TABLE": "ingestion_execution_logs",
            "TARGET_DATASET": "ds_bronze_netsuite",
            "TARGET_TABLE": "netsuite_department",
            "QUERY_PARAMS": '{"limit": 10, "active_only": true}',
            "PAGE_SIZE": "500",
            "TIMEOUT_SECONDS": "120",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = JobConfig.from_env()

        assert cfg.task_id == "netsuite_department"
        assert cfg.source_type == "netsuite"
        assert cfg.gcs_bucket == "test-bronze-bucket"
        assert cfg.destination_format == "parquet"
        assert cfg.page_size == 500
        assert cfg.timeout_seconds == 120
        assert cfg.query_params == {"limit": 10, "active_only": True}
        assert cfg.audit_enabled is True
        assert cfg.audit_full_table == "pid-nse-stg-core-apps-k8ti.ds_operations.ingestion_execution_logs"

    def test_missing_required_env_raises_value_error(self):
        env = {
            "TASK_ID": "test_task",
            "SOURCE_TYPE": "atlas",
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                JobConfig.from_env()
            assert "Missing required environment config" in str(exc_info.value)
            assert "gcs_bucket" in str(exc_info.value)

    def test_unrendered_jinja_template_rejected(self):
        env = {
            "TASK_ID": "test_task",
            "SOURCE_TYPE": "p6",
            "GCS_BUCKET": "my-bucket",
            "GCS_PREFIX": "p6/raw/{{ ds }}",
        }
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                JobConfig.from_env()
            assert "Unrendered Jinja template detected" in str(exc_info.value)

    def test_invalid_query_params_json_fallback(self):
        env = {
            "TASK_ID": "test_task",
            "SOURCE_TYPE": "p6",
            "GCS_BUCKET": "my-bucket",
            "GCS_PREFIX": "p6/raw/projects",
            "QUERY_PARAMS": "INVALID_NOT_JSON",
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = JobConfig.from_env()
            assert cfg.query_params == {}

    def test_redacted_query_truncation(self):
        long_query = ("SELECT * FROM large_table WHERE " + ("x = 1 AND " * 50)).strip()
        env = {
            "TASK_ID": "test_task",
            "SOURCE_TYPE": "netsuite",
            "GCS_BUCKET": "my-bucket",
            "GCS_PREFIX": "netsuite/raw",
            "QUERY": long_query,
        }
        with patch.dict(os.environ, env, clear=True):
            cfg = JobConfig.from_env()
            redacted = cfg.redacted()
            assert len(redacted["query"]) <= 203
            assert redacted["query"].endswith("...")
            assert cfg.query == long_query

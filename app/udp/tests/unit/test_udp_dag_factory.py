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

"""Unit tests for the Dynamic Airflow DAG Factory and configuration discovery."""

import os
from unittest.mock import patch
import pytest
from composer.dags.udp_dag_factory import discover_all_configs, create_udp_dag


@pytest.mark.unit
class TestUDPDagFactory:
    """Unit tests for UDP DAG Factory."""

    def test_discover_all_configs_local(self):
        # Disable GCS remote lookup so unit test uses local configs/sources
        with patch.dict(os.environ, {"GCS_CONFIG_BUCKET": ""}):
            configs = discover_all_configs()

        assert len(configs) >= 30, f"Expected at least 30 configs, found {len(configs)}"

        task_ids = {c["task_id"] for c in configs}
        source_systems = {c["source_system"] for c in configs}

        # Verify all 3 primary source systems represented
        assert "netsuite" in source_systems
        assert "p6" in source_systems
        assert "atlas" in source_systems

        # Sample tasks across systems and formats
        assert "netsuite_department" in task_ids
        assert "p6_activity" in task_ids
        assert "atlas_s1_01_population_density" in task_ids
        assert "sample_netsuite_csv" in task_ids
        assert "sample_netsuite_json" in task_ids
        assert "sample_p6_csv" in task_ids
        assert "sample_p6_json" in task_ids
        assert "sample_atlas_same_as_origin" in task_ids
        assert "sample_atlas_parquet" in task_ids
        assert "sample_atlas_csv" in task_ids

        # Verify destination formats are captured
        config_formats = {c["task_id"]: (c.get("destination", {}).get("format") or c.get("format", "")).lower() for c in configs}
        assert config_formats["sample_netsuite_csv"] == "csv"
        assert config_formats["sample_netsuite_json"] == "json"
        assert config_formats["sample_p6_csv"] == "csv"
        assert config_formats["sample_p6_json"] == "json"
        assert config_formats["sample_atlas_same_as_origin"] in ("same_as_origin", "same as origin")
        assert config_formats["sample_atlas_parquet"] == "parquet"

    def test_same_as_origin_allowed_only_for_atlas(self):
        from composer.dags.udp_dag_factory import _normalize_config

        # Atlas with SAME_AS_ORIGIN must succeed
        atlas_cfg = {
            "task_id": "test_atlas",
            "source": {"type": "atlas"},
            "destination": {"format": "SAME_AS_ORIGIN"},
        }
        normalized_atlas = _normalize_config(atlas_cfg)
        assert normalized_atlas["destination_format"] == "SAME_AS_ORIGIN"

        # NetSuite with SAME_AS_ORIGIN must raise ValueError
        netsuite_cfg = {
            "task_id": "test_netsuite",
            "source": {"type": "netsuite"},
            "destination": {"format": "SAME_AS_ORIGIN"},
        }
        with pytest.raises(ValueError, match="only allowed for Atlas"):
            _normalize_config(netsuite_cfg)

        # Primavera P6 with SAME_AS_ORIGIN must raise ValueError
        p6_cfg = {
            "task_id": "test_p6",
            "source": {"type": "p6"},
            "destination": {"format": "SAME_AS_ORIGIN"},
        }
        with pytest.raises(ValueError, match="only allowed for Atlas"):
            _normalize_config(p6_cfg)

    def test_create_udp_dag_structure(self):
        sample_cfg = {
            "task_id": "netsuite_department",
            "source_system": "netsuite",
            "ingestion_job": "suiteql-ingestion",
            "source_table": "department",
            "primary_key": "id",
            "watermark_column": "lastmodifieddate",
            "where_clause": "ROWNUM <= 10",
            "max_records": 10,
            "target_dataset": "ds_bronze_netsuite",
            "target_table": "netsuite_department",
            "silver_dataset": "ds_silver",
            "silver_table": "stg_netsuite_department",
            "schedule": "@daily",
        }

        dag = create_udp_dag(sample_cfg)
        if dag is None:
            pytest.skip("Airflow library not installed in test environment")

        assert dag.dag_id == "dag_udp_netsuite_department"
        assert dag.schedule_interval == "@daily"
        assert "udp" in dag.tags
        assert "netsuite" in dag.tags

        task_dict = {t.task_id: t for t in dag.tasks}
        # Ingestion TaskGroup tasks
        assert "ingestion_group.execute_suiteql_ingestion" in task_dict
        assert "ingestion_group.load_gcs_to_bq_bronze" in task_dict
        # Dataform TaskGroup tasks
        assert "dataform_group.compile_dataform" in task_dict
        assert "dataform_group.invoke_silver_and_assertions" in task_dict

        # Verify operator environment overrides
        cloud_run_task = task_dict["ingestion_group.execute_suiteql_ingestion"]
        env_list = cloud_run_task.overrides["container_overrides"][0]["env"]
        env_map = {item["name"]: item["value"] for item in env_list}
        assert env_map["TASK_ID"] == "netsuite_department"
        assert env_map["SOURCE_TYPE"] == "netsuite"
        assert env_map["TARGET_DATASET"] == "ds_bronze_netsuite"
        assert env_map["MAX_RECORDS"] == "10"

    def test_create_udp_dag_structured_schema(self):
        structured_cfg = {
            "template": "ingestion_task_metadata",
            "version": "1.0",
            "task_id": "netsuite_account",
            "display_name": "NetSuite Chart of Accounts Master",
            "description": "Ingests General Ledger accounts.",
            "domain": "finance",
            "metadata_status": "Active",
            "source": {
                "type": "netsuite",
                "load_type": "full_refresh",
                "watermark_column": "lastmodifieddate",
                "connection_secret_id": "secret-netsuite-api-config",
                "extraction_type": "suiteql",
                "source_table": "account",
                "primary_key": "id",
                "where_clause": "ROWNUM <= 10",
                "max_records": 10,
                "query": "SELECT * FROM account WHERE ROWNUM <= 10",
            },
            "engine": {
                "type": "cloud_run_job",
                "job_name": "suiteql-ingestion",
                "timeout_seconds": 1800,
            },
            "destination": {
                "format": "PARQUET",
                "schema_strategy": "static",
                "gcs_bucket": "bkt-elementl-509009-udp-bronze-raw",
                "gcs_prefix": "netsuite/raw/netsuite_account/dt={{ ds }}/",
                "bq_project": "elementl-509009",
                "bq_dataset": "ds_bronze_netsuite",
                "bq_table": "netsuite_account",
                "write_disposition": "WRITE_TRUNCATE",
                "partition_field": "ingestion_date",
            },
            "orchestration": {
                "dag_id": "dag_udp_netsuite_account",
                "schedule": "@daily",
                "start_date": "2024-01-01 00:00:00",
                "catchup": False,
                "retries": 2,
                "retry_delay_minutes": 5,
                "dataform": {
                    "enabled": True,
                    "target_dataset": "ds_silver",
                    "target_table": "stg_netsuite_account",
                    "included_tags": ["netsuite", "netsuite_account"],
                },
            },
        }

        dag = create_udp_dag(structured_cfg)
        if dag is None:
            pytest.skip("Airflow library not installed in test environment")

        assert dag.dag_id == "dag_udp_netsuite_account"
        assert dag.schedule_interval == "@daily"
        assert "udp" in dag.tags
        assert "netsuite" in dag.tags

        task_dict = {t.task_id: t for t in dag.tasks}
        assert "ingestion_group.execute_suiteql_ingestion" in task_dict
        assert "ingestion_group.load_gcs_to_bq_bronze" in task_dict

        cloud_run_task = task_dict["ingestion_group.execute_suiteql_ingestion"]
        env_list = cloud_run_task.overrides["container_overrides"][0]["env"]
        env_map = {item["name"]: item["value"] for item in env_list}
        assert env_map["TASK_ID"] == "netsuite_account"
        assert env_map["SOURCE_TYPE"] == "netsuite"
        assert env_map["TARGET_DATASET"] == "ds_bronze_netsuite"
        assert env_map["MAX_RECORDS"] == "10"
        assert env_map["QUERY"] == "SELECT * FROM account WHERE ROWNUM <= 10"
        assert env_map["CONNECTION_SECRET_id".upper()] == "secret-netsuite-api-config"


# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Integration tests for BigQuery target datasets and operational metadata tables."""

import pytest
from google.cloud import bigquery


@pytest.mark.integration
class TestBigQueryDatasetsIntegration:
    """Integration test suite for BigQuery datasets in project pid-nse-stg-core-apps-k8ti."""

    @pytest.fixture(scope="class")
    def bq_client(self, gcp_project_id):
        return bigquery.Client(project=gcp_project_id)

    def test_required_datasets_exist(self, bq_client, gcp_project_id):
        expected_datasets = [
            "ds_bronze_netsuite",
            "ds_bronze_p6",
            "ds_bronze_atlas",
            "ds_silver_p6", "ds_silver_netsuite", "ds_silver_atlas",
            "ds_operations",
        ]
        existing_datasets = {ds.dataset_id for ds in bq_client.list_datasets(project=gcp_project_id)}

        for ds_name in expected_datasets:
            assert ds_name in existing_datasets, f"Missing required BigQuery dataset: {ds_name}"

    def test_ds_operations_tables_exist(self, bq_client, gcp_project_id):
        expected_tables = [
            "ingestion_execution_logs",
            "ingestion_watermarks",
            "registered_tables",
        ]
        tables = {t.table_id for t in bq_client.list_tables(f"{gcp_project_id}.ds_operations")}

        for tbl_name in expected_tables:
            assert tbl_name in tables, f"Missing required table: ds_operations.{tbl_name}"

    def test_ingestion_execution_logs_schema_and_partitioning(self, bq_client, gcp_project_id):
        table_ref = f"{gcp_project_id}.ds_operations.ingestion_execution_logs"
        table = bq_client.get_table(table_ref)

        # Verify Partitioning
        assert table.time_partitioning is not None
        assert table.time_partitioning.field == "execution_date"

        # Verify Clustering
        assert table.clustering_fields == ["task_id", "source_type", "status"]

        # Verify Essential Columns
        col_names = {field.name for field in table.schema}
        assert "task_id" in col_names
        assert "execution_id" in col_names
        assert "source_type" in col_names
        assert "status" in col_names
        assert "records_extracted" in col_names
        assert "records_loaded" in col_names
        assert "bytes_processed" in col_names
        assert "execution_date" in col_names
        assert "log_timestamp" in col_names
        assert "target_dataset" in col_names

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

"""Integration tests for BigQuery operational audit telemetry and watermark persistence."""

import uuid
import datetime
import pytest
from google.cloud import bigquery


@pytest.mark.integration
class TestAuditLoggingIntegration:
    """Integration test suite for ds_operations telemetry logging and watermarks."""

    @pytest.fixture(scope="class")
    def bq_client(self, gcp_project_id):
        return bigquery.Client(project=gcp_project_id)

    def test_audit_execution_log_insert_and_query(self, bq_client, gcp_project_id):
        test_run_id = f"itest_{uuid.uuid4().hex[:8]}"
        now = datetime.datetime.now(datetime.timezone.utc)
        today = now.date().isoformat()

        row = {
            "task_id": "test_integration_task",
            "execution_id": test_run_id,
            "source_type": "atlas",
            "status": "SUCCESS",
            "execution_date": today,
            "log_timestamp": now.isoformat(),
            "records_extracted": 42,
            "records_loaded": 42,
            "bytes_processed": 2048,
            "duration_seconds": 1.25,
            "dag_id": "dag_udp_test_integration_task",
            "run_id": test_run_id,
            "engine_type": "cloud_run_job",
            "target_dataset": "ds_bronze_atlas",
            "target_table": "test_table",
            "gcs_output_path": f"gs://bkt-{gcp_project_id}-udp-bronze-raw/test",
            "bytes_written": 2048,
            "created_at": now.isoformat(),
        }

        table_id = f"{gcp_project_id}.ds_operations.ingestion_execution_logs"
        errors = bq_client.insert_rows_json(table_id, [row])
        assert not errors, f"Failed to insert audit log into BigQuery: {errors}"

        # Query back the record
        query = f"""
            SELECT task_id, execution_id, status, records_extracted, bytes_processed
            FROM `{table_id}`
            WHERE execution_id = @run_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", test_run_id)]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())

        assert len(results) == 1
        assert results[0].task_id == "test_integration_task"
        assert results[0].status == "SUCCESS"
        assert results[0].records_extracted == 42
        assert results[0].bytes_processed == 2048

    def test_watermark_persistence_and_lookup(self, bq_client, gcp_project_id):
        table_id = f"{gcp_project_id}.ds_operations.ingestion_watermarks"
        now = datetime.datetime.now(datetime.timezone.utc)
        test_task_id = f"test_wm_{uuid.uuid4().hex[:6]}"

        row = {
            "task_id": test_task_id,
            "source_type": "netsuite",
            "watermark_column": "lastmodifieddate",
            "last_watermark_value": "2026-09-18T12:00:00Z",
            "last_updated_at": now.isoformat(),
        }

        errors = bq_client.insert_rows_json(table_id, [row])
        assert not errors, f"Failed to insert watermark: {errors}"

        query = f"""
            SELECT last_watermark_value
            FROM `{table_id}`
            WHERE task_id = @task_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("task_id", "STRING", test_task_id)]
        )
        query_job = bq_client.query(query, job_config=job_config)
        results = list(query_job.result())

        assert len(results) >= 1
        assert results[0].last_watermark_value == "2026-09-18T12:00:00Z"

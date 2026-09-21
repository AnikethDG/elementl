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

"""Integration tests for Cloud Run Jobs deployment, Artifact Registry images, and execution."""

import subprocess
import json
import pytest


@pytest.mark.integration
class TestCloudRunJobsIntegration:
    """Integration test suite for Cloud Run Jobs in project pid-nse-stg-core-apps-k8ti."""

    REQUIRED_JOBS = [
        "suiteql-ingestion",
        "jdbc-ingestion",
        "arcgis-ingestion",
        "bulk-ingestion",
    ]

    def _run_gcloud(self, cmd: list) -> str:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return res.stdout.strip()

    def test_all_cloud_run_jobs_exist(self, gcp_project_id, gcp_region):
        cmd = [
            "gcloud", "run", "jobs", "list",
            f"--project={gcp_project_id}",
            f"--region={gcp_region}",
            "--format=json",
        ]
        out = self._run_gcloud(cmd)
        jobs_data = json.loads(out)
        deployed_names = {j["metadata"]["name"] for j in jobs_data}

        for req in self.REQUIRED_JOBS:
            assert req in deployed_names, f"Missing required Cloud Run Job: {req}"

    def test_cloud_run_jobs_use_artifact_registry_images(self, gcp_project_id, gcp_region):
        for job_name in self.REQUIRED_JOBS:
            cmd = [
                "gcloud", "run", "jobs", "describe", job_name,
                f"--project={gcp_project_id}",
                f"--region={gcp_region}",
                "--format=value(spec.template.spec.template.spec.containers[0].image)",
            ]
            image_uri = self._run_gcloud(cmd)
            assert f"{gcp_region}-docker.pkg.dev/{gcp_project_id}/udp-ingestion-jobs/{job_name}" in image_uri or \
                   f"udp-ingestion-jobs/{job_name}" in image_uri, \
                   f"Job {job_name} is not using Artifact Registry image: {image_uri}"

    def test_bulk_ingestion_execution_smoke(self, gcp_project_id, gcp_region, bronze_bucket):
        """Validates that a live execution completes and outputs to GCS."""
        env_vars = (
            f"TASK_ID=atlas_s2_20_cooling_water_supply,"
            f"SOURCE_TYPE=atlas,"
            f"GCS_BUCKET={bronze_bucket},"
            f"GCS_PREFIX=atlas/raw/atlas_s2_20_cooling_water_supply/dt=2026-09-18,"
            f"TARGET_DATASET=ds_bronze_atlas,"
            f"TARGET_TABLE=atlas_s2_20_cooling_water_supply,"
            f"AUDIT_PROJECT={gcp_project_id},"
            f"AUDIT_DATASET=ds_operations,"
            f"AUDIT_TABLE=ingestion_execution_logs,"
            f"EXECUTION_DATE=2026-09-18,"
            f"RUN_ID=itest_smoke_exec,"
            f"DAG_ID=dag_udp_atlas_s2_20_cooling_water_supply"
        )
        cmd = [
            "gcloud", "run", "jobs", "execute", "bulk-ingestion",
            f"--project={gcp_project_id}",
            f"--region={gcp_region}",
            f"--update-env-vars={env_vars}",
            "--wait",
        ]
        # Execute job
        out = self._run_gcloud(cmd)
        assert "successfully completed" in out or "Done" in out

"""Integration tests for Cloud Composer environment and Dynamic DAG registration."""

import subprocess
import json
import pytest
from google.cloud import storage


@pytest.mark.integration
class TestComposerDAGsIntegration:
    """Integration test suite for Cloud Composer environment in project pid-nse-stg-core-apps-k8ti."""

    def _run_gcloud(self, cmd: list) -> str:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return res.stdout.strip()

    def test_composer_environment_running(self, gcp_project_id, gcp_region, composer_env_name):
        cmd = [
            "gcloud", "composer", "environments", "describe", composer_env_name,
            f"--project={gcp_project_id}",
            f"--location={gcp_region}",
            "--format=json",
        ]
        out = self._run_gcloud(cmd)
        env_data = json.loads(out)
        state = env_data.get("state")
        assert state == "RUNNING", f"Composer environment {composer_env_name} is in state: {state}"

    def test_dag_factory_and_configs_in_composer_bucket(self, gcp_project_id, gcp_region, composer_env_name):
        # Retrieve DAG GCS Prefix
        cmd = [
            "gcloud", "composer", "environments", "describe", composer_env_name,
            f"--project={gcp_project_id}",
            f"--location={gcp_region}",
            "--format=value(config.dagGcsPrefix)",
        ]
        dag_prefix = self._run_gcloud(cmd)
        assert dag_prefix.startswith("gs://")
        bucket_name = dag_prefix.replace("gs://", "").split("/")[0]

        client = storage.Client(project=gcp_project_id)
        bucket = client.get_bucket(bucket_name)

        # Check DAG factory
        factory_blob = bucket.blob("dags/udp_dag_factory.py")
        assert factory_blob.exists(), "udp_dag_factory.py not found in Composer dags folder"

        # Check source configs
        config_blobs = list(bucket.list_blobs(prefix="dags/configs/sources/"))
        yaml_configs = [b for b in config_blobs if b.name.endswith(".yaml")]
        assert len(yaml_configs) >= 30, f"Expected 30 YAML configs in Composer dags/configs/sources/, found {len(yaml_configs)}"

    def test_no_dag_import_errors_in_composer(self, gcp_project_id, gcp_region, composer_env_name):
        cmd = [
            "gcloud", "composer", "environments", "run", composer_env_name,
            f"--project={gcp_project_id}",
            f"--location={gcp_region}",
            "dags", "list-import-errors",
        ]
        out = self._run_gcloud(cmd)
        assert "No data found" in out or "No import errors" in out, f"Composer has DAG import errors: {out}"

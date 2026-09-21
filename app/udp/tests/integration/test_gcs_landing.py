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

"""Integration tests for Google Cloud Storage landing lake and configuration buckets."""

import time
import pytest
from google.cloud import storage


@pytest.mark.integration
class TestGCSLandingIntegration:
    """Integration test suite for GCS buckets in project pid-nse-stg-core-apps-k8ti."""

    @pytest.fixture(scope="class")
    def storage_client(self, gcp_project_id):
        return storage.Client(project=gcp_project_id)

    def test_bronze_landing_bucket_exists(self, storage_client, bronze_bucket):
        bucket = storage_client.get_bucket(bronze_bucket)
        assert bucket.exists()
        assert bucket.location.lower() in {"us-central1", "us"}

    def test_config_bucket_contains_sources(self, storage_client, config_bucket):
        bucket = storage_client.get_bucket(config_bucket)
        blobs = list(bucket.list_blobs(prefix="sources/"))
        yaml_blobs = [b for b in blobs if b.name.endswith(".yaml") or b.name.endswith(".yml")]

        assert len(yaml_blobs) >= 30, f"Expected at least 30 YAML configs in {config_bucket}/sources/, got {len(yaml_blobs)}"

        sources_found = {b.name.split("/")[1] for b in yaml_blobs if len(b.name.split("/")) > 2}
        assert "netsuite" in sources_found
        assert "p6" in sources_found
        assert "atlas" in sources_found

    def test_write_and_read_lifecycle_in_bronze_bucket(self, storage_client, bronze_bucket):
        bucket = storage_client.get_bucket(bronze_bucket)
        test_blob_name = f"_integration_tests/test_write_{int(time.time())}.txt"
        blob = bucket.blob(test_blob_name)

        payload = "Elementl UDP GCS Integration Test Probe"
        blob.upload_from_string(payload, content_type="text/plain")

        try:
            assert blob.exists()
            content = blob.download_as_text()
            assert content == payload
        finally:
            blob.delete()
            assert not blob.exists()

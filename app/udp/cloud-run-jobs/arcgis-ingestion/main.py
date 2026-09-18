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

"""Containerized Cloud Run Ingestion Job: arcgis-ingestion."""

import json
import logging
import os
import sys
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("arcgis-ingestion")


def main() -> int:
    project_id = os.environ.get("GCP_PROJECT_ID", "pid-nse-stg-core-apps-k8ti")
    raw_bucket = os.environ.get("RAW_BUCKET_NAME", f"bkt-{project_id}-udp-bronze-raw")
    task_id = os.environ.get("TASK_ID", "smoke_test")
    source_table = os.environ.get("SOURCE_TABLE", "")
    logger.info(
        "Starting arcgis-ingestion worker | project=%s | bucket=%s | task_id=%s | source_table=%s",
        project_id,
        raw_bucket,
        task_id,
        source_table,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

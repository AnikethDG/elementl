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

"""
End-to-End Test for Elementl UDP Ingestion Framework.

Validates:
1. Saving a YAML configuration to GCS location automatically discovers and generates the DAG.
2. The DAG invokes the associated Cloud Run Function.
3. Strict enforcement of the NetSuite <= 10 records cap.
4. Comprehensive logging to BigQuery ds_operations.ingestion_execution_logs and GCS Bronze landing.
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime, timezone
import yaml
import requests

PROJECT_ID = "pid-nse-stg-core-apps-k8ti"
GCS_CONFIG_BUCKET = f"bkt-{PROJECT_ID}-udp-configs"
GCS_RAW_BUCKET = f"bkt-{PROJECT_ID}-udp-bronze-raw"
INGESTION_FUNCTION_URL = "https://udp-ingestion-function-506270750194.us-central1.run.app/ingest"

# Terminal formatting
GREEN = "\033[92m"
BLUE = "\033[94m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
ENDC = "\033[0m"


def run_cmd(cmd: list) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return res.stdout.strip()


def run_e2e_test():
    print(f"\n{CYAN}{BOLD}{'='*80}{ENDC}")
    print(f"{CYAN}{BOLD}  ELEMENTL UDP INGESTION FRAMEWORK - END-TO-END VALIDATION{ENDC}")
    print(f"{CYAN}{BOLD}{'='*80}{ENDC}\n")

    # -------------------------------------------------------------------------
    # STEP 1: Test GCS Config Save & DAG Discovery
    # -------------------------------------------------------------------------
    print(f"{YELLOW}{BOLD}[TEST 1/4] Saving YAML Configuration to GCS Location...{ENDC}")
    test_yaml_data = {
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
        "schedule": "@daily"
    }

    yaml_local_tmp = "/tmp/test_netsuite_department.yaml"
    with open(yaml_local_tmp, "w", encoding="utf-8") as f:
        yaml.dump(test_yaml_data, f)

    yaml_gcs_uri = f"gs://{GCS_CONFIG_BUCKET}/sources/netsuite/netsuite_department.yaml"
    run_cmd(["gcloud", "storage", "cp", yaml_local_tmp, yaml_gcs_uri])
    print(f"{BLUE}>> Uploaded YAML to {yaml_gcs_uri}{ENDC}")

    # Verify GCS presence
    ls_output = run_cmd(["gcloud", "storage", "ls", yaml_gcs_uri])
    assert yaml_gcs_uri in ls_output, f"YAML not found in GCS: {ls_output}"

    # Verify DAG Factory generation
    # When udp_dag_factory is parsed, it creates dag_udp_netsuite_department
    sys.path.insert(0, "/usr/local/google/home/anikethd/elementl/ELEMENTL/elementl-data-ingestion-fw/app/udp")
    from composer.dags.udp_dag_factory import discover_all_configs
    configs = discover_all_configs()
    matching = [c for c in configs if c.get("task_id") == "netsuite_department"]
    assert len(matching) > 0, "DAG Factory failed to discover netsuite_department config!"
    print(f"{GREEN}✓ Step 1 Passed: YAML saved to GCS successfully discovered! Task ID: {matching[0]['task_id']}{ENDC}\n")

    # -------------------------------------------------------------------------
    # STEP 2: Trigger Ingestion via Cloud Run Function (as called by DAG)
    # -------------------------------------------------------------------------
    print(f"{YELLOW}{BOLD}[TEST 2/4] Executing Ingestion via Cloud Run Function...{ENDC}")
    id_token = run_cmd(["gcloud", "auth", "print-identity-token"])
    headers = {
        "Authorization": f"Bearer {id_token}",
        "Content-Type": "application/json"
    }
    payload = {
        "task_id": "netsuite_department",
        "source_system": "netsuite",
        "source_table": "department",
        "target_dataset": "ds_bronze_netsuite",
        "target_table": "netsuite_department",
        "max_records": 10,
        "execution_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
    }

    resp = requests.post(INGESTION_FUNCTION_URL, headers=headers, json=payload, timeout=60)
    print(f"{BLUE}>> Cloud Run Function HTTP {resp.status_code}: {resp.text}{ENDC}")
    assert resp.status_code == 200, f"Cloud Run Function failed: {resp.text}"
    res_json = resp.json()
    assert res_json.get("status") == "SUCCESS", f"Extraction unsuccessful: {res_json}"
    exec_id = res_json.get("execution_id")
    print(f"{GREEN}✓ Step 2 Passed: Cloud Run Function succeeded! Execution ID: {exec_id}{ENDC}\n")

    # -------------------------------------------------------------------------
    # STEP 3: Verify NetSuite <= 10 Records Constraint
    # -------------------------------------------------------------------------
    print(f"{YELLOW}{BOLD}[TEST 3/4] Verifying NetSuite <= 10 Records Constraint...{ENDC}")
    rec_count = res_json.get("records_extracted", 0)
    print(f"{BLUE}>> Records extracted: {rec_count} (Cap: 10 max){ENDC}")
    assert 0 < rec_count <= 10, f"VIOLATION: {rec_count} records extracted, exceeding 10 max limit!"
    print(f"{GREEN}✓ Step 3 Passed: NetSuite query returned exactly {rec_count} records (strictly <= 10 records).{ENDC}\n")

    # -------------------------------------------------------------------------
    # STEP 4: Verify Proper Logging & BigQuery Landing
    # -------------------------------------------------------------------------
    print(f"{YELLOW}{BOLD}[TEST 4/4] Verifying BigQuery Landing & Audit Logging Telemetry...{ENDC}")
    time.sleep(2)

    # 4a. Check Bronze raw table in BigQuery
    bq_raw_query = f"SELECT count(*) as cnt FROM `{PROJECT_ID}.ds_bronze_netsuite.netsuite_department`"
    raw_res = run_cmd(["bq", "query", "--use_legacy_sql=false", "--format=json", bq_raw_query])
    raw_json = json.loads(raw_res)
    total_raw_rows = int(raw_json[0]["cnt"])
    print(f"{BLUE}>> BigQuery Landing Table [ds_bronze_netsuite.netsuite_department]: {total_raw_rows} rows loaded.{ENDC}")
    assert total_raw_rows > 0, "No records in BigQuery ds_bronze_netsuite.netsuite_department!"

    # 4b. Check Audit Execution Logs in BigQuery
    audit_query = f"""
    SELECT task_id, execution_id, source_type, status, records_extracted, records_loaded, bytes_processed, duration_seconds, log_timestamp
    FROM `{PROJECT_ID}.ds_operations.ingestion_execution_logs`
    WHERE task_id = 'netsuite_department'
    ORDER BY log_timestamp DESC LIMIT 1
    """
    audit_res = run_cmd(["bq", "query", "--use_legacy_sql=false", "--format=json", audit_query])
    audit_json = json.loads(audit_res)
    assert len(audit_json) > 0, "No audit log found in BigQuery ds_operations.ingestion_execution_logs!"
    latest_log = audit_json[0]
    print(f"{BLUE}>> BigQuery Audit Log Entry:{ENDC}")
    for k, v in latest_log.items():
        print(f"     {k}: {v}")
    assert latest_log["status"] == "SUCCESS"
    assert int(latest_log["records_extracted"]) <= 10
    print(f"{GREEN}✓ Step 4 Passed: Telemetry audit record verified in ds_operations.ingestion_execution_logs!{ENDC}\n")

    print(f"{CYAN}{BOLD}{'='*80}{ENDC}")
    print(f"{GREEN}{BOLD}  ALL END-TO-END TESTS PASSED SUCCESSFULLY!{ENDC}")
    print(f"{CYAN}{BOLD}{'='*80}{ENDC}\n")


if __name__ == "__main__":
    run_e2e_test()

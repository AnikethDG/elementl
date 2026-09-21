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

# Consolidated Project BigQuery Datasets (Bronze, Silver, Gold, Assertions, Metadata)
locals {
  udp_datasets = {
    # Standardized Naming Convention Datasets (Elementl UDP Naming Convention Doc + Per-Source Silver Split)
    ds_bronze_p6           = "Bronze raw landing dataset for Oracle Primavera P6 (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
    ds_bronze_netsuite     = "Bronze raw landing dataset for Oracle NetSuite (SuiteQL 11 tables)"
    ds_bronze_atlas        = "Bronze raw landing dataset for Atlas GIS & Tabular (envelope schema + layers)"
    ds_silver_p6           = "Silver 1:1 cleansed & standardized models for Oracle Primavera P6"
    ds_silver_netsuite     = "Silver 1:1 cleansed & standardized models for Oracle NetSuite"
    ds_silver_atlas        = "Silver 1:1 cleansed & standardized models for Atlas GIS & Tabular"
    ds_gold                = "Gold conformed & curated business data models and executive views"
    ds_dataform_assertions = "Dataform data quality assertions failed records (Bronze-to-Silver validation)"
    ds_operations          = "Operational metadata table driving DAG Factory, watermarks, and framework audit logs"
    ds_atlas_analytics     = "Exposed analytics views, site suitability parameters, and AI search tables for Project Atlas"

    # Legacy / Retained Datasets (preserved in state to avoid destructive replacement)
    raw_p6              = "Bronze raw landing dataset for Oracle Primavera P6 (ELEMENTL_PMDB_SBOX_PXRPTUSER)"
    raw_netsuite        = "Bronze raw landing dataset for Oracle NetSuite (SuiteQL 11 tables)"
    raw_atlas           = "Bronze raw landing dataset for Atlas GIS & Tabular (envelope schema + layers)"
    silver_p6           = "Silver 1:1 conformed models for Oracle Primavera P6"
    silver_netsuite     = "Silver 1:1 conformed models for Oracle NetSuite"
    silver_atlas        = "Silver 1:1 conformed models for Atlas GIS & Tabular"
    gold_analytics      = "Gold cross-domain analytical marts and executive views"
    dataform_assertions = "Dataform data quality assertions (Bronze-to-Silver validation)"
    udp_metadata        = "UDP pipeline watermarks, audit logs, and reconciliation telemetry"
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each                   = local.udp_datasets
  project                    = var.project_id
  dataset_id                 = each.key
  location                   = var.region
  description                = each.value
  delete_contents_on_destroy = false
}

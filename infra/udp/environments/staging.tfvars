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

project_id                = "pid-nse-stg-core-apps-k8ti"
region                    = "us-central1"
environment               = "staging"
vpc_network               = "projects/pid-ns-npd-us-netw-ucyd/global/networks/vpc-ns-stg-us"
vpc_subnet                = "projects/pid-ns-npd-us-netw-ucyd/regions/us-central1/subnetworks/sub-ns-stg-usc1"
artifact_registry_repo_id = "udp-ingestion-jobs"
image_tag                 = "latest"
enable_composer           = true
composer_image_version    = "composer-3-airflow-3"

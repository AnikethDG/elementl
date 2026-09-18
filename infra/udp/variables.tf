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

variable "project_id" {
  description = "Consolidated GCP Project ID (e.g. pid-nse-stg-core-apps-k8ti)"
  type        = string
  default     = "pid-nse-stg-core-apps-k8ti"
}

variable "region" {
  description = "GCP region for all UDP resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment name (development, staging, production)"
  type        = string
  default     = "staging"
}

variable "vpc_network" {
  description = "Shared VPC network name or self_link"
  type        = string
  default     = "projects/pid-ns-npd-us-netw-ucyd/global/networks/vpc-ns-stg-us"
}

variable "vpc_subnet" {
  description = "Shared VPC subnetwork self_link (egresses via static NAT IP 136.115.148.145)"
  type        = string
  default     = "projects/pid-ns-npd-us-netw-ucyd/regions/us-central1/subnetworks/sub-ns-stg-usc1"
}

variable "enable_composer" {
  description = "Whether to provision the Cloud Composer 3 environment via Terraform"
  type        = bool
  default     = false
}

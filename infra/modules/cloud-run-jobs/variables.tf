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
  description = "The GCP Project ID hosting the Cloud Run Job"
  type        = string
}

variable "region" {
  description = "GCP region for the Cloud Run Job"
  type        = string
  default     = "us-central1"
}

variable "job_name" {
  description = "Name of the Cloud Run Job (e.g. jdbc-ingestion)"
  type        = string
}

variable "description" {
  description = "Human-readable description of the Cloud Run Job"
  type        = string
  default     = "UDP containerized ingestion job"
}

variable "image" {
  description = "Container image URI (managed via Cloud Build after initial creation)"
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/job:latest"
}

variable "cpu" {
  description = "CPU allocation per task"
  type        = string
  default     = "2"
}

variable "memory" {
  description = "Memory allocation per task"
  type        = string
  default     = "4Gi"
}

variable "max_retries" {
  description = "Maximum task retry attempts"
  type        = number
  default     = 2
}

variable "timeout" {
  description = "Task execution timeout (e.g. 3600s)"
  type        = string
  default     = "3600s"
}

variable "env_vars" {
  description = "Environment variables passed to the container"
  type        = map(string)
  default     = {}
}

variable "vpc_network" {
  description = "Shared VPC network self_link or name for Direct VPC egress"
  type        = string
  default     = ""
}

variable "vpc_subnet" {
  description = "Shared VPC subnet self_link or name for Direct VPC egress (routes via static NAT 136.115.148.145)"
  type        = string
  default     = ""
}

variable "vpc_egress" {
  description = "VPC egress mode (ALL_TRAFFIC or PRIVATE_RANGES_ONLY)"
  type        = string
  default     = "ALL_TRAFFIC"
}

variable "raw_bucket_name" {
  description = "Optional GCS Bronze raw landing bucket to grant storage.objectAdmin on"
  type        = string
  default     = ""
}

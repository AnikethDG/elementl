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

# NOTE ON TERRAFORM STATE SEPARATION:
# Root `infra/` manages the core application Artifact Registry (`app-repo` in `infra/main.tf`)
# under state prefix `terraform/infra/state`.
# The UDP Docker Artifact Registry (`udp-ingestion-jobs`) and its Cloud Run Job IAM bindings
# are managed exclusively in `infra/udp/artifact_registry.tf` under the isolated UDP state
# prefix `terraform/udp/state` to prevent cross-state 409 resource collisions.

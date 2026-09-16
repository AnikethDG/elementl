# Cloud Run Monorepo Template (`terraform-app-infra-template`)

A production-ready Google Cloud Run monorepo template combining Infrastructure-as-Code (Terraform) and microservices (`apps/`) with independent CI/CD pipelines via Google Cloud Build.

---

## 1. Architecture Overview

```text
+---------------------------------------------------------------------------------------------------+
|                                          GIT MONOREPO                                             |
|                                                                                                   |
|   +---------------------------------------+       +-------------------------------------------+   |
|   |                infra/                 |       |             apps/service-api/             |   |
|   |  - Terraform configs (main.tf, etc.)  |       |  - Application code (main.py, etc.)       |   |
|   |  - Pipeline (cloudbuild.yaml)         |       |  - Pipeline (cloudbuild.yaml)             |   |
|   +---------------------------------------+       +-------------------------------------------+   |
+-----------------------|---------------------------------------------------|-----------------------+
                        | (Push: infra/**)                                  | (Push: apps/service-api/**)
                        v                                                   v
+---------------------------------------------------------------------------------------------------+
|                                CLOUD BUILD TRIGGERS (PATH FILTERS)                                |
|                                                                                                   |
|   +---------------------------------------+       +-------------------------------------------+   |
|   |         Trigger: deploy-infra         |       |      Trigger: deploy-service-api          |   |
|   |       includedFiles: ["infra/**"]     |       |   includedFiles: ["apps/service-api/**"]  |   |
|   +---------------------------------------+       +-------------------------------------------+   |
+-----------------------|---------------------------------------------------|-----------------------+
                        |                                                   |
      [terraform apply] |                                                   | [docker build & push]
                        |                                                   v
                        |                                   +-------------------------------+
                        |                                   |       ARTIFACT REGISTRY       |
                        |                                   |  (Docker Container Images)    |
                        |                                   +---------------+---------------+
                        |                                                   |
                        |                                                   | [gcloud run deploy]
                        |                                                   | (image: service-api:$COMMIT_SHA)
                        v                                                   v
+---------------------------------------------------------------------------------------------------+
|                                      GOOGLE CLOUD PLATFORM                                        |
|                                                                                                   |
|   +---------------------------------------+       +-------------------------------------------+   |
|   |        Core / Shared Resources        |       |            Cloud Run Service              |   |
|   |                                       |       |             (service-api)                 |   |
|   |  - Artifact Registry repo             |       |                                           |   |
|   |  - Runtime Service Account (least-priv|       |  - Initial shell created by Terraform     |   |
|   |  - Cloud Run service shell            |------>|  - Revisions deployed by Cloud Build      |   |
|   |                                       |       |  - lifecycle { ignore_changes = [image] } |   |
|   +---------------------------------------+       +-------------------------------------------+   |
+---------------------------------------------------------------------------------------------------+
```

---

## 2. Directory Layout

```
terraform-app-infra-template/
├── .gitignore
├── README.md
├── infra/                                # Infrastructure as Code (Terraform)
│   ├── backend.tf                        # GCS remote state backend configuration
│   ├── main.tf                           # Core infra, Artifact Registry, Service Accounts, Cloud Run shell
│   ├── variables.tf                      # Parameterized variables (including environment)
│   ├── outputs.tf                        # Exposed outputs (URLs, IDs, SAs)
│   ├── versions.tf                       # Terraform & Google provider constraints
│   ├── terraform.tfvars.example          # Sample configuration variables
│   ├── environments/                     # Environment-specific variable files
│   │   ├── dev.tfvars                    # Dev environment variables
│   │   ├── staging.tfvars                # Staging environment variables
│   │   └── prod.tfvars                   # Production environment variables
│   └── cloudbuild.yaml                   # CI/CD pipeline to test & apply Terraform
└── apps/                                 # Microservices directory
    ├── README.md                         # Guide for managing and adding services
    └── service-api/                      # Sample microservice (FastAPI / Python)
        ├── main.py                       # App entrypoint (health checks, security headers)
        ├── requirements.txt              # App dependencies
        ├── test_main.py                  # Unit tests
        ├── Dockerfile                    # Multi-stage/lean non-root container
        ├── .dockerignore                 # Docker build exclusions
        └── cloudbuild.yaml               # Dedicated CI/CD pipeline for service-api
```

---

## 3. Key Concepts & Patterns

### A. The Deployment Handoff Pattern (Crucial)

When combining Terraform and Cloud Run, you face the **"State Conflict"** problem:
1. Terraform creates the Cloud Run service and stores the initial image tag (e.g., `us-docker.pkg.dev/cloudrun/container/hello`) in `terraform.tfstate`.
2. When a developer pushes app code, Cloud Build builds a new image tag (`service-api:$COMMIT_SHA`) and deploys a new Cloud Run revision.
3. If Terraform does not account for this, the next `terraform apply` will detect drift and **revert Cloud Run back to the initial placeholder image**.

#### The Solution: `lifecycle { ignore_changes = [...] }`
In `infra/main.tf`, Terraform provisions the Cloud Run service shell and delegates ongoing image and revision management to Cloud Build:

```hcl
resource "google_cloud_run_v2_service" "services" {
  # ... configuration ...

  lifecycle {
    # CRITICAL: Prevents Terraform from overriding subsequent Cloud Build deployments
    ignore_changes = [
      template[0].containers[0].image,
      template[0].revision,
      client,
      client_version
    ]
  }
}
```

### B. CI/CD Trigger Strategy (Trigger Path Filters)

To ensure fast builds and prevent unnecessary deployments in a monorepo, Cloud Build Triggers use the `includedFiles` filter:

| Trigger Name | Event | Included Files Filter | Configuration File | Target Env | Approval Required |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `deploy-infra-dev` | Push to `^main$` | `infra/**` | `infra/cloudbuild.yaml` | `dev` | No |
| `deploy-service-api-dev` | Push to `^main$` | `apps/service-api/**` | `apps/service-api/cloudbuild.yaml` | `dev` | No |
| `deploy-service-api-staging` | Push tag `^v.*` | N/A (Tag push) | `apps/service-api/cloudbuild.yaml` | `staging` | No |
| `deploy-service-api-prod` | Push tag `^v.*` | N/A (Tag push) | `apps/service-api/cloudbuild.yaml` | `prod` | **Yes (Manual)** |

- **Blast Radius Reduction**: Changes in `service-api` do not trigger Terraform or other services.
- **Fast Execution**: Only modified components are built and tested.
- **Scoping via `dir:`**: Each step in Cloud Build uses `dir: 'apps/<service>'` or `dir: 'infra'` to execute within its respective directory context.

### C. Branching & Promotion Strategy: Deep Dive

A robust branching and deployment strategy balances **developer velocity**, **production stability**, and **security isolation**. Below is a detailed breakdown of the **Primary Trunk-Based Strategy** (recommended for startups and modern cloud-native teams) and the **Fallback Environment-Branching Model**.

---

#### 1. Primary Strategy: Trunk-Based Development (Recommended)

In Trunk-Based Development, developers collaborate on a single main branch (`main`) with short-lived feature branches (`feat/*`, `fix/*`). Environments are decoupled from Git branches and instead map to **commits** and **Git Tags**.

```text
+---------------------------------------------------------------------------------------------------+
|                         PRIMARY: TRUNK-BASED PROMOTION WORKFLOW                                   |
|                                                                                                   |
|   [feature/login] --------(PR to main)------> [main] -------(Git Tag: v1.0.0)-----> [PROMOTION]   |
|         |                                        |                                        |       |
|         v                                        v                                        v       |
|   - Local dev / tests                      - Automated Deploy                       +-----------+ |
|   - CI: pytest & tf plan                   - DEV GCP Project                        | Staging & | |
|                                            - Smoke tests & QA                       | Prod Sync | |
|                                                                                     +-----+-----+ |
|                                                                                           |       |
|                                              +--------------------------------------------+       |
|                                              |                                                    |
|                                              v                                                    v
|                                   +---------------------+                      +----------------------+
|                                   | Automated Deploy    |                      | Trigger for PROD     |
|                                   | STAGING GCP Project |                      | (REQUIRES APPROVAL)  |
|                                   +---------------------+                      +----------+-----------+
|                                                                                           |
|                                                                                           | (Manual Click)
|                                                                                           v
|                                                                                +----------------------+
|                                                                                | Deploy to PRODUCTION |
|                                                                                | PROD GCP Project     |
|                                                                                +----------------------+
+---------------------------------------------------------------------------------------------------+
```

##### Why This Works Best for Startups
1. **Zero Branch Drift**: No divergent `develop` or `release` branches. What is tested on `main` is what goes to production.
2. **Build Once, Promote Everywhere**:
   - The container image is built and tagged once with the immutable `$COMMIT_SHA`.
   - The **identical image binary** that ran in Dev is promoted to Staging and Prod—eliminating "works in staging, fails in prod" caused by container rebuilds.
3. **3-Project Multi-Environment Isolation**:
   - **Dev Project** (`p-...-dev`): Fast deploys, debug logging, minimal instance limits.
   - **Staging Project** (`p-...-staging`): Production replica for QA and integration testing.
   - **Prod Project** (`p-...-prod`): High availability, min instances = 1 (zero cold starts), strict IAM access.
4. **Human-in-the-Loop Safety Gate**:
   - Pushing tag `v1.0.0` immediately deploys to **Staging**.
   - The same tag fires the **Production** trigger in Cloud Build, but pauses in `PENDING_APPROVAL` status until an authorized engineer clicks **Approve** in the GCP Console.

##### Rollback Procedure in Trunk-Based Model
- **Instant Cloud Run Traffic Rollback (Zero Downtime)**:
  Cloud Run keeps previous revisions available. If a bug is detected in production, roll traffic back immediately via `gcloud`:
  ```bash
  gcloud run services update-traffic service-api \
      --to-revisions=service-api-PREVIOUS-REVISION=100 \
      --project=YOUR_PROD_PROJECT_ID \
      --region=us-central1
  ```
- **Git Tag Rollback**:
  Re-deploy any known healthy tag (e.g. `v0.9.8`) to redeploy the previous container image across environments.

---

#### 2. Fallback Strategy: Environment-Branching (`dev` -> `staging` -> `main`/`prod`)

While Trunk-Based Development is recommended, some organizations prefer or require an **Environment-Branching Model** (GitOps-style branch segregation).

```text
+---------------------------------------------------------------------------------------------------+
|                        FALLBACK: ENVIRONMENT-BRANCHING WORKFLOW                                   |
|                                                                                                   |
|   [feature/xyz] ---(PR)---> [dev branch] --------(PR)-------> [staging branch] --------(PR)----->|
|                                  |                                  |                             |
|                                  v                                  v                             |
|                           [Deploy to DEV]                   [Deploy to STAGING]                   |
|                           (Dev GCP Project)                 (Staging GCP Project)                 |
|                                                                                                   |
|   ---------------------------------------------------------------------------------------------   |
|   ... Continued:                                                                                  |
|   ----(PR)----> [main / prod branch]                                                              |
|                        |                                                                          |
|                        v                                                                          |
|                 [Deploy to PROD]                                                                  |
|                 (Prod GCP Project)                                                                |
+---------------------------------------------------------------------------------------------------+
```

##### When to Use the Fallback Model
- **Strict Compliance & Audit**: Regulated industries (FinTech, HealthTech) requiring audit logs of PR approvals specifically between environment branches.
- **Branch-Level Access Restrictions**: When GitHub branch protection rules are used to restrict who can merge into `staging` or `main`.

##### Known Pitfalls & Trade-Offs of Environment-Branching
- **Branch Drift**: If a critical hotfix is merged into `main`, it must be manually cherry-picked or back-merged into `staging` and `dev`. If missed, branches diverge quickly ("merge hell").
- **Image Rebuild Risk**: If each branch triggers `docker build`, the container image in Production might contain different package versions or compiler artifacts than what was verified in Staging.
- **Slower Velocity**: Every release requires opening, reviewing, and merging 3 separate Pull Requests.

##### How to Adopt the Fallback Model in this Repo
If you choose to switch to Environment-Branching:
1. Create persistent branches: `dev`, `staging`, and `main` (or `prod`).
2. Update Cloud Build Triggers:
   - `deploy-service-api-dev`: Trigger on Push to branch `^dev$`.
   - `deploy-service-api-staging`: Trigger on Push to branch `^staging$`.
   - `deploy-service-api-prod`: Trigger on Push to branch `^main$` (or `^prod$`).
3. Set `_TARGET_PROJECT_ID` and `_ENVIRONMENT` accordingly in each trigger.

---

#### 3. Comparative Summary: Trunk-Based vs Environment-Branching

| Dimension | Trunk-Based Development (Primary) | Environment-Branching (Fallback) |
| :--- | :--- | :--- |
| **Release Velocity** | **Very High**: Merge to `main` deploys to Dev; Git tag promotes. | **Moderate / Low**: Requires 3 sequential PRs per release. |
| **Branch Maintenance** | **Minimal**: Only short-lived feature branches + `main`. | **High**: Long-lived `dev`, `staging`, and `main` branches. |
| **Merge Conflicts** | **Rare**: Frequent small merges to trunk. | **Frequent**: Hotfixes & back-merges create conflict risk. |
| **Container Immutability** | **Guaranteed**: Same `$COMMIT_SHA` promoted to all envs. | **Risk**: Typically rebuilds on each branch merge. |
| **Production Gate** | Cloud Build **Approval Gate** triggered on Git tag. | GitHub PR merge approval into `main` branch. |
| **Rollback Speed** | Instant Cloud Run traffic switch or tag redeploy. | Revert PR, merge, and wait for full pipeline. |
| **Best For** | Startups, SaaS, high-growth engineering teams. | Regulated enterprise with strict compliance mandates. |



---

## 4. Getting Started

### Step 1: Prerequisites & IAM Setup

Ensure you have the Google Cloud CLI (`gcloud`) installed and authenticated:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

#### Cloud Build Service Account Permissions

##### Option A: Using a Custom Service Account (e.g. `tf-deploy@...`) - Recommended
When configuring a trigger with a user-specified custom service account, GCP requires:
1. **`roles/logging.logWriter`** on the project (so Cloud Build can write build logs).
2. **`roles/iam.serviceAccountUser`** granted to the Cloud Build Service Agent (`service-<PROJECT_NUMBER>@gcp-sa-cloudbuild.iam.gserviceaccount.com`) on the custom service account.
3. Relevant deployment roles on the project (e.g., `roles/run.admin`, `roles/artifactregistry.writer`, `roles/iam.serviceAccountUser`, and for Terraform: resource management permissions).

Run these commands to configure the custom service account:

```bash
PROJECT_ID="YOUR_PROJECT_ID"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
CUSTOM_SA="tf-deploy@${PROJECT_ID}.iam.gserviceaccount.com"
CB_SERVICE_AGENT="service-${PROJECT_NUMBER}@gcp-sa-cloudbuild.iam.gserviceaccount.com"

# 1. Project-level permissions for Cloud Build logging
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CUSTOM_SA" \
    --role="roles/logging.logWriter"

# 2. Allow Cloud Build Service Agent to impersonate the custom service account
gcloud iam service-accounts add-iam-policy-binding $CUSTOM_SA \
    --member="serviceAccount:$CB_SERVICE_AGENT" \
    --role="roles/iam.serviceAccountUser" \
    --project=$PROJECT_ID

# 3. Cloud Run & Artifact Registry deployment permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CUSTOM_SA" \
    --role="roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CUSTOM_SA" \
    --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CUSTOM_SA" \
    --role="roles/artifactregistry.writer"
```

##### Option B: Using the Default Cloud Build Service Account
If using the default Cloud Build Service Account (`<PROJECT_NUMBER>@cloudbuild.gserviceaccount.com`):

```bash
PROJECT_ID="YOUR_PROJECT_ID"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
CB_SA="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"

# Cloud Run Admin, Service Account User, and Artifact Registry Writer
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CB_SA" \
    --role="roles/run.admin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CB_SA" \
    --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CB_SA" \
    --role="roles/artifactregistry.writer"
```

---

### Step 2: Bootstrap Infrastructure with Terraform

1. Navigate to the `infra/` directory:
   ```bash
   cd infra
   ```

2. Configure your remote backend in `infra/backend.tf`:
   Ensure `bucket` matches your project's state bucket (created with project creation):
   ```hcl
   terraform {
     backend "gcs" {
       bucket = "p-nsedusc1-core-app-fe-01-irzf-tfstate" # Update if your bucket name differs
       prefix = "terraform/state"
     }
   }
   ```

3. Create your `terraform.tfvars`:
   ```bash
   cp terraform.tfvars.example terraform.tfvars
   # Edit terraform.tfvars with your GCP project_id and region
   ```

4. Initialize and apply Terraform:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

This will create:
- Google Cloud APIs enabled (`run.googleapis.com`, `artifactregistry.googleapis.com`, etc.)
- Artifact Registry repository (`app-repo`)
- Dedicated runtime service accounts (e.g., `sa-service-api`)
- Initial Cloud Run service shell for `service-api`

---

### Step 3: Configure Cloud Build Triggers

Create Cloud Build triggers in the [Google Cloud Console](https://console.cloud.google.com/cloud-build/triggers) or using `gcloud`:

#### 1. Infrastructure Triggers
- **Dev (`deploy-infra-dev`)**:
  - **Event**: Push to branch `^main$`
  - **Included files filter**: `infra/**`
  - **Configuration**: Cloud Build configuration file -> `infra/cloudbuild.yaml`
  - **Substitutions**:
    - `_ENVIRONMENT`: `dev`
    - `_REGION`: `us-central1`

#### 2. Application Triggers (Trunk-Based Promotion)
- **Dev (`deploy-service-api-dev`)**:
  - **Event**: Push to branch `^main$`
  - **Included files filter**: `apps/service-api/**`
  - **Configuration**: Cloud Build configuration file -> `apps/service-api/cloudbuild.yaml`
  - **Substitutions**:
    - `_ENVIRONMENT`: `dev`
    - `_TARGET_PROJECT_ID`: `YOUR_DEV_PROJECT_ID` (e.g. `p-nsedusc1-core-app-fe-01-irzf`)
    - `_SERVICE_NAME`: `service-api`
    - `_REGION`: `us-central1`
    - `_ARTIFACT_REPO`: `app-repo`

- **Staging (`deploy-service-api-staging`)**:
  - **Event**: Push new tag (Regex: `^v.*` or `^service-api-v.*`)
  - **Configuration**: Cloud Build configuration file -> `apps/service-api/cloudbuild.yaml`
  - **Substitutions**:
    - `_ENVIRONMENT`: `staging`
    - `_TARGET_PROJECT_ID`: `YOUR_STAGING_PROJECT_ID`
    - `_SERVICE_NAME`: `service-api`
    - `_REGION`: `us-central1`
    - `_ARTIFACT_REPO`: `app-repo`

- **Production (`deploy-service-api-prod` - Approval Required)**:
  - **Event**: Push new tag (Regex: `^v.*` or `^service-api-v.*`)
  - **Approval**: Enable **"Require approval before build executes"**
  - **Configuration**: Cloud Build configuration file -> `apps/service-api/cloudbuild.yaml`
  - **Substitutions**:
    - `_ENVIRONMENT`: `prod`
    - `_TARGET_PROJECT_ID`: `YOUR_PROD_PROJECT_ID`
    - `_SERVICE_NAME`: `service-api`
    - `_REGION`: `us-central1`
    - `_ARTIFACT_REPO`: `app-repo`

---

### Step 4: Deploying & Promoting Changes

1. **Deploying to Dev**:
   - Merge your feature branch PR into `main`.
   - The `deploy-service-api-dev` trigger automatically runs tests, builds the container image with `$COMMIT_SHA`, and deploys to `dev`.
   - The `deploy-infra-dev` trigger automatically runs `terraform apply -var-file="environments/dev.tfvars"`.

2. **Promoting to Staging**:
   - Create and push a Git tag:
     ```bash
     git tag v1.0.0
     git push origin v1.0.0
     ```
   - Cloud Build automatically triggers `deploy-service-api-staging` and deploys the image to `staging`.

3. **Promoting to Production (Manual Approval)**:
   - The same tag triggers `deploy-service-api-prod` in a **Pending Approval** state.
   - Go to [Cloud Build History](https://console.cloud.google.com/cloud-build/builds) in the GCP Console, review the pending build, and click **Approve**.
   - Cloud Build deploys the verified image to **Production**.

---

## 5. Adding a New Service

To add a new service (e.g. `service-worker` or `service-web`):

1. **Create the folder**:
   ```bash
   mkdir -p apps/service-worker
   ```
2. **Add application files**:
   Add `main.py` (or Node.js/Go code), `Dockerfile`, `.dockerignore`, and `cloudbuild.yaml`.
3. **Register in `infra/variables.tf` (or `terraform.tfvars`)**:
   ```hcl
   services = {
     "service-api" = { ... },
     "service-worker" = {
       description  = "Worker service"
       port         = 8080
       allow_unauth = false
       ingress      = "INGRESS_TRAFFIC_INTERNAL_ONLY"
     }
   }
   ```
4. **Create a Cloud Build Trigger**:
   Target `apps/service-worker/**` pointing to `apps/service-worker/cloudbuild.yaml`.

---

## 6. Security Best Practices

- **Non-Root Containers**: Dockerfile creates and executes as a dedicated `appuser` (UID 1000).
- **Least Privilege Runtime SA**: Each service has its own dedicated Service Account (`sa-${service_name}`) rather than sharing the default Compute Engine SA.
- **Security Headers Middleware**: The API includes `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`, and `Strict-Transport-Security`.
- **Private / Internal Ingress**: Internal microservices can set `ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"` and `allow_unauth = false`.
- **Secrets Management**: For sensitive credentials (database passwords, API keys), mount them from **Google Secret Manager** directly into Cloud Run environment variables or volumes.

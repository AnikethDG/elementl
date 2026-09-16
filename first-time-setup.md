# First-Time Setup Guide: 4 Core Cloud Build Triggers

Welcome! You have **successfully created a new repository** from the `terraform-app-infra-template`.

This guide walks you through setting up the **4 core Cloud Build triggers** according to your organization's deployment convention:

1. **`dev-core-apps-plan`** (Pull Request to `main` -> Runs tests and `terraform plan`, no apply)
2. **`dev-core-apps-push`** (Push to `main` -> Automated build & deploy to **Dev**)
3. **`stg-core-apps-push`** (Push Git Tag -> Automated promote to **Staging**)
4. **`prd-core-apps-push`** (Push Git Tag -> Manual Approval Gate -> Deploy to **Production**)

---

## 1. Initial Setup Checklist

Before configuring triggers, ensure you have:

1. **Cloned your new repository**:
   ```bash
   git clone git@github.com:YOUR_ORG/YOUR_NEW_REPO.git
   cd YOUR_NEW_REPO
   ```

2. **Updated Project IDs in `infra/environments/`**:
   - [`infra/environments/dev.tfvars`](file:///Users/umeshkumhar/workspace/bizz/elementl/elementl-nse-core-apps/infra/environments/dev.tfvars): `project_id = "pid-nse-dev-core-apps-dz09"`
   - [`infra/environments/staging.tfvars`](file:///Users/umeshkumhar/workspace/bizz/elementl/elementl-nse-core-apps/infra/environments/staging.tfvars): `project_id = "YOUR_STAGING_PROJECT_ID"`
   - [`infra/environments/prod.tfvars`](file:///Users/umeshkumhar/workspace/bizz/elementl/elementl-nse-core-apps/infra/environments/prod.tfvars): `project_id = "YOUR_PROD_PROJECT_ID"`

3. **Set your Terraform GCS State Bucket in [`infra/backend.tf`](file:///Users/umeshkumhar/workspace/bizz/elementl/elementl-nse-core-apps/infra/backend.tf)**:
   ```hcl
   terraform {
     backend "gcs" {
       bucket = "gcs-pid-ns-cmn-app-tfstate-iv0b"
       prefix = "terraform/app-infra/development/pid-nse-dev-core-apps-dz09/infra/state"
     }
   }
   ```
   *Note: The shared bucket `gcs-pid-ns-cmn-app-tfstate-iv0b` is used across all environments. State isolation is maintained via environment folders (`development/`, `staging/`, `production/`) and project IDs: `terraform/app-infra/<env_folder>/<project_id>/infra/state`.*

4. **Bootstrapped the Dev Infrastructure**:
   ```bash
   cd infra
   terraform init -backend-config="bucket=gcs-pid-ns-cmn-app-tfstate-iv0b" -backend-config="prefix=terraform/app-infra/development/pid-nse-dev-core-apps-dz09/infra/state"
   terraform apply -var-file="environments/dev.tfvars"
   cd ..
   ```

---

## 2. The 4 Core Cloud Build Triggers

```text
+----------------------------------------------------------------------------------------------------+
|                                    4-TRIGGER LIFECYCLE FLOW                                        |
|                                                                                                    |
| 1. [PR to main]  =============================> Trigger: dev-core-apps-plan                        |
|                                                 - Runs pytest in apps/service-api                  |
|                                                 - Runs terraform plan (_TF_ACTION=plan)            |
|                                                 - Validates changes without applying               |
|                                                                                                    |
| 2. [Merge to main] ===========================> Trigger: dev-core-apps-push                        |
|                                                 - Deploys app to Dev project                       |
|                                                 - Applies infra to Dev project (_TF_ACTION=apply)  |
|                                                                                                    |
| 3. [Push Tag v*.*.*] =========================> Trigger: stg-core-apps-push                        |
|                                                 - Promotes image to Staging project                |
|                                                 - Applies infra to Staging project                 |
|                                                                                                    |
| 4. [Push Tag v*.*.*] (Same Tag) ==============> Trigger: prd-core-apps-push                        |
|                                                 - Status: PENDING_APPROVAL                         |
|                                                 - Team lead reviews & clicks "Approve"             |
|                                                 - Promotes image & infra to Production project     |
+----------------------------------------------------------------------------------------------------+
```

---

### Trigger 1: `dev-core-apps-plan` (Pull Request Validation)

- **Event**: Pull Request targeting `^main$`
- **Action**: Validates code, runs unit tests, and performs `terraform plan` without applying.
- **Approval Required**: No

#### Via Google Cloud Console:
1. Navigate to **Cloud Build** > **Triggers** in your **Dev GCP Project**.
2. Click **Create Trigger**:
   - **Name**: `dev-core-apps-plan`
   - **Description**: `Validate PRs: run unit tests and terraform plan`
   - **Event**: `Pull request`
   - **Source**: Select your 2nd-gen repository connection
   - **Base branch**: `^main$`
   - **Comment control**: `Required` (or `Not required`)
   - **Configuration**: `Cloud Build configuration file (yaml or json)`
   - **File location**: `infra/cloudbuild.yaml`
   - **Advanced > Substitution variables**:
     - `_TF_ACTION`: `plan`
     - `_ENVIRONMENT`: `dev`
     - `_REGION`: `us-central1`
3. Click **Create**.

#### Via `gcloud` CLI:
```bash
gcloud builds triggers create github \
    --name="dev-core-apps-plan" \
    --repo-name="YOUR_NEW_REPO" \
    --repo-owner="YOUR_ORG" \
    --pull-request-pattern="^main$" \
    --build-config="infra/cloudbuild.yaml" \
    --substitutions="_TF_ACTION=plan,_ENVIRONMENT=dev,_REGION=us-central1" \
    --description="Validate PRs with terraform plan and tests" \
    --project="YOUR_DEV_PROJECT_ID"
```

---

### Trigger 2: `dev-core-apps-push` (Push to Main -> Dev Deploy)

- **Event**: Push to branch `^main$`
- **Action**: Builds image with `$COMMIT_SHA`, deploys to Dev Cloud Run, and runs `terraform apply`.
- **Approval Required**: No

#### Via Google Cloud Console:
1. In your **Dev GCP Project**, go to **Cloud Build** > **Triggers**.
2. Click **Create Trigger**:
   - **Name**: `dev-core-apps-push`
   - **Description**: `Build and deploy to Dev on push to main`
   - **Event**: `Push to a branch`
   - **Branch**: `^main$`
   - **Configuration**: `Cloud Build configuration file (yaml or json)`
   - **File location**: `apps/service-api/cloudbuild.yaml`
   - **Advanced > Substitution variables**:
     - `_ENVIRONMENT`: `dev`
     - `_TARGET_PROJECT_ID`: `YOUR_DEV_PROJECT_ID`
     - `_SERVICE_NAME`: `service-api`
     - `_REGION`: `us-central1`
     - `_ARTIFACT_REPO`: `app-repo`
3. Click **Create**.

#### Via `gcloud` CLI:
```bash
gcloud builds triggers create github \
    --name="dev-core-apps-push" \
    --repo-name="YOUR_NEW_REPO" \
    --repo-owner="YOUR_ORG" \
    --branch-pattern="^main$" \
    --build-config="apps/service-api/cloudbuild.yaml" \
    --substitutions="_ENVIRONMENT=dev,_TARGET_PROJECT_ID=YOUR_DEV_PROJECT_ID,_SERVICE_NAME=service-api,_REGION=us-central1,_ARTIFACT_REPO=app-repo" \
    --description="Build and deploy to Dev on main push" \
    --project="YOUR_DEV_PROJECT_ID"
```

---

### Trigger 3: `stg-core-apps-push` (Tag Push -> Staging Promotion)

- **Event**: Push new tag `^v.*` (e.g. `v1.0.0`)
- **Action**: Promotes the verified container image to the **Staging** project.
- **Approval Required**: No

#### Via Google Cloud Console:
1. In your **Staging GCP Project**, go to **Cloud Build** > **Triggers**.
2. Click **Create Trigger**:
   - **Name**: `stg-core-apps-push`
   - **Description**: `Promote verified image to Staging on tag push`
   - **Event**: `Push new tag`
   - **Tag**: `^v.*`
   - **Configuration**: `Cloud Build configuration file (yaml or json)`
   - **File location**: `apps/service-api/cloudbuild.yaml`
   - **Advanced > Substitution variables**:
     - `_ENVIRONMENT`: `staging`
     - `_TARGET_PROJECT_ID`: `YOUR_STAGING_PROJECT_ID`
     - `_SERVICE_NAME`: `service-api`
     - `_REGION`: `us-central1`
     - `_ARTIFACT_REPO`: `app-repo`
3. Click **Create**.

#### Via `gcloud` CLI:
```bash
gcloud builds triggers create github \
    --name="stg-core-apps-push" \
    --repo-name="YOUR_NEW_REPO" \
    --repo-owner="YOUR_ORG" \
    --tag-pattern="^v.*" \
    --build-config="apps/service-api/cloudbuild.yaml" \
    --substitutions="_ENVIRONMENT=staging,_TARGET_PROJECT_ID=YOUR_STAGING_PROJECT_ID,_SERVICE_NAME=service-api,_REGION=us-central1,_ARTIFACT_REPO=app-repo" \
    --description="Promote image to Staging on tag push" \
    --project="YOUR_STAGING_PROJECT_ID"
```

---

### Trigger 4: `prd-core-apps-push` (Tag Push -> Production with Approval Gate)

- **Event**: Push new tag `^v.*` (same tag as Staging)
- **Action**: Pauses in `PENDING_APPROVAL` status until a team lead approves in the Cloud Build Console, then deploys to **Production**.
- **Approval Required**: **YES**

#### Via Google Cloud Console:
1. In your **Production GCP Project**, go to **Cloud Build** > **Triggers**.
2. Click **Create Trigger**:
   - **Name**: `prd-core-apps-push`
   - **Description**: `Deploy to Production on tag push (Approval Required)`
   - **Event**: `Push new tag`
   - **Tag**: `^v.*`
   - **Configuration**: `Cloud Build configuration file (yaml or json)`
   - **File location**: `apps/service-api/cloudbuild.yaml`
   - **Approval**: Check **"Require approval before build executes"**
   - **Advanced > Substitution variables**:
     - `_ENVIRONMENT`: `prod`
     - `_TARGET_PROJECT_ID`: `YOUR_PROD_PROJECT_ID`
     - `_SERVICE_NAME`: `service-api`
     - `_REGION`: `us-central1`
     - `_ARTIFACT_REPO`: `app-repo`
3. Click **Create**.

#### Via `gcloud` CLI:
```bash
gcloud builds triggers create github \
    --name="prd-core-apps-push" \
    --repo-name="YOUR_NEW_REPO" \
    --repo-owner="YOUR_ORG" \
    --tag-pattern="^v.*" \
    --build-config="apps/service-api/cloudbuild.yaml" \
    --require-approval \
    --substitutions="_ENVIRONMENT=prod,_TARGET_PROJECT_ID=YOUR_PROD_PROJECT_ID,_SERVICE_NAME=service-api,_REGION=us-central1,_ARTIFACT_REPO=app-repo" \
    --description="Deploy to Prod on tag push (Approval Required)" \
    --project="YOUR_PROD_PROJECT_ID"
```

---

## 3. Quick Reference: The 4 Triggers

| # | Trigger Name | Event | Target Project | Target Env | Action | Approval Required |
| :- | :--- | :--- | :--- | :--- | :--- | :- |
| 1 | **`dev-core-apps-plan`** | PR to `^main$` | Dev Project | `dev` | Runs tests & `terraform plan` | No |
| 2 | **`dev-core-apps-push`** | Push to `^main$` | Dev Project | `dev` | Deploys app & applies infra | No |
| 3 | **`stg-core-apps-push`** | Push Tag `^v.*` | Staging Project | `staging` | Promotes to Staging | No |
| 4 | **`prd-core-apps-push`** | Push Tag `^v.*` | Prod Project | `prod` | Promotes to Production | **Yes (Manual)** |

---

## 4. Testing the Full Lifecycle

1. **Test Trigger 1 (`dev-core-apps-plan`)**:
   - Create a feature branch: `git checkout -b feat/test-trigger`
   - Make a minor edit in `apps/service-api/main.py`.
   - Push and open a Pull Request targeting `main`.
   - Verify that `dev-core-apps-plan` runs and reports status on the PR.

2. **Test Trigger 2 (`dev-core-apps-push`)**:
   - Merge the PR into `main`.
   - Verify that `dev-core-apps-push` triggers, builds the image with `$COMMIT_SHA`, and deploys to Dev Cloud Run.

3. **Test Triggers 3 & 4 (`stg-core-apps-push` & `prd-core-apps-push`)**:
   - Create and push a semantic release tag:
     ```bash
     git tag v1.0.0
     git push origin v1.0.0
     ```
   - Verify that `stg-core-apps-push` runs automatically in Staging.
   - Verify that `prd-core-apps-push` enters `PENDING_APPROVAL` status in the Production Cloud Build Console.
   - Click **Approve** to deploy to Production.

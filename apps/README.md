# Applications Directory (`apps/`)

This directory houses all independent microservices for the monorepo. Each service is completely isolated in its own subfolder.

## Directory Structure

```
apps/
├── service-api/              # Example Python FastAPI service
│   ├── main.py               # Service code
│   ├── requirements.txt      # Dependencies
│   ├── test_main.py          # Unit tests
│   ├── Dockerfile            # Container definition
│   ├── .dockerignore         # Docker build exclusions
│   └── cloudbuild.yaml       # Dedicated CI/CD pipeline
└── <new-service>/            # Add additional services here
```

## Adding a New Service

To onboard a new service (e.g., `service-worker` or `service-web`):

1. **Create the service directory**:
   ```bash
   mkdir -p apps/service-worker
   ```

2. **Add application code, tests, and Dockerfile**:
   Ensure the container listens on port `$PORT` (Cloud Run dynamically sets this, default is `8080`).

3. **Add `cloudbuild.yaml`**:
   Copy and adapt `apps/service-api/cloudbuild.yaml`:
   - Update `_SERVICE_NAME: 'service-worker'`
   - Update `dir: 'apps/service-worker'` for all build steps.

4. **Register the service in Terraform (`infra/variables.tf` or `infra/terraform.tfvars`)**:
   Add an entry under `services`:
   ```hcl
   services = {
     "service-api" = { ... },
     "service-worker" = {
       description  = "Background worker service"
       port         = 8080
       allow_unauth = false # Internal or authenticated only
       ingress      = "INGRESS_TRAFFIC_INTERNAL_ONLY"
     }
   }
   ```

5. **Configure a Cloud Build Trigger**:
   - **Name**: `deploy-service-worker`
   - **Event**: Push to branch `^main$`
   - **Included files filter**: `apps/service-worker/**`
   - **Configuration**: Cloud Build configuration file -> `apps/service-worker/cloudbuild.yaml`

## Local Development

You can run and test services locally without Docker or inside Docker:

```bash
# Run locally with virtualenv
cd apps/service-api
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest
python main.py

# Run with Docker
docker build -t service-api .
docker run -p 8080:8080 service-api
```

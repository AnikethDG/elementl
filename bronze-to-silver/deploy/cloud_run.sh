#!/usr/bin/env bash
# Build / deploy / run the silver-transform Cloud Run Job (idempotent).
#
#   deploy/cloud_run.sh build                 # gcloud builds submit -> Artifact Registry (:<tag> and :latest)
#   deploy/cloud_run.sh deploy [image]        # gcloud run jobs deploy (create or update)
#   deploy/cloud_run.sh run <transform> [ds] [run_id] [output_table] [params_json]
#   deploy/cloud_run.sh all <transform>...    # build + deploy + run each transform sequentially
#
# Overridable env: PROJECT REGION REPO IMAGE JOB_NAME SERVICE_ACCOUNT BRONZE_DATASET SILVER_DATASET TAG
set -euo pipefail

export CLOUDSDK_METRICS_ENVIRONMENT="${CLOUDSDK_METRICS_ENVIRONMENT:-datacloud.antigravity}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JOB_DIR="${JOB_DIR:-${SCRIPT_DIR}/../cloud-run-jobs/silver-transform}"

PROJECT="${PROJECT:-ani-labs}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-us-central1-docker.pkg.dev/${PROJECT}/udp-ingestion-jobs}"
IMAGE="${IMAGE:-${REPO}/silver-transform}"
JOB_NAME="${JOB_NAME:-silver-transform}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-5473845300-compute@developer.gserviceaccount.com}"
BRONZE_DATASET="${BRONZE_DATASET:-elementl_bronze}"
SILVER_DATASET="${SILVER_DATASET:-elementl_silver}"
TASK_TIMEOUT="${TASK_TIMEOUT:-30m}"
MEMORY="${MEMORY:-2Gi}"
CPU="${CPU:-1}"

log() { echo "[cloud_run.sh $(date -u +%H:%M:%S)] $*" >&2; }

default_tag() {
  local git_sha
  git_sha="$(git -C "${JOB_DIR}" rev-parse --short HEAD 2>/dev/null || true)"
  if [[ -n "${git_sha}" ]]; then echo "${git_sha}-$(date -u +%Y%m%d%H%M)"; else date -u +%Y%m%d%H%M%S; fi
}

# transform name -> default silver table (mirrors transforms/<name>.py DEFAULT_TABLE)
default_table() {
  case "$1" in
    fiscal_calendar)        echo "dim_netsuite_fiscal_calendar" ;;
    address_enrichment)     echo "stg_netsuite_address_enriched" ;;
    customer_domains)       echo "stg_netsuite_customer_domains" ;;
    transaction_anomalies)  echo "fct_netsuite_transaction_anomalies" ;;
    budget_vs_actual)       echo "fct_netsuite_budget_vs_actual" ;;
    *)                      echo "$1" ;;
  esac
}

cmd_build() {
  local tag="${TAG:-$(default_tag)}"
  log "building ${IMAGE}:${tag} (+ :latest) from ${JOB_DIR}"
  gcloud builds submit "${JOB_DIR}" --project "${PROJECT}" --region "${REGION}" \
    --tag "${IMAGE}:${tag}" --quiet
  # also tag :latest without rebuilding
  gcloud artifacts docker tags add "${IMAGE}:${tag}" "${IMAGE}:latest" --project "${PROJECT}" --quiet
  echo "${IMAGE}:${tag}" > "${JOB_DIR}/.last_image"
  log "built ${IMAGE}:${tag}"
  echo "${IMAGE}:${tag}"
}

cmd_deploy() {
  local image="${1:-}"
  if [[ -z "${image}" ]]; then
    if [[ -f "${JOB_DIR}/.last_image" ]]; then image="$(cat "${JOB_DIR}/.last_image")"; else image="${IMAGE}:latest"; fi
  fi
  log "deploying job ${JOB_NAME} in ${REGION} with ${image}"
  # `gcloud run jobs deploy` creates or updates -> idempotent
  gcloud run jobs deploy "${JOB_NAME}" \
    --project "${PROJECT}" --region "${REGION}" \
    --image "${image}" \
    --service-account "${SERVICE_ACCOUNT}" \
    --max-retries 0 --task-timeout "${TASK_TIMEOUT}" --tasks 1 --parallelism 1 \
    --memory "${MEMORY}" --cpu "${CPU}" \
    --set-env-vars "GCP_PROJECT=${PROJECT},BRONZE_DATASET=${BRONZE_DATASET},SILVER_DATASET=${SILVER_DATASET}" \
    --quiet
  log "deployed ${JOB_NAME}"
}

cmd_run() {
  local transform="${1:?usage: run <transform> [ds] [run_id] [output_table] [params_json]}"
  local ds="${2:-$(date -u +%F)}"
  local run_id="${3:-manual-$(date -u +%Y%m%dT%H%M%SZ)}"
  local output_table="${4:-${PROJECT}.${SILVER_DATASET}.$(default_table "${transform}")}"
  local default_params='{}'
  local params="${5:-${default_params}}"
  log "executing ${JOB_NAME}: TRANSFORM=${transform} OUTPUT_TABLE=${output_table} EXECUTION_DATE=${ds} RUN_ID=${run_id}"
  local start; start=$(date +%s)
  # ^|^ delimiter so PARAMS may contain commas
  gcloud run jobs execute "${JOB_NAME}" --project "${PROJECT}" --region "${REGION}" --wait \
    --update-env-vars "^|^TRANSFORM=${transform}|OUTPUT_TABLE=${output_table}|EXECUTION_DATE=${ds}|RUN_ID=${run_id}|PARAMS=${params}" \
    --format='value(metadata.name)' | tee "${JOB_DIR}/.last_execution"
  local rc=${PIPESTATUS[0]}
  log "execution $(cat "${JOB_DIR}/.last_execution" 2>/dev/null) finished rc=${rc} in $(( $(date +%s) - start ))s"
  return "${rc}"
}

cmd_logs() {
  local execution="${1:-$(cat "${JOB_DIR}/.last_execution" 2>/dev/null)}"
  gcloud logging read \
    "resource.type=\"cloud_run_job\" AND resource.labels.job_name=\"${JOB_NAME}\" AND labels.\"run.googleapis.com/execution_name\"=\"${execution}\"" \
    --project "${PROJECT}" --limit 200 --freshness 2h --format='value(textPayload)' | tac
}

case "${1:-}" in
  build)  shift; cmd_build "$@" ;;
  deploy) shift; cmd_deploy "$@" ;;
  run)    shift; cmd_run "$@" ;;
  logs)   shift; cmd_logs "$@" ;;
  all)
    shift
    image="$(cmd_build)"
    cmd_deploy "${image}"
    for t in "$@"; do cmd_run "${t}"; done
    ;;
  *)
    sed -n '2,10p' "$0"; exit 1 ;;
esac

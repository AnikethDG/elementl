#!/usr/bin/env bash
# Deploy / run the UDP bronze-to-silver Spark engine on Dataproc Serverless.
#
#   deploy/spark.sh                 upload lib + all jobs to GCS (idempotent, overwrites)
#   deploy/spark.sh deploy          same
#   deploy/spark.sh run <job> [--ds YYYY-MM-DD] [--run-id ID] [--params '<json>'] [--output-table p.d.t] [--no-wait]
#                                   submit one job as a batch with the standard CLI contract
#   deploy/spark.sh list            list known jobs and their default output tables
#
# Environment overrides (defaults = ani-labs sandbox):
#   UDP_PROJECT UDP_REGION UDP_BUCKET UDP_SUBNET UDP_SERVICE_ACCOUNT UDP_RUNTIME_VERSION
#   UDP_BRONZE_DATASET UDP_SILVER_DATASET
set -euo pipefail

export CLOUDSDK_METRICS_ENVIRONMENT=datacloud.antigravity

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SPARK_DIR="${REPO_ROOT}/spark"

PROJECT="${UDP_PROJECT:-ani-labs}"
REGION="${UDP_REGION:-us-central1}"
BUCKET="${UDP_BUCKET:-bkt-ani-labs-udp-configs}"
SUBNET="${UDP_SUBNET:-acai-network}"
SERVICE_ACCOUNT="${UDP_SERVICE_ACCOUNT:-5473845300-compute@developer.gserviceaccount.com}"
RUNTIME_VERSION="${UDP_RUNTIME_VERSION:-2.2}"
BRONZE_DATASET="${UDP_BRONZE_DATASET:-elementl_bronze}"
SILVER_DATASET="${UDP_SILVER_DATASET:-elementl_silver}"

GCS_BASE="gs://${BUCKET}/silver/spark"
GCS_JOBS="${GCS_BASE}/jobs"
GCS_LIB="${GCS_BASE}/lib/udp_silver_common.py"
GCS_TMP="${GCS_BASE}/tmp"

# job name -> silver output table (keep in sync with spark/README.md)
declare -A JOB_TABLES=(
  [fct_account_running_balance]=fct_netsuite_account_running_balance
  [stg_item_tags]=stg_netsuite_item_tags
  [stg_employee_masked]=stg_netsuite_employee
  [fct_transaction_usd]=fct_netsuite_transaction_usd
  [dim_classification_path]=dim_netsuite_classification_path
)

log() { echo "[spark.sh] $*" >&2; }

cmd_deploy() {
  log "compiling"
  python3 -m py_compile "${SPARK_DIR}/lib/udp_silver_common.py" "${SPARK_DIR}"/jobs/*.py
  log "uploading lib -> ${GCS_LIB}"
  gcloud storage cp "${SPARK_DIR}/lib/udp_silver_common.py" "${GCS_LIB}" --project "${PROJECT}" --quiet
  log "uploading jobs -> ${GCS_JOBS}/"
  gcloud storage cp "${SPARK_DIR}"/jobs/*.py "${GCS_JOBS}/" --project "${PROJECT}" --quiet
  # make sure the connector temp prefix exists (only used with writeMethod=indirect)
  gcloud storage ls "${GCS_TMP}/" >/dev/null 2>&1 || echo -n | gcloud storage cp - "${GCS_TMP}/.keep" --quiet
  log "deployed:"
  gcloud storage ls "${GCS_BASE}/lib/" "${GCS_JOBS}/"
}

cmd_list() {
  for j in "${!JOB_TABLES[@]}"; do
    printf '%-32s -> %s.%s.%s\n' "$j" "${PROJECT}" "${SILVER_DATASET}" "${JOB_TABLES[$j]}"
  done | sort
}

cmd_run() {
  local job="${1:-}"; shift || true
  [[ -n "${job}" && -n "${JOB_TABLES[${job}]:-}" ]] || { log "unknown job '${job}'. Known:"; cmd_list; exit 2; }
  local ds run_id params output_table wait=1
  ds="$(date -u +%F)"
  run_id="manual__$(date -u +%Y%m%dT%H%M%SZ)"
  params='{}'
  output_table="${PROJECT}.${SILVER_DATASET}.${JOB_TABLES[${job}]}"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --ds) ds="$2"; shift 2 ;;
      --run-id) run_id="$2"; shift 2 ;;
      --params) params="$2"; shift 2 ;;
      --output-table) output_table="$2"; shift 2 ;;
      --no-wait) wait=0; shift ;;
      *) log "unknown option $1"; exit 2 ;;
    esac
  done
  local batch_id
  batch_id="udp-silver-$(echo "${job}" | tr '_' '-')-$(date -u +%Y%m%d-%H%M%S)-$RANDOM"
  batch_id="${batch_id:0:63}"
  log "submitting batch ${batch_id} (${job} -> ${output_table}, ds=${ds}, run_id=${run_id})"
  local gcloud_flags=(--project "${PROJECT}" --region "${REGION}" --batch "${batch_id}"
    --subnet "${SUBNET}" --service-account "${SERVICE_ACCOUNT}" --version "${RUNTIME_VERSION}"
    --py-files "${GCS_LIB}"
    --labels "datacloud=antigravity,udp-engine=spark,udp-job=${job//_/-}")
  [[ ${wait} -eq 1 ]] || gcloud_flags+=(--async)   # must precede '--': everything after it goes to the job
  gcloud dataproc batches submit pyspark "${GCS_JOBS}/${job}.py" "${gcloud_flags[@]}" \
    -- --project "${PROJECT}" --bronze-dataset "${BRONZE_DATASET}" --silver-dataset "${SILVER_DATASET}" \
       --output-table "${output_table}" --ds "${ds}" --run-id "${run_id}" --params "${params}"
  echo "${batch_id}"
}

case "${1:-deploy}" in
  deploy) cmd_deploy ;;
  list) cmd_list ;;
  run) shift; cmd_run "$@" ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac

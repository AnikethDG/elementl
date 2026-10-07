#!/usr/bin/env bash
# Push / compile / run the bronze-to-silver Dataform project in GCP WITHOUT a git connection,
# using only the Dataform REST API (same surface the Airflow orchestrator uses).
#
#   deploy/dataform.sh push                 create repo + workspace if missing, upload all project files
#   deploy/dataform.sh compile              compile the workspace, print errors + actions, save result name
#   deploy/dataform.sh run <tag> [run_id]   compile (if needed) + invoke actions with <tag>, poll to completion
#   deploy/dataform.sh status <invocation>  print invocation state and per-action results
#
# Environment overrides (defaults = ani-labs sandbox):
#   PROJECT, LOCATION, REPO, WORKSPACE, BRONZE_DATASET, SILVER_DATASET, ASSERTION_DATASET, DATAFORM_DIR,
#   RUN_SERVICE_ACCOUNT (identity the invocation runs as; required in ani-labs, see below)
# Requires: gcloud (authenticated), curl, python3.
set -euo pipefail

export CLOUDSDK_METRICS_ENVIRONMENT="${CLOUDSDK_METRICS_ENVIRONMENT:-datacloud.antigravity}"

PROJECT="${PROJECT:-ani-labs}"
LOCATION="${LOCATION:-us-central1}"
REPO="${REPO:-bronze-to-silver}"
WORKSPACE="${WORKSPACE:-labs}"
BRONZE_DATASET="${BRONZE_DATASET:-elementl_bronze}"
SILVER_DATASET="${SILVER_DATASET:-elementl_silver}"
ASSERTION_DATASET="${ASSERTION_DATASET:-elementl_silver_assertions}"
# ani-labs enforces "strict act-as" (createWorkflowInvocation fails with 400 "Service account must
# be set when strict act as checks are enabled" without it). The Dataform service agent needs
# roles/iam.serviceAccountTokenCreator on this SA; the SA needs bigquery.dataEditor + jobUser.
# Set RUN_SERVICE_ACCOUNT="" to omit the field (projects without the constraint).
RUN_SERVICE_ACCOUNT="${RUN_SERVICE_ACCOUNT-5473845300-compute@developer.gserviceaccount.com}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATAFORM_DIR="${DATAFORM_DIR:-$SCRIPT_DIR/../dataform}"
STATE_FILE="${STATE_FILE:-/tmp/dataform_${PROJECT}_${REPO}_${WORKSPACE}.compilation}"

API="https://dataform.googleapis.com/v1beta1"
PARENT="projects/${PROJECT}/locations/${LOCATION}"
REPO_NAME="${PARENT}/repositories/${REPO}"
WS_NAME="${REPO_NAME}/workspaces/${WORKSPACE}"

log() { echo "[dataform.sh] $*" >&2; }

token() { gcloud auth print-access-token; }

# api <METHOD> <path-after-API> [json-body]
api() {
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" -H "Authorization: Bearer $(token)" -H "Content-Type: application/json")
  [[ -n "$body" ]] && args+=(-d "$body")
  curl "${args[@]}" "${API}/${path}"
}

# Fails (exit 1) if the JSON on stdin is an API error object.
check_error() {
  python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print(raw); sys.exit(1)
if isinstance(d, dict) and "error" in d:
    print(json.dumps(d["error"], indent=2)); sys.exit(1)
print(raw)'
}

ensure_repo() {
  local code
  code=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(token)" "${API}/${REPO_NAME}")
  if [[ "$code" == "200" ]]; then
    log "repository ${REPO_NAME} exists"
  else
    log "creating repository ${REPO_NAME}"
    api POST "${PARENT}/repositories?repositoryId=${REPO}" '{}' | check_error >/dev/null
  fi
}

ensure_workspace() {
  local code
  code=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $(token)" "${API}/${WS_NAME}")
  if [[ "$code" == "200" ]]; then
    log "workspace ${WS_NAME} exists"
  else
    log "creating workspace ${WS_NAME}"
    api POST "${REPO_NAME}/workspaces?workspaceId=${WORKSPACE}" '{}' | check_error >/dev/null
  fi
}

# Upload every project file (workflow_settings.yaml, definitions/**, includes/**, package.json if any).
push_files() {
  local f rel body
  pushd "$DATAFORM_DIR" >/dev/null
  while IFS= read -r -d '' f; do
    rel="${f#./}"
    body=$(python3 - "$rel" <<'PY'
import base64, json, sys
p = sys.argv[1]
print(json.dumps({"path": p, "contents": base64.b64encode(open(p, "rb").read()).decode()}))
PY
)
    api POST "${WS_NAME}:writeFile" "$body" | check_error >/dev/null
    log "uploaded ${rel}"
  done < <(find . -type f \( -name '*.sqlx' -o -name '*.js' -o -name 'workflow_settings.yaml' -o -name 'package.json' -o -name 'dataform.json' \) \
              -not -path './node_modules/*' -not -name 'README.md' -print0 | sort -z)
  popd >/dev/null
}

cmd_push() {
  ensure_repo
  ensure_workspace
  push_files
  log "push complete"
}

compile_payload() {
  local run_id="${1:-dataform}"
  cat <<EOF
{
  "workspace": "${WS_NAME}",
  "codeCompilationConfig": {
    "defaultDatabase": "${PROJECT}",
    "defaultSchema": "${SILVER_DATASET}",
    "assertionSchema": "${ASSERTION_DATASET}",
    "defaultLocation": "${LOCATION}",
    "vars": {
      "udp_project": "${PROJECT}",
      "bronze_dataset": "${BRONZE_DATASET}",
      "silver_dataset": "${SILVER_DATASET}",
      "run_id": "${run_id}"
    }
  }
}
EOF
}

cmd_compile() {
  local run_id="${1:-dataform}" out name
  out=$(api POST "${REPO_NAME}/compilationResults" "$(compile_payload "$run_id")" | check_error)
  name=$(echo "$out" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')
  echo "$out" | python3 -c '
import json, sys
d = json.load(sys.stdin)
errs = d.get("compilationErrors", [])
print("compilationResult:", d["name"], file=sys.stderr)
print("dataformCoreVersion:", d.get("dataformCoreVersion"), file=sys.stderr)
if errs:
    print(f"{len(errs)} COMPILATION ERROR(S):", file=sys.stderr)
    for e in errs:
        print(" -", e.get("path"), ":", e.get("message"), file=sys.stderr)
    sys.exit(2)
'
  echo "$name" > "$STATE_FILE"
  log "actions:"
  api GET "${name}:query?pageSize=200" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for a in d.get("compilationResultActions", []):
    t = a["target"]; kind = next(k for k in ("relation","assertion","declaration","operations","notebook") if k in a)
    tags = a.get(kind, {}).get("tags", [])
    print("  %-11s %s.%s.%s  tags=%s" % (kind, t["database"], t["schema"], t["name"], tags), file=sys.stderr)
'
  echo "$name"
}

cmd_run() {
  local tag="${1:?usage: run <tag> [run_id]}" run_id="${2:-dataform}" comp inv_name
  comp=$(cmd_compile "$run_id")
  local payload sa_line=""
  [[ -n "$RUN_SERVICE_ACCOUNT" ]] && sa_line=",
    \"serviceAccount\": \"${RUN_SERVICE_ACCOUNT}\""
  payload=$(cat <<EOF
{
  "compilationResult": "${comp}",
  "invocationConfig": {
    "includedTags": ["${tag}"],
    "transitiveDependenciesIncluded": true,
    "transitiveDependentsIncluded": false${sa_line}
  }
}
EOF
)
  inv_name=$(api POST "${REPO_NAME}/workflowInvocations" "$payload" | check_error | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')
  log "workflowInvocation: ${inv_name}"
  poll "$inv_name"
}

poll() {
  local inv="$1" state
  while true; do
    state=$(api GET "$inv" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state","UNKNOWN"))')
    log "state=${state}"
    case "$state" in
      SUCCEEDED) cmd_status "$inv"; return 0 ;;
      FAILED|CANCELLED|CANCELING) cmd_status "$inv"; return 1 ;;
      *) sleep 5 ;;
    esac
  done
}

cmd_status() {
  local inv="${1:?usage: status <invocation name or id>}"
  [[ "$inv" == projects/* ]] || inv="${REPO_NAME}/workflowInvocations/${inv}"
  api GET "$inv" | python3 -c '
import json, sys
d = json.load(sys.stdin)
print("invocation:", d["name"]); print("state:", d.get("state")); print("timing:", d.get("invocationTiming"))'
  api GET "${inv}:query?pageSize=200" | python3 -c '
import json, sys
d = json.load(sys.stdin)
for a in d.get("workflowInvocationActions", []):
    t = a["target"]
    line = "  %-9s %s.%s" % (a.get("state"), t["schema"], t["name"])
    if a.get("failureReason"): line += "\n      FAILURE: " + a["failureReason"]
    print(line)'
}

case "${1:-}" in
  push) cmd_push ;;
  compile) cmd_compile "${2:-dataform}" >/dev/null ;;
  run) cmd_run "${2:-}" "${3:-dataform}" ;;
  status) cmd_status "${2:-}" ;;
  *) sed -n '2,13p' "$0"; exit 1 ;;
esac

#!/usr/bin/env bash
# Upload the BigQuery SQL scripts to the per-project UDP config bucket so Composer can read them.
#
#   deploy/bigquery.sh <gcp-project> [bucket]
#
# Default bucket: gs://bkt-<gcp-project>-udp-configs  (same bucket the ingestion factory uses).
# Scripts land under silver/bigquery/sql/<name>.sql which is what the transformation YAMLs reference:
#   transformation.bigquery.sql_file: gs://bkt-${GCP_PROJECT_ID}-udp-configs/silver/bigquery/sql/<name>.sql
set -euo pipefail
PROJECT=${1:?usage: deploy/bigquery.sh <gcp-project> [bucket]}
BUCKET=${2:-gs://bkt-${PROJECT}-udp-configs}
HERE=$(cd "$(dirname "$0")/.." && pwd)
export CLOUDSDK_METRICS_ENVIRONMENT=${CLOUDSDK_METRICS_ENVIRONMENT:-datacloud.antigravity}

PY=${PYTHON:-$HERE/.venv/bin/python}; [ -x "$PY" ] || PY=python3

echo "Validating placeholders render offline ..."
"$PY" -m pytest -q "$HERE/tests/bigquery" >/dev/null

echo "Uploading $HERE/bigquery/sql/*.sql -> $BUCKET/silver/bigquery/sql/"
gcloud storage cp --project="$PROJECT" "$HERE"/bigquery/sql/*.sql "$BUCKET/silver/bigquery/sql/"
gcloud storage ls "$BUCKET/silver/bigquery/sql/"

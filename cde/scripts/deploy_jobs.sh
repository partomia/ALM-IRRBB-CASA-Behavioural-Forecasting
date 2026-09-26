#!/usr/bin/env bash
# Create/sync the CDE Repository for this GitHub repo and (re)create the four
# Spark jobs, each reading its application file straight from the repo.
#
# After a code change: git push, then either re-run this script or just
#   cde repository sync --name rsingh-casa-alb-pipeline
#
# Private repo? Store a GitHub PAT as a CDE credential first (the flag takes
# the credential NAME, not the token):
#   cde credential create --name my-github-pat --type basic --username <github-user>
#   GIT_CREDENTIAL=my-github-pat ./cde/scripts/deploy_jobs.sh
#
# No python-env resource: the jobs only use PySpark and the standard library.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/ALM-IRRBB-CASA-Behavioural-Forecasting}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-casa-alb-pipeline}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-casa-alb}"
DB_PREFIX="${DB_PREFIX:-rsingh_casa_alb}"

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  create_args=(--name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}")
  [[ -n "${GIT_CREDENTIAL:-}" ]] && create_args+=(--credential "${GIT_CREDENTIAL}")
  cde repository create "${create_args[@]}"
fi
cde repository sync --name "${REPO_NAME}"

create_job() {
  local name=$1 file=$2
  shift 2
  if cde job describe --name "${name}" &>/dev/null; then
    cde job delete --name "${name}"
  fi
  echo "==> Creating job ${name} (${file})"
  cde job create --name "${name}" --type spark \
    --mount-1-resource "${REPO_NAME}" \
    --application-file "${file}" \
    --arg=--db-prefix --arg="${DB_PREFIX}" "$@"
}

create_job "${JOB_PREFIX}-generate-cbs-bronze" "cde/jobs/generate_cbs_bronze.py" \
  --driver-memory 4g --executor-memory 4g --max-executors 4
create_job "${JOB_PREFIX}-validate-bronze"     "cde/jobs/validate_bronze.py"
create_job "${JOB_PREFIX}-build-silver-daily"  "cde/jobs/build_silver_daily.py"
create_job "${JOB_PREFIX}-build-gold-weekly"   "cde/jobs/build_gold_weekly.py"

echo ""
echo "Jobs deployed from ${REPO_NAME}. Run one:"
echo "  cde job run --name ${JOB_PREFIX}-generate-cbs-bronze --wait"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"

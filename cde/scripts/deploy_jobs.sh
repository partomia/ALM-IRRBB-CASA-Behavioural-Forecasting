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
# Resources: the vcluster default (1 core / 1 GB) is very slow for ~2.5M rows,
# so every job gets a 2-core / 4 GB driver and executors of 2 cores / 4 GB,
# 1 min / 2 initial / 4 max. The vcluster's YuniKorn queue must fit the driver
# plus the initial executors up front (PySpark adds 40% memory overhead), or the
# run is rejected with "queue ... cannot fit application". The federal queue caps
# at 27 vCPU / ~110 GB, shared by several projects. Every size can be overridden,
# e.g. INITIAL_EXECUTORS=1 MAX_EXECUTORS=2 ./cde/scripts/deploy_jobs.sh
#
# This script does not delete jobs it no longer defines; remove orphans by hand.

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/partomia/ALM-IRRBB-CASA-Behavioural-Forecasting}"
REPO_BRANCH="${REPO_BRANCH:-main}"
REPO_NAME="${REPO_NAME:-rsingh-casa-alb-pipeline}"
PYTHON_ENV="${PYTHON_ENV:-rsingh-casa-alb-python-env}"
JOB_PREFIX="${JOB_PREFIX:-rsingh-casa-alb}"
DB_PREFIX="${DB_PREFIX:-rsingh_casa_alb}"
REQUIREMENTS="$(cd "$(dirname "$0")/.." && pwd)/resources/requirements.txt"
RESOURCES=(--driver-cores "${DRIVER_CORES:-2}" --driver-memory "${DRIVER_MEMORY:-4g}"
           --executor-cores "${EXECUTOR_CORES:-2}" --executor-memory "${EXECUTOR_MEMORY:-4g}"
           --min-executors "${MIN_EXECUTORS:-1}" --initial-executors "${INITIAL_EXECUTORS:-2}" --max-executors "${MAX_EXECUTORS:-4}"
           --conf spark.sql.shuffle.partitions=48)

echo "==> Repository: ${REPO_NAME}"
if cde repository describe --name "${REPO_NAME}" &>/dev/null; then
  echo "    exists, syncing ${REPO_BRANCH}"
else
  create_args=(--name "${REPO_NAME}" --url "${REPO_URL}" --branch "${REPO_BRANCH}")
  [[ -n "${GIT_CREDENTIAL:-}" ]] && create_args+=(--credential "${GIT_CREDENTIAL}")
  cde repository create "${create_args[@]}"
fi
cde repository sync --name "${REPO_NAME}"

echo "==> Python environment resource: ${PYTHON_ENV}"
cde resource create --name "${PYTHON_ENV}" --type python-env 2>/dev/null || true
cde resource upload --name "${PYTHON_ENV}" --local-path "${REQUIREMENTS}"
echo "    building (1-3 min); jobs fail fast until it is ready:"
for _ in $(seq 1 30); do
  status="$(cde resource describe --name "${PYTHON_ENV}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")"
  echo "    status: ${status}"
  [[ "${status}" == "ready" ]] && break
  [[ "${status}" == "failed" ]] && { echo "python-env build failed"; exit 1; }
  sleep 20
done
[[ "${status}" == "ready" ]] || { echo "python-env not ready after 10 minutes"; exit 1; }

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
    --python-env-resource-name "${PYTHON_ENV}" \
    "${RESOURCES[@]}" \
    --arg=--db-prefix --arg="${DB_PREFIX}" "$@"
}

create_job "${JOB_PREFIX}-generate-cbs-bronze" "cde/jobs/generate_cbs_bronze.py"
create_job "${JOB_PREFIX}-validate-bronze"     "cde/jobs/validate_bronze.py"
create_job "${JOB_PREFIX}-build-silver-daily"  "cde/jobs/build_silver_daily.py"
create_job "${JOB_PREFIX}-build-gold-weekly"   "cde/jobs/build_gold_weekly.py"

echo ""
echo "Jobs deployed from ${REPO_NAME}. Run the chain for one as-of date (--wait can return"
echo "early on this vcluster: poll 'cde run describe --id <run id>' until it ends):"
echo "  cde job run --name ${JOB_PREFIX}-generate-cbs-bronze --arg=--db-prefix --arg=${DB_PREFIX} --arg=--as-of --arg=YYYY-MM-DD"
echo "  ... validate-bronze, build-silver-daily, build-gold-weekly"
echo "Then register the DAG: ./cde/scripts/deploy_dag.sh"

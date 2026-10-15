#!/usr/bin/env bash
# Register/update the Airflow DAG as a `--type airflow` CDE job sourced from
# the repository. Re-run after every DAG change: `cde repository sync` alone
# does not refresh an already-registered DAG.

set -euo pipefail

REPO_NAME="${REPO_NAME:-rsingh-casa-alb-pipeline}"
DAG_JOB_NAME="${DAG_JOB_NAME:-rsingh-casa-alb-orchestration}"
DAG_PATH="cde/dags/casa_alm_dag.py"
JOB_PREFIX="${JOB_PREFIX:-rsingh-casa-alb-}"

# In-flight runs keep the old DAG, and a redeploy under them leaves tasks half on each version.
in_flight="$(cde run list --filter 'status[in]running,starting' --filter "job[rlike]^${JOB_PREFIX}" \
  | python3 -c "import json,sys; print(' '.join(f\"{r['job']}#{r['id']}\" for r in json.load(sys.stdin) or []))")"
if [[ -n "${in_flight}" ]]; then
  echo "Runs in flight, not redeploying: ${in_flight}"
  echo "Wait for them to end (or: python scripts/platform.py stop --only cde), then re-run."
  exit 1
fi

cde repository sync --name "${REPO_NAME}"

if cde job describe --name "${DAG_JOB_NAME}" &>/dev/null; then
  type="$(cde job describe --name "${DAG_JOB_NAME}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('type'))")"
  if [[ "${type}" != "airflow" ]]; then
    echo "==> ${DAG_JOB_NAME} has type ${type}; recreating as airflow"
    cde job delete --name "${DAG_JOB_NAME}"
  else
    echo "==> Updating ${DAG_JOB_NAME}"
    cde job update --name "${DAG_JOB_NAME}" --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
    echo "Give Airflow ~30s to re-parse before triggering."
    exit 0
  fi
fi

echo "==> Creating ${DAG_JOB_NAME}"
cde job create --name "${DAG_JOB_NAME}" --type airflow --dag-file "${DAG_PATH}" --mount-1-resource "${REPO_NAME}"
echo "DAG casa_alm_behavioural_pipeline registered (monthly, 06:00 UTC on the 1st). Run now:"
echo "  cde job run --name ${DAG_JOB_NAME}"

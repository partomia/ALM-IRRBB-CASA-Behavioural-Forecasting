"""
The CAI resources of this project: jobs, model and application, read by
ci/setup_cai.py (creates or adopts them) and ci/trigger_cai_pipeline.py, and
checked against docs/DEMO_RUNBOOK.md by the tests. Pure Python, so the GitHub
runner can import it without cmlapi.

The jobs have no CAI dependencies: monthly, Airflow starts rsingh-casa-alb-monthly-forecast
(cde/dags/casa_alm_dag.py); on a push, GitHub Actions starts GITHUB_CHAIN one by
one through the CAI API v2. A job run ignores arguments, so both pass settings
through the run's environment.

The GitHub chain is a candidate check, not a publish: the forecast job runs with
CASA_DRY_RUN=1, so it backtests and forecasts with TimesFM but writes no table.
The monthly DAG publishes.
"""
from __future__ import annotations

CAI_PROJECT_NAME = "rsingh-casa-alb"
GIT_URL = "https://github.com/partomia/ALM-IRRBB-CASA-Behavioural-Forecasting"
RUNTIME = "docker.repository.cloudera.com/cloudera/cdsw/ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5"

SYNC_JOB = "rsingh-casa-alb-sync-code"
MONTHLY_JOB = "rsingh-casa-alb-monthly-forecast"
BACKFILL_JOB = "rsingh-casa-alb-backfill-alco-history"
JOBS = [
    # name, script, vCPU, memory GB, GPUs, timeout minutes (the runbook's job table)
    {"name": SYNC_JOB, "script": "cai/jobs/sync_code.py", "cpu": 2, "memory": 8, "gpu": 0, "timeout": 60},
    {"name": MONTHLY_JOB, "script": "cai/jobs/monthly_forecast.py", "cpu": 4, "memory": 16, "gpu": 0, "timeout": 60},
    # one-off, started by hand; CASA_BACKFILL_MONTHS in the run's environment (default 6)
    {"name": BACKFILL_JOB, "script": "cai/jobs/backfill_alco_history.py", "cpu": 4, "memory": 16, "gpu": 0,
     "timeout": 240},
]
# CPU only: on federal GPUs cannot be scheduled from these projects, and more than
# 4 vCPU / 16 GB sits in ENGINE_SCHEDULING. TimesFM runs on the CPU torch wheels.
BY_NAME = {j["name"]: j for j in JOBS}
DEADLINE_MIN = 90                     # per job run, as the DAG (cde/dags/casa_alm_dag.py CAI_DEADLINE_MIN)

MODEL = {"name": "rsingh-casa-alb-model", "file": "cai/model/predict.py", "cpu": 2, "memory": 8, "gpu": 0,
         "description": "CASA what-if: TimesFM 52-week forecast, core split and SLS slotting under a stress"}
APP = {"name": "rsingh-casa-alb-alco", "subdomain": "rsingh-casa-alb-alco", "script": "app/run.py",
       "cpu": 2, "memory": 4, "description": "Streamlit ALCO app over the published month-end runs"}

GITHUB_CHAIN = [SYNC_JOB, MONTHLY_JOB]


def github_env(name: str, sha: str) -> dict:
    """The environment of one job run in the GitHub chain."""
    if name == SYNC_JOB:
        return {"EXPECTED_GIT_SHA": sha[:12]}
    return {"CASA_TRIGGERED_BY": "github", "CASA_DRY_RUN": "1"}

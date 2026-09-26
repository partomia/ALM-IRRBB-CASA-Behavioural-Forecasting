"""
Airflow DAG (CDE): month-end CASA behavioural forecasting pipeline.

  generate_cbs_bronze -> validate_bronze -> build_silver_daily -> build_gold_weekly   (CDE Spark)
    -> cai_monthly_forecast                                                        (CAI Job via API v2)

The CAI step triggers the Cloudera AI job `cai/jobs/monthly_forecast.py` and
waits for it, so one DAG run goes from core banking extract to the ALCO
tables. It needs these Airflow Variables (CDE Airflow UI > Admin > Variables):
  CASA_CAI_HOST        https://ml-xxxx.<env>.cloudera.site  (CAI workbench URL)
  CASA_CAI_PROJECT_ID  project id (from the project URL or API)
  CASA_CAI_JOB_ID      id of the monthly forecast job
  CASA_CAI_API_KEY     CAI API v2 key (User settings > API keys)
If CASA_CAI_HOST is not set the CAI step is skipped, so the Spark part can be
tested on its own.

Scheduled monthly at 06:00 UTC on the 1st: the run is the ALCO run as of the
month-end just closed. Manual trigger (Trigger DAG w/ config): {"as_of":
"2026-08-31"}; empty = yesterday / latest week.

Job names must match cde/scripts/deploy_jobs.sh exactly (CDEJobRunOperator
fails with 404 "job not found" otherwise).
"""

import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

JOB_PREFIX = "rsingh-casa-alb"
DB_PREFIX = "rsingh_casa_alb"
# Scheduled runs: month-end of the interval just closed; manual runs: the as_of param.
AS_OF = ("{{ params.as_of or ((data_interval_end - macros.timedelta(days=1)).strftime('%Y-%m-%d') "
         "if dag_run.run_type == 'scheduled' else '') }}")
TERMINAL_OK = {"succeeded"}
TERMINAL_BAD = {"failed", "stopped", "timedout"}
MONTHLY = "0 6 1 * *"


def trigger_cai_job(as_of: str, **_):
    host = Variable.get("CASA_CAI_HOST", default_var="").rstrip("/")
    if not host:
        raise AirflowSkipException("CASA_CAI_HOST not set: skipping the CAI forecast step")
    project = Variable.get("CASA_CAI_PROJECT_ID")
    job = Variable.get("CASA_CAI_JOB_ID")
    headers = {"Authorization": f"Bearer {Variable.get('CASA_CAI_API_KEY')}", "Content-Type": "application/json"}
    args = "--triggered-by airflow" + (f" --as-of {as_of}" if as_of else "")
    # A job run ignores "arguments" (the job's own are used); the environment map is applied.
    env = {"CASA_TRIGGERED_BY": "airflow", "CASA_AS_OF": as_of or ""}
    url = f"{host}/api/v2/projects/{project}/jobs/{job}/runs"
    resp = requests.post(url, json={"arguments": args, "environment": env}, headers=headers, timeout=60)
    resp.raise_for_status()
    run_id = resp.json()["id"]
    print(f"Started CAI job run {run_id} with environment: {env}")

    deadline = time.time() + 60 * 60
    while time.time() < deadline:
        time.sleep(30)
        r = requests.get(f"{url}/{run_id}", headers=headers, timeout=60)
        r.raise_for_status()
        status = str(r.json().get("status", "")).lower().replace("engine_", "")
        print(f"CAI run {run_id}: {status}")
        if status in TERMINAL_OK:
            return run_id
        if status in TERMINAL_BAD:
            raise AirflowException(f"CAI job run {run_id} ended with status {status}")
    raise AirflowException(f"CAI job run {run_id} did not finish within 60 minutes")


default_args = {
    "owner": "casa-alm",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="casa_alm_behavioural_pipeline",
    description="CBS extract -> bronze/silver/gold (CDE) -> TimesFM ALCO run (CAI)",
    default_args=default_args,
    schedule_interval=MONTHLY,
    # the first interval (Sep) closes 1 Oct 06:00; an earlier start would fire a run on deploy
    start_date=datetime(2026, 9, 1, 6),
    catchup=False,
    is_paused_upon_creation=False,
    params={"as_of": ""},
    tags=["casa", "alm", "irrbb", "iceberg"],
) as dag:

    generate = CDEJobRunOperator(
        task_id="generate_cbs_bronze",
        job_name=f"{JOB_PREFIX}-generate-cbs-bronze",
        # run-time args replace the job's own args, so repeat --db-prefix
        overrides={"spark": {"args": ["--db-prefix", DB_PREFIX, "--as-of", AS_OF]}},
        wait=True,
    )
    validate = CDEJobRunOperator(task_id="validate_bronze", job_name=f"{JOB_PREFIX}-validate-bronze", wait=True)
    silver = CDEJobRunOperator(task_id="build_silver_daily", job_name=f"{JOB_PREFIX}-build-silver-daily", wait=True)
    gold = CDEJobRunOperator(task_id="build_gold_weekly", job_name=f"{JOB_PREFIX}-build-gold-weekly", wait=True)
    forecast = PythonOperator(
        task_id="cai_monthly_forecast",
        python_callable=trigger_cai_job,
        op_kwargs={"as_of": AS_OF},
        retries=0,
    )

    generate >> validate >> silver >> gold >> forecast

# Build plan — CASA Behavioural Forecasting for ALM & IRRBB

Demo of a governed data + modelling pipeline on Cloudera: synthetic core
banking balances land in Iceberg, get rolled up to weekly segment balances,
and a zero-shot TimesFM 2.5 forecast gives the core / non-core split used in
the RBI Structural Liquidity Statement (SLS) and IRRBB reporting.

Not a validated regulatory model. It covers volume behaviour only.

## Platform mapping

CDP environment **federal** since 3 Oct 2026 (go01 before, left as it was): CDW Impala
`coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site` (443, HTTP
`cliservice`, LDAP), the shared CDE vcluster (queue 27 vCPU / ~110 GB), CAI workbench
`federal-cml-ws` (CPU only). Schedule: monthly 06:00 UTC on the 1st. Neighbours on the
vcluster: Mule 20:30, Spend 21:00, Churn 22:00, Collections 00:30 UTC daily
(`cde job list`, 3 Oct 2026).

| Layer | Service | What runs there |
|---|---|---|
| Ingest + medallion | CDE (Spark 3, Iceberg) | generate bronze, validate, silver daily, gold weekly |
| Orchestration | CDE Airflow | DAG chains the Spark jobs, then triggers the CAI forecast job |
| Forecast | CAI Job | TimesFM backtest, 52-week forecast, core split, SLS buckets |
| What-if API | CAI Model Deployment | `predict.py`, stress scenarios on demand |
| SQL + reports | CDW Impala (Hue) | report queries, Iceberg time travel |
| Demo UI | CAI Application (Streamlit) | fan charts, core split, SLS, stress, time travel |

Names: databases `rsingh_casa_alb_{bronze,silver,gold,ref}`, CDE resources
`rsingh-casa-alb-*`, CAI project `rsingh-casa-alb` with jobs
`rsingh-casa-alb-{sync-code,monthly-forecast,backfill-alco-history}`, model
`rsingh-casa-alb-model`, application `rsingh-casa-alb-alco` (`ci/cai_jobs.py`).

## Data flow

```
CDE  generate_cbs_bronze   -> rsingh_casa_alb_bronze.cbs_daily_balance (account-level, daily)
                             rsingh_casa_alb_ref.casa_segment_map
CDE  validate_bronze       -> gate (fails the DAG on bad data)
CDE  build_silver_daily    -> rsingh_casa_alb_silver.casa_daily_balance (segment-level, daily)
CDE  build_gold_weekly     -> rsingh_casa_alb_gold.casa_weekly_balance (MERGE, snapshot per run)
CAI  monthly_forecast      -> rsingh_casa_alb_gold.casa_behavioural_split / casa_forecast_path /
                             casa_sls_buckets / casa_backtest_metrics / casa_model_run
                             (keyed by as_of_date, so every ALCO run is kept)
CDW  Hue reports, time travel     CAI  Streamlit app + model endpoint
```

## Phases

- [x] **0. Scaffold** — layout, config (`config/casa.yaml`, `config/policy.yaml`), requirements, venv.
- [x] **1. Bronze** — synthetic CBS daily balances for ~14 segments from FY2021-22, with
  trend, salary cycle, quarter-end spikes, a 2022 rate-hike outflow; history is
  prefix-stable so a later as-of date only adds weeks.
- [x] **2. Silver + gold** — validation gate, daily segment totals, weekly roll-up
  (complete Sat–Fri weeks only), MERGE into gold. Verified locally on Spark + Iceberg.
- [x] **3. Core logic** (`casa/`) — TimesFM wrapper, core split with caps, SLS slotting,
  rolling backtest, storage backends (Impala / parquet). Unit tests with a stub model.
- [x] **4. Monthly job** — CAI job + backfill drill (three month-end as-of dates) for the
  ALCO history and time travel.
- [x] **5. Model endpoint** — `predict.py` with stress input, test script.
- [x] **6. Streamlit app** — CAI Application launcher + Dockerfile for running elsewhere
  from parquet or Impala.
- [x] **7. Orchestration + docs** — Airflow DAG, CDE deploy scripts, Hue SQL, README,
  demo runbook.
- [x] **8. Scripted Cloudera setup, federal config** (decisions 1-6): `ci/cai_jobs.py`,
  `ci/setup_cai.py`, `ci/trigger_cai_pipeline.py`, `cai/jobs/sync_code.py`,
  `cde/scripts/set_airflow_variables.py`, `.github/workflows/ci.yml` (`test` +
  `cai-pipeline`), DAG paused on creation with a tolerant CAI poll, federal Impala host,
  CDE sizing overridable, one process per backfill month-end.
- [ ] **9. Federal, from scratch**: CDE jobs and the chain for one as_of, CAI project and
  first CPU run, model and app, Data Visualization, history backfill, Airflow Variables,
  DAG (paused, then unpaused), GitHub -> CAI check.

## Decisions

1. **CAI is set up over the API v2 from the laptop**, as in Churn and Collections.
   `ci/cai_jobs.py` holds every CAI resource (60-min job timeout, not the UI's 15),
   `ci/setup_cai.py` creates or adopts them by name and corrects drift (`--dry-run`,
   `--no-serving` until a run is published, `--sync`, `--dataviz`), and
   `cde/scripts/set_airflow_variables.py` sets only the four `CASA_CAI_*` Variables
   through the vcluster's Airflow API. A job run ignores arguments, so Airflow and
   GitHub pass `CASA_AS_OF`, `CASA_TRIGGERED_BY`, `CASA_DRY_RUN` in the run's environment.
2. **`rsingh-casa-alb-sync-code` replaces `git pull` in a session, and GitHub checks each
   push on CAI.** On a push to main, `cai-pipeline` (after `test`) runs sync-code to the
   pushed commit, then the monthly job with `CASA_DRY_RUN=1`: real TimesFM backtest and
   forecast, no table written. The monthly DAG publishes. The trigger retries GETs (the
   federal endpoint drops TLS now and then) and never retries a POST (a second run).
3. **The DAG registers paused** (`is_paused_upon_creation=True`): an unpaused
   registration, or unpausing later, runs the latest closed interval at once. Its CAI
   poll survives a dropped connection (RequestException and 5xx retried, up to 10 polls
   in a row; 4xx re-raised), since a task retry would start a second CAI run.
4. **CAI runs on CPU on federal.** GPUs cannot be scheduled from these projects, and more
   than 4 vCPU / 16 GB sits in `ENGINE_SCHEDULING`. TimesFM 2.5 (200M) already ran on the
   CPU torch wheels on go01; jobs are 4 vCPU / 16 GB, the model 2 vCPU / 8 GB. Laptop CPU
   at 4 threads (M3 Pro): model load 4.3 s, backtest + 52-week forecast for 14 segments
   4.2 s, peak RSS 2.0 GiB.
5. **CDE jobs are sized for the shared federal queue** (27 vCPU / ~110 GB): 2-core / 4 GB
   driver and executors, 1 min / 2 initial / 4 max, each overridable in `deploy_jobs.sh`.
   `cde job run --wait` can return early, so the drill polls `cde run describe`.
6. **The backfill runs each month-end in its own process** (as Collections, its decision
   12): a killed CAI engine can be reported as `ENGINE_SUCCEEDED`, so a child that exits
   non-zero or is killed fails the job by name, and every period is checked in Impala
   afterwards. Laptop: 7 s per month-end, 43 s for six; CAI: 24 s per month-end, 145 s for
   six.
7. **The Data Visualization dashboard is code** (as Churn and Collections):
   `dataviz/build_dashboard.py` creates the views, the connection and workspace
   `rsingh-casa-alb` if missing, writes `dataviz/casa_alco_dashboard.json` with fixed
   UUIDs and imports it into that workspace (the form field `workspace=<name>`; without it
   the import lands in Private). Views carry `is_latest`, so the monthly run refreshes the
   dashboard without a rebuild.

## Method (demo policy, see `config/policy.yaml`)

- core share (model) = min over 52 weeks of the P10 path ÷ today's balance
- core share = min(model share, BCBS category cap); non-core = the rest
- SLS: run-off to the P10 envelope spread over Day 1 – 1 year; cap excess in Day 1;
  core spread over 1–3y / 3–5y / 5y+ with category weights, average maturity
  checked against the BCBS cap
- trust number: rolling-origin backtest, share of actual weeks above P10 (target ≈ 90%)
  and inside P10–P90 (target ≈ 80%)

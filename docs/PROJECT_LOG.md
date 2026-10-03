# Project log

## 2026-09-26: initial build on go01

Phases 0-7 of `PLAN.md` in one day (see `git log`). On go01: CDE jobs and the DAG on
the vcluster, CAI project `alm-casa-timesfm` with job `casa-monthly-forecast`, model
`casa-behavioural-model`, application `CASA ALCO`. go01's ALCO run as of 31 Aug 2026
(README screenshot, storage Impala): total CASA ₹123,446 cr, core ₹95,868 cr (77.7%),
non-core ₹27,578 cr, cap binding in 12 of 14 segments, backtest trust number 96%.

## 2026-10-03: move to the federal environment (Cursor, Opus 5.5)

Moved end to end from go01 to federal, the same way as Customer-Churn-Prediction and
Collections-Delinquency-Roll-Forward-Prediction (its PLAN decisions 7-12). go01 resources
were left as they were.

### Phase 8: scripted Cloudera setup, federal config (code)

- Read-only checks on federal first: `cde job list` shows the scheduled DAGs Mule 20:30,
  Spend 21:00, Churn 22:00, Collections 00:30 UTC (gdl unscheduled) and no
  `rsingh-casa-alb` job, so the monthly `0 6 1 * *` stays; runtime
  `ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5` `ENABLED` in the CAI API;
  no `rsingh-casa-alb` project; Impala LDAP login works (3.2 s) and no `rsingh_casa*`
  database exists.
- `config/casa.yaml`: Impala host `coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site`.
- `deploy_jobs.sh`: 2-core / 4 GB driver and executors, 1 / 2 / 4 with
  `--initial-executors`, each overridable by environment variable; exits if the
  python-env is not ready. `backfill_drill.sh` polls `cde run describe` instead of `--wait`.
- DAG: `is_paused_upon_creation=True`; the CAI run gets the environment only (a job run
  ignores `arguments`); the poll retries RequestException and 5xx (up to 10 in a row),
  re-raises 4xx; deadline 90 min.
- New: `ci/cai_jobs.py`, `ci/setup_cai.py`, `ci/trigger_cai_pipeline.py`,
  `cai/jobs/sync_code.py`, `cde/scripts/set_airflow_variables.py`,
  `.github/workflows/ci.yml` (`test` + `cai-pipeline`), `requirements-ci.txt`,
  `CASA_DRY_RUN` in `monthly_forecast.py`, `CASA_BACKFILL_MONTHS` and one process per
  month-end in `backfill_alco_history.py`, `tests/test_ci_trigger.py`,
  `tests/test_orchestration.py`. `pytest -q`: 70 passed (36 before).
- `run_cde_local.py export` printed its target relative to the repo, which fails for a
  `--parquet-dir` outside it (found in the Collections move); fixed.
- Laptop run of the local pipeline in a scratch warehouse, as of 2026-09-30: the four CDE
  jobs plus export in 28 s (bronze `cbs_daily_balance` 2,535,343 rows, 2021-04-03 ..
  2026-09-30, 2,007 days; silver 28,098 segment-days; gold 4,004 segment-weeks). TimesFM on
  the laptop CPU (M3 Pro, `OMP_NUM_THREADS=4`): model load 4.3 s, backtest + forecast
  4.2 s, 9.2 s wall, peak RSS 2.0 GiB. Backfill of six month-ends, one process each:
  7 s per month-end, 43 s in all.
- Laptop ALCO runs (parquet):

  | As of | Balance (cr) | Core (cr) | Core % | Cap binds | P10 hit | P10-P90 |
  |---|---|---|---|---|---|---|
  | 03-31 | 125,232.56 | 96,329.23 | 76.9% | 12 / 14 | 92.6% | 74.5% |
  | 04-30 | 122,864.05 | 95,480.01 | 77.7% | 13 / 14 | 97.4% | 87.1% |
  | 05-31 | 121,569.61 | 94,383.52 | 77.6% | 12 / 14 | 96.4% | 80.6% |
  | 06-30 | 121,798.12 | 94,816.10 | 77.8% | 13 / 14 | 92.5% | 77.6% |
  | 07-31 | 121,406.32 | 94,545.38 | 77.9% | 12 / 14 | 96.7% | 84.9% |
  | 08-31 | 123,446.31 | 95,868.41 | 77.7% | 12 / 14 | 95.6% | 81.7% |
  | 09-30 | 124,781.88 | 96,489.14 | 77.3% | 12 / 14 | 92.9% | 76.9% |

  31 Aug matches go01 to the crore (₹123,446 / ₹95,868 cr, 12 of 14, 96%): the generator
  is prefix-stable, so federal should reproduce these numbers.

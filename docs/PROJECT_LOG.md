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

### Phase 9: federal deployment (measured)

- `deploy_jobs.sh`: 13:02:53-13:05:47 UTC (2 min 55 s; the python-env was ready after about
  2.5 min). Jobs have 2-core / 4 GB driver and executors, initial 2, min 1, max 4.
- CDE chain for as of 2026-09-30, each run polled with `cde run describe`; no queue
  rejection:

  | Job | Run | Time |
  |---|---|---|
  | `rsingh-casa-alb-generate-cbs` | 262 | 223 s |
  | `rsingh-casa-alb-validate-bronze` | 263 | 86 s |
  | `rsingh-casa-alb-silver` | 264 | 85 s |
  | `rsingh-casa-alb-gold` | 265 | 85 s |

  479 s in all. validate_bronze passed: 2,535,343 rows, 2,007 days, 497 duplicate
  account-days. Impala row counts: bronze `cbs_daily_balance` 2,535,343, ref
  `casa_segment_map` 14, silver 28,098, gold `casa_weekly_balance` 4,004 (14 segments x
  286 weeks, 2021-04-09 .. 2026-09-25), the same as the laptop. Gold has one Iceberg
  snapshot, 2639762512591380740.
- `ci/setup_cai.py --no-serving --sync`: project `rsingh-casa-alb` (id in `.env` as `CASA_CAI_PROJECT_ID`),
  jobs `rsingh-casa-alb-sync-code` (`l74p-bnnz-teu2-6ttm`),
  `rsingh-casa-alb-monthly-forecast` (`1vfh-7l8k-5xwc-6liu`),
  `rsingh-casa-alb-backfill-alco-history` (`sv33-isyn-xoli-bsg4`). The first sync run
  installed the requirements: 8.4 min running.
- First monthly forecast on CAI (as of 2026-09-30, `triggered_by=manual`, 4 vCPU / 16 GB,
  no GPU): 2.4 min scheduling, 49 s running, including the first TimesFM download from
  Hugging Face. run_id `20260930-930b419b`, model revision
  `1d952420fba87f3c6dee4f240de0f1a0fbc790e3`, 286 history weeks. Numbers identical to the
  laptop: ₹124,781.88 / ₹96,489.14 cr (77.3%), 12 of 14, P10 hit 92.9%, P10-P90 76.9%,
  MAPE 4.15%. Rows: split 14, path 728, SLS 143, backtest 56, model_run 1.
- Serving (`ci/setup_cai.py`): model `rsingh-casa-alb-model`
  (`462339e7-bb83-44a5-aa26-8d3ac4e1038c`), build 13:20:53-13:24:00, deployment 2 vCPU /
  8 GB, 1 replica, deployed 13:24:42; no CPU-group stall, so the workbench was not resized.
  Endpoint from the laptop: 2.05 / 1.94 / 1.92 s per call. SA_RETAIL_URBAN base: balance
  ₹24,540.1 cr, model core 94.5%, core 90.0% (cap binds); stress -10% over 8 weeks: ₹22,086.1
  cr, model core 89.5%, cap not binding. App `rsingh-casa-alb-alco` (`bqnu-o5km-6kii-kuqv`)
  running; its URL answers 302 to the CAI login.
- Data Visualization (`docs/DATAVIZ.md`): views (10 statements), connection
  `rsingh-casa-alb-impala`, workspace `rsingh-casa-alb`, 5 datasets, 14 visuals, dashboard 219.
  File build 29.6 s, import 1.9 s, `--verify` 14 of 14 visuals ok in 18 s (total 124,782,
  core 96,489, 77.3%, 12 binds). The first import landed in Private: the import ignores
  the file's workspace; sending the form field `workspace=<name>` fixes it.
- Backfill on CAI, one process per month-end: one period (`CASA_BACKFILL_MONTHS=1`, 31 Aug)
  29.9 s running after 29 s scheduling; six periods (31 Mar .. 31 Aug) 145 s running, about
  24 s each. Every as of has 14 split rows, 728 path rows and one model_run row in Impala
  (`triggered_by=backfill`). Federal against go01, as of 31 Aug 2026:

  | | go01 | federal |
  |---|---|---|
  | Total CASA | ₹123,446 cr | ₹123,446.31 cr |
  | Core | ₹95,868 cr (77.7%) | ₹95,868.41 cr (77.66%) |
  | Non-core | ₹27,578 cr | ₹27,577.91 cr |
  | Cap binds | 12 of 14 | 12 of 14 |
  | Trust (P10 hit) | 96% | 95.6% |

  All seven runs on federal match the laptop table above.
- Airflow: `set_airflow_variables.py` created the four `CASA_CAI_*` Variables;
  `deploy_dag.sh` registered `rsingh-casa-alb-orchestration` in 9 s, paused
  (`pausedUponCreation`, no runs). Unpaused at 13:36:51 UTC: Airflow started
  `scheduled__2026-09-01T06:00:00+00:00` (as of 2026-09-30) at once:

  | Task | Start (UTC) | Time |
  |---|---|---|
  | `generate_cbs_bronze` | 13:37:12 | 223 s |
  | `validate_bronze` | 13:41:09 | 82 s |
  | `build_silver_daily` | 13:42:44 | 72 s |
  | `build_gold_weekly` | 13:44:09 | 73 s |
  | `cai_monthly_forecast` | 13:45:36 | 63 s |

  9 min 46 s in all, success, no retries. The CAI run `pq9yeu33iccmvs8s` got
  `CASA_AS_OF=2026-09-30`, `CASA_TRIGGERED_BY=airflow` in its environment: 14 s scheduling,
  35 s running. Impala has the row: run_id `20260930-d87e0be7`, `triggered_by=airflow`, gold
  snapshot 5017955801739624589 (gold rebuilt, still 4,004 rows), numbers unchanged
  (₹124,781.88 / ₹96,489.14 cr, 12 of 14, P10 hit 92.9%). Next run created after
  2026-11-01 06:00 UTC (not observed).
- Data Visualization after the Airflow run: `--verify` 14 of 14 ok, History and trust shows
  all 7 runs. A verify straight after the run still showed the earlier results: the
  connection caches results for 30 minutes (`docs/DATAVIZ.md`).
- GitHub -> CAI: secrets `CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID` set from `.env`. The push
  of c1ddc4d ran `test` (65 s) and `cai-pipeline` (78 s), both green: sync-code
  `823jji5tkk19tdli` to c1ddc4df2b2d (18 s scheduling, 3 s running, requirements
  unchanged), then the monthly job as a dry run, `bjcshu7d50733kau` (13 s scheduling, 24 s
  running, `triggered_by=github`). No row written: `casa_model_run` still has 7 rows.

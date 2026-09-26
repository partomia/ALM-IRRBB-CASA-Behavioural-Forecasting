# Build plan — CASA Behavioural Forecasting for ALM & IRRBB

Demo of a governed data + modelling pipeline on Cloudera: synthetic core
banking balances land in Iceberg, get rolled up to weekly segment balances,
and a zero-shot TimesFM 2.5 forecast gives the core / non-core split used in
the RBI Structural Liquidity Statement (SLS) and IRRBB reporting.

Not a validated regulatory model. It covers volume behaviour only.

## Platform mapping

Same CDP environment as `partomia/insurance-claim-approval`.

| Layer | Service | What runs there |
|---|---|---|
| Ingest + medallion | CDE (Spark 3, Iceberg) | generate bronze, validate, silver daily, gold weekly |
| Orchestration | CDE Airflow | DAG chains the Spark jobs, then triggers the CAI forecast job |
| Forecast | CAI Job | TimesFM backtest, 52-week forecast, core split, SLS buckets |
| What-if API | CAI Model Deployment | `predict.py`, stress scenarios on demand |
| SQL + reports | CDW Impala (Hue) | report queries, Iceberg time travel |
| Demo UI | CAI Application (Streamlit) | fan charts, core split, SLS, stress, time travel |

Names: databases `rsingh_casa_alb_{bronze,silver,gold,ref}`, CDE resources
`rsingh-casa-alb-*`.

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
- [ ] **7. Orchestration + docs** — Airflow DAG, CDE deploy scripts, Hue SQL, README,
  demo runbook.

## Method (demo policy, see `config/policy.yaml`)

- core share (model) = min over 52 weeks of the P10 path ÷ today's balance
- core share = min(model share, BCBS category cap); non-core = the rest
- SLS: run-off to the P10 envelope spread over Day 1 – 1 year; cap excess in Day 1;
  core spread over 1–3y / 3–5y / 5y+ with category weights, average maturity
  checked against the BCBS cap
- trust number: rolling-origin backtest, share of actual weeks above P10 (target ≈ 90%)
  and inside P10–P90 (target ≈ 80%)

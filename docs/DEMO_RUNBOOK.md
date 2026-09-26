# Demo runbook (about 12 minutes)

Before the demo: DAG run finished, backfill done (6+ ALCO runs), app open,
Hue open on `sql/reports.sql`, endpoint deployed. Warm the vcluster with a job
run 10 minutes before if you plan to show CDE live.

## 1. The question (1 min)

CASA has no contractual maturity. ALM (RBI SLS) and IRRBB (BCBS d368) both need
to know how much of today's balance is core, and for how long. Usually a
spreadsheet study refreshed once a year; here it is refreshed from governed data
every month.

## 2. The pipeline (2 min): CDE Airflow UI

- DAG `casa_alm_behavioural_pipeline`: CBS extract → validation gate → silver →
  gold (Iceberg MERGE) → CAI forecast job via API.
- Point out the validation gate: bad data stops the run before it reaches ALCO.

## 3. ALCO overview (2 min): app, first tab

- Total CASA, core, non-core, number of segments where the cap binds.
- Show the cap: the info box ("the data says 98% is stable, the BCBS cap limits
  it to 90%"). The model and the regulation working together.
- Point at the trust number in the sidebar: in rolling backtests the actual
  balance stayed above the P10 path about 90% of the time.

## 4. One segment (2 min): Segment forecast tab

- `SA_RETAIL_URBAN`: salary cycle and quarter-ends in the history; the band
  widens with the horizon; the dashed line is the core amount after the cap.
- `CA_BANKS_FI`: volatile interbank balances, model core share below the 50% cap,
  so the data decides, not the rule.

## 5. SLS (1 min): Structural liquidity tab

- Run-off (red) in Day 1 to 1 year, cap excess (amber) in Day 1, core (blue) in
  the long buckets. Every bucket adds back to today's balance.

## 6. Stress what-if (2 min): Stress tab

- Select `SA_RETAIL_URBAN` and `SA_HNI`, 8 weeks, 10% cut, run.
- Urban savings: the cap stops binding, the core share drops below 90% and run-off
  moves into near buckets. Scored live by the CAI endpoint.

## 7. Audit and time travel (2 min): History tab + Hue

- Core share by ALCO run: `SA_HNI` balance is down about 18% since March (money
  moving to wealth products) and the model core share hovers just above the 70%
  cap, dipping below it in May. `CA_BANKS_FI` moves above and below its 50% cap.
  ALCO sees the drift month by month instead of once a year.
- Lineage table: model id, Hugging Face revision, and the Iceberg snapshot id of
  the gold table each run read.
- In Hue: `DESCRIBE HISTORY` on `casa_weekly_balance`, then
  `FOR SYSTEM_VERSION AS OF <source_snapshot_id>` to reproduce exactly what a past
  ALCO pack was computed from.

## Honest framing (close)

Volume behaviour only. Repricing maturity, pass-through and rate sensitivity need
further modelling, and a bank's model risk and ALCO process set the final
assumptions. TimesFM is zero-shot: no training, same method every month, easy to
explain.

## If something goes wrong

| Symptom | Fix |
|---|---|
| App: "Could not read ... casa_model_run" | Run the CAI job once; check `CASA_IMPALA_*` |
| Hue shows old data | `INVALIDATE METADATA <table>;` |
| CDE job stuck in `starting` | vcluster scale-up; `cde run describe --id <id>` |
| DAG task 404 "job not found" | Job names in DAG and `deploy_jobs.sh` differ |
| DAG change not picked up | Re-run `./cde/scripts/deploy_dag.sh`, wait 30s |
| Stress tab slow on first click | No endpoint configured: TimesFM loads in the app |

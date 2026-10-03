-- Flat reporting views for the Data Visualization dashboard "CASA ALCO" (docs/DATAVIZ.md).
-- One view per dataset, in the gold database; is_latest = 1 marks the latest ALCO run.
-- Impala has no CREATE OR REPLACE VIEW: drop and create. Run with dataviz/build_dashboard.py --views.

DROP VIEW IF EXISTS rsingh_casa_alb_gold.v_alco_split;
CREATE VIEW rsingh_casa_alb_gold.v_alco_split AS
SELECT s.as_of_date, s.run_id, s.segment_id, s.irrbb_category, s.balance_inr_cr, s.min_p10_inr_cr,
       s.core_share_model, s.core_cap, s.core_share, CAST(s.cap_binding AS INT) AS cap_binds,
       s.core_inr_cr, s.non_core_inr_cr, s.core_avg_maturity_years, s.backtest_p10_hit_rate,
       CASE WHEN s.as_of_date = m.latest THEN 1 ELSE 0 END AS is_latest
FROM rsingh_casa_alb_gold.casa_behavioural_split s
CROSS JOIN (SELECT max(as_of_date) AS latest FROM rsingh_casa_alb_gold.casa_behavioural_split) m;

DROP VIEW IF EXISTS rsingh_casa_alb_gold.v_sls;
CREATE VIEW rsingh_casa_alb_gold.v_sls AS
SELECT b.as_of_date, b.segment_id, b.irrbb_category, b.bucket_order,
       concat(lpad(CAST(b.bucket_order AS STRING), 2, '0'), ' ', b.bucket_label) AS bucket,
       b.component, b.amount_inr_cr,
       CASE WHEN b.as_of_date = m.latest THEN 1 ELSE 0 END AS is_latest
FROM rsingh_casa_alb_gold.casa_sls_buckets b
CROSS JOIN (SELECT max(as_of_date) AS latest FROM rsingh_casa_alb_gold.casa_sls_buckets) m;

DROP VIEW IF EXISTS rsingh_casa_alb_gold.v_model_run;
CREATE VIEW rsingh_casa_alb_gold.v_model_run AS
SELECT r.as_of_date, r.run_id, r.run_ts, r.model_id, r.model_revision, r.history_weeks, r.last_week_end,
       r.source_snapshot_id, r.backtest_p10_hit_rate, r.backtest_band_coverage, r.backtest_mape_p50,
       r.triggered_by,
       CASE WHEN r.as_of_date = m.latest THEN 1 ELSE 0 END AS is_latest
FROM rsingh_casa_alb_gold.casa_model_run r
CROSS JOIN (SELECT max(as_of_date) AS latest FROM rsingh_casa_alb_gold.casa_model_run) m;

DROP VIEW IF EXISTS rsingh_casa_alb_gold.v_forecast_path;
CREATE VIEW rsingh_casa_alb_gold.v_forecast_path AS
SELECT f.as_of_date, f.segment_id, f.horizon_week, f.week_end_date, f.p10, f.p50, f.p90,
       CASE WHEN f.as_of_date = m.latest THEN 1 ELSE 0 END AS is_latest
FROM rsingh_casa_alb_gold.casa_forecast_path f
CROSS JOIN (SELECT max(as_of_date) AS latest FROM rsingh_casa_alb_gold.casa_forecast_path) m;

DROP VIEW IF EXISTS rsingh_casa_alb_gold.v_weekly_balance;
CREATE VIEW rsingh_casa_alb_gold.v_weekly_balance AS
SELECT segment_id, irrbb_category, week_end_date, balance_inr_cr
FROM rsingh_casa_alb_gold.casa_weekly_balance;

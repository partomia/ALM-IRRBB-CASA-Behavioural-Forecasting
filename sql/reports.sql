-- CASA behavioural forecasting: report queries for Hue (CDW Impala).
-- Tables written by Spark (CDE) or another engine may need a metadata refresh first.
INVALIDATE METADATA rsingh_casa_alb_gold.casa_weekly_balance;
REFRESH rsingh_casa_alb_gold.casa_behavioural_split;

-- 1. Latest ALCO run: core / non-core by IRRBB category
WITH latest AS (SELECT MAX(as_of_date) AS d FROM rsingh_casa_alb_gold.casa_behavioural_split)
SELECT irrbb_category,
       ROUND(SUM(balance_inr_cr), 0)                        AS balance_cr,
       ROUND(SUM(core_inr_cr), 0)                           AS core_cr,
       ROUND(SUM(non_core_inr_cr), 0)                       AS non_core_cr,
       ROUND(SUM(core_inr_cr) / SUM(balance_inr_cr), 4)     AS core_share,
       SUM(CASE WHEN cap_binding THEN 1 ELSE 0 END)         AS segments_cap_binding
FROM rsingh_casa_alb_gold.casa_behavioural_split s, latest
WHERE s.as_of_date = latest.d
GROUP BY irrbb_category
ORDER BY irrbb_category;

-- 2. Segments where the BCBS cap binds (model says more is stable than the rule allows)
SELECT as_of_date, segment_id, irrbb_category, core_share_model, core_cap, core_share,
       ROUND(balance_inr_cr * (core_share_model - core_share), 1) AS cap_haircut_cr
FROM rsingh_casa_alb_gold.casa_behavioural_split
WHERE cap_binding AND as_of_date = (SELECT MAX(as_of_date) FROM rsingh_casa_alb_gold.casa_behavioural_split)
ORDER BY cap_haircut_cr DESC;

-- 3. Structural Liquidity Statement: CASA outflows by RBI time bucket, latest run
SELECT bucket_order, bucket_code, bucket_label,
       ROUND(SUM(CASE WHEN component = 'runoff'     THEN amount_inr_cr ELSE 0 END), 1) AS runoff_cr,
       ROUND(SUM(CASE WHEN component = 'cap_excess' THEN amount_inr_cr ELSE 0 END), 1) AS cap_excess_cr,
       ROUND(SUM(CASE WHEN component = 'core'       THEN amount_inr_cr ELSE 0 END), 1) AS core_cr,
       ROUND(SUM(amount_inr_cr), 1)                                                     AS total_cr
FROM rsingh_casa_alb_gold.casa_sls_buckets
WHERE as_of_date = (SELECT MAX(as_of_date) FROM rsingh_casa_alb_gold.casa_sls_buckets)
GROUP BY bucket_order, bucket_code, bucket_label
ORDER BY bucket_order;

-- 4. Trust number and lineage for every ALCO run
SELECT as_of_date, run_id, run_ts, model_id, model_revision, history_weeks,
       backtest_p10_hit_rate, backtest_band_coverage, source_snapshot_id, triggered_by
FROM rsingh_casa_alb_gold.casa_model_run
ORDER BY as_of_date DESC;

-- 5. How the core share moved across ALCO runs
SELECT as_of_date, segment_id, core_share_model, core_share, cap_binding
FROM rsingh_casa_alb_gold.casa_behavioural_split
WHERE segment_id IN ('SA_HNI', 'CA_BANKS_FI', 'SA_RETAIL_URBAN')
ORDER BY segment_id, as_of_date;

-- 6. Iceberg time travel: the gold input exactly as a past ALCO run saw it
DESCRIBE HISTORY rsingh_casa_alb_gold.casa_weekly_balance;

-- use a snapshot_id from the history above, or casa_model_run.source_snapshot_id
SELECT segment_id, COUNT(*) AS weeks, MAX(week_end_date) AS last_week
FROM rsingh_casa_alb_gold.casa_weekly_balance FOR SYSTEM_VERSION AS OF 1234567890123456789
GROUP BY segment_id ORDER BY segment_id;

-- or by wall-clock time (must be after the first snapshot's creation_time)
SELECT segment_id, MAX(week_end_date) AS last_week
FROM rsingh_casa_alb_gold.casa_weekly_balance FOR SYSTEM_TIME AS OF now() - INTERVAL 1 MINUTES
GROUP BY segment_id ORDER BY segment_id;

-- 7. Forecast band for one segment, latest run
SELECT week_end_date, ROUND(p10, 1) AS p10, ROUND(p50, 1) AS p50, ROUND(p90, 1) AS p90
FROM rsingh_casa_alb_gold.casa_forecast_path
WHERE segment_id = 'SA_RETAIL_URBAN'
  AND as_of_date = (SELECT MAX(as_of_date) FROM rsingh_casa_alb_gold.casa_forecast_path)
ORDER BY week_end_date;

"""Column definitions for the gold output tables written by the monthly job.

Every output table is keyed by as_of_date: a rerun for the same month
replaces that month only, so the history of ALCO runs stays queryable.
"""

OUTPUT_TABLES = {
    "casa_behavioural_split": [
        ("as_of_date", "DATE"), ("run_id", "STRING"), ("segment_id", "STRING"), ("irrbb_category", "STRING"),
        ("balance_inr_cr", "DOUBLE"), ("min_p10_inr_cr", "DOUBLE"), ("core_share_model", "DOUBLE"),
        ("core_cap", "DOUBLE"), ("core_share", "DOUBLE"), ("cap_binding", "BOOLEAN"),
        ("core_inr_cr", "DOUBLE"), ("non_core_inr_cr", "DOUBLE"),
        ("core_avg_maturity_years", "DOUBLE"), ("max_avg_maturity_years", "DOUBLE"),
        ("backtest_p10_hit_rate", "DOUBLE"), ("created_at", "TIMESTAMP"),
    ],
    "casa_forecast_path": [
        ("as_of_date", "DATE"), ("run_id", "STRING"), ("segment_id", "STRING"),
        ("horizon_week", "INT"), ("week_end_date", "DATE"),
        ("mean", "DOUBLE"), ("p10", "DOUBLE"), ("p50", "DOUBLE"), ("p90", "DOUBLE"),
    ],
    "casa_sls_buckets": [
        ("as_of_date", "DATE"), ("run_id", "STRING"), ("segment_id", "STRING"), ("irrbb_category", "STRING"),
        ("bucket_order", "INT"), ("bucket_code", "STRING"), ("bucket_label", "STRING"),
        ("component", "STRING"), ("amount_inr_cr", "DOUBLE"),
    ],
    "casa_backtest_metrics": [
        ("as_of_date", "DATE"), ("run_id", "STRING"), ("segment_id", "STRING"), ("cutoff_week_end", "DATE"),
        ("horizon_weeks", "INT"), ("p10_hit_rate", "DOUBLE"), ("band_coverage", "DOUBLE"), ("mape_p50", "DOUBLE"),
    ],
    "casa_model_run": [
        ("as_of_date", "DATE"), ("run_id", "STRING"), ("run_ts", "TIMESTAMP"), ("model_id", "STRING"),
        ("model_revision", "STRING"), ("horizon_weeks", "INT"), ("n_segments", "INT"), ("history_weeks", "INT"),
        ("last_week_end", "DATE"), ("source_table", "STRING"), ("source_snapshot_id", "STRING"),
        ("backtest_p10_hit_rate", "DOUBLE"), ("backtest_band_coverage", "DOUBLE"), ("backtest_mape_p50", "DOUBLE"),
        ("policy_json", "STRING"), ("triggered_by", "STRING"),
    ],
}

#!/usr/bin/env python3
"""
CAI Job: month-end ALCO run.

Reads <prefix>_gold.casa_weekly_balance, backtests TimesFM, forecasts 52 weeks
per segment, applies the IRRBB caps and SLS slotting, and writes the gold
output tables for that as-of date (a rerun replaces the same month only).

CAI Job settings: Script cai/jobs/monthly_forecast.py, Arguments e.g.
  --as-of 2026-08-31            (default: latest complete week in gold)
  --backend parquet             (default: storage.backend in config/casa.yaml)
  --triggered-by airflow
Env: CASA_IMPALA_HOST / CASA_IMPALA_USER / CASA_IMPALA_PASSWORD, HF_HOME.
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def _repo_root() -> Path:
    # CAI can run job scripts inside an IPython kernel, where __file__ is undefined.
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from casa.model import NaiveQuantileModel, load_model  # noqa: E402
from casa.pipeline import portfolio_summary, run_monthly  # noqa: E402
from casa.storage import get_storage  # noqa: E402


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Month-end CASA behavioural forecast")
    # Airflow passes these as job-run environment variables (CASA_AS_OF, CASA_TRIGGERED_BY).
    p.add_argument("--as-of", default=os.environ.get("CASA_AS_OF") or None,
                   help="YYYY-MM-DD; default latest week in gold")
    p.add_argument("--backend", default=None, choices=["impala", "parquet"])
    p.add_argument("--triggered-by",
                   default=os.environ.get("CASA_TRIGGERED_BY") or os.environ.get("JOB_TRIGGERED_BY", "cai-job"))
    p.add_argument("--naive-model", action="store_true", help="skip TimesFM (smoke test only)")
    p.add_argument("--dry-run", action="store_true", help="compute but do not write")
    args, _ = p.parse_known_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    as_of = datetime.strptime(args.as_of, "%Y-%m-%d").date() if args.as_of else None
    storage = get_storage(args.backend)
    model = NaiveQuantileModel() if args.naive_model else load_model()
    out = run_monthly(storage, model, as_of=as_of, triggered_by=args.triggered_by,
                      model_id="naive-random-walk" if args.naive_model else None, write=not args.dry_run)

    run = out["casa_model_run"].iloc[0]
    split = out["casa_behavioural_split"]
    print(f"\nRun {run.run_id}  as of {run.as_of_date}  storage={storage.name}")
    if run.backtest_p10_hit_rate is not None:
        print(f"Backtest: actual balance stayed above P10 in {run.backtest_p10_hit_rate:.0%} of segment-weeks, "
              f"inside P10-P90 in {run.backtest_band_coverage:.0%}")
    cols = ["segment_id", "irrbb_category", "balance_inr_cr", "core_share_model", "core_share", "cap_binding"]
    print(split[cols].sort_values("segment_id").to_string(index=False))
    print()
    print(portfolio_summary(split).to_string(index=False))


if __name__ == "__main__":
    main()

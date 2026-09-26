#!/usr/bin/env python3
"""
Backfill past month-end ALCO runs so the app and Hue have a history to
compare (e.g. last quarter's pack vs today's). Loads TimesFM once and runs
the monthly pipeline for each month-end, using only the weeks available at
that date (gold history is prefix-stable, so this matches what a real run on
that date would have seen).

  python cai/jobs/backfill_alco_history.py --months 6
  python cai/jobs/backfill_alco_history.py --as-of 2026-03-31 2026-06-30 --backend parquet
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

import pandas as pd  # noqa: E402

from casa.model import NaiveQuantileModel, load_model  # noqa: E402
from casa.pipeline import run_monthly  # noqa: E402
from casa.storage import get_storage  # noqa: E402


def month_ends_before(last: date, n: int) -> list[date]:
    out, d = [], date(last.year, last.month, 1) - timedelta(days=1)
    if (last + timedelta(days=1)).month != last.month:  # `last` is itself a month-end
        out.append(last)
    while len(out) < n:
        out.append(d)
        d = date(d.year, d.month, 1) - timedelta(days=1)
    return sorted(out)


def main(argv=None) -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--months", type=int, default=6)
    p.add_argument("--as-of", nargs="*", default=None)
    p.add_argument("--backend", default=None, choices=["impala", "parquet"])
    p.add_argument("--naive-model", action="store_true")
    args, _ = p.parse_known_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    storage = get_storage(args.backend)
    if args.as_of:
        dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in args.as_of)
    else:
        last = pd.to_datetime(storage.read("casa_weekly_balance")["week_end_date"]).max().date()
        dates = month_ends_before(last, args.months)
    model = NaiveQuantileModel() if args.naive_model else load_model()
    for d in dates:
        out = run_monthly(storage, model, as_of=d, triggered_by="backfill",
                          model_id="naive-random-walk" if args.naive_model else None)
        s = out["casa_behavioural_split"]
        print(f"{d}: core {s.core_inr_cr.sum():,.0f} of {s.balance_inr_cr.sum():,.0f} INR cr "
              f"({s.core_inr_cr.sum() / s.balance_inr_cr.sum():.1%}), cap binding in {int(s.cap_binding.sum())} segments")


if __name__ == "__main__":
    main()

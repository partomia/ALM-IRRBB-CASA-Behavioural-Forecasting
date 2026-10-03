#!/usr/bin/env python3
"""
Backfill past month-end ALCO runs so the app and Hue have a history to
compare (e.g. last quarter's pack vs today's). Runs the monthly pipeline for
each month-end, oldest first, using only the weeks available at that date
(gold history is prefix-stable, so this matches what a real run on that date
would have seen).

Each month-end runs cai/jobs/monthly_forecast.py in its own process: memory is
released after every period, and a killed run (out of memory) fails this job by
name instead of ending it silently (a killed CAI engine can still be reported
as ENGINE_SUCCEEDED).

  python cai/jobs/backfill_alco_history.py --months 6
  python cai/jobs/backfill_alco_history.py --as-of 2026-03-31 2026-06-30 --backend parquet

As the CAI job rsingh-casa-alb-backfill-alco-history (ci/cai_jobs.py), a run ignores
arguments: set CASA_BACKFILL_MONTHS in the run's environment (default 6).
"""

import argparse
import logging
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

import pandas as pd  # noqa: E402

from casa.storage import get_storage  # noqa: E402

# the run's own settings must not leak into every period
CHILD_ENV_DROP = ("CASA_AS_OF", "CASA_DRY_RUN", "CASA_TRIGGERED_BY")


def month_ends_before(last: date, n: int) -> list[date]:
    out, d = [], date(last.year, last.month, 1) - timedelta(days=1)
    if (last + timedelta(days=1)).month != last.month:  # `last` is itself a month-end
        out.append(last)
    while len(out) < n:
        out.append(d)
        d = date(d.year, d.month, 1) - timedelta(days=1)
    return sorted(out)


def run_one(d: date, args) -> None:
    """One month-end in a fresh process; a non-zero or killed child fails the backfill by name."""
    cmd = [sys.executable, str(_repo_root() / "cai" / "jobs" / "monthly_forecast.py"),
           "--as-of", d.isoformat(), "--triggered-by", "backfill"]
    if args.backend:
        cmd += ["--backend", args.backend]
    if args.naive_model:
        cmd.append("--naive-model")
    env = {k: v for k, v in os.environ.items() if k not in CHILD_ENV_DROP}
    t = time.time()
    code = subprocess.run(cmd, env=env).returncode
    if code != 0:
        raise SystemExit(f"backfill {d}: monthly_forecast exited {code}"
                         + (" (killed, likely out of memory)" if code < 0 else ""))
    print(f"backfill {d}: done in {time.time() - t:.0f}s", flush=True)


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--months", type=int, default=int(os.environ.get("CASA_BACKFILL_MONTHS") or 6))
    p.add_argument("--as-of", nargs="*", default=None)
    p.add_argument("--backend", default=None, choices=["impala", "parquet"])
    p.add_argument("--naive-model", action="store_true")
    args, _ = p.parse_known_args(argv)  # a Jupyter-kernel job runtime adds -f <kernel.json>
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.as_of:
        dates = sorted(datetime.strptime(d, "%Y-%m-%d").date() for d in args.as_of)
    else:
        weekly = get_storage(args.backend).read("casa_weekly_balance")
        last = pd.to_datetime(weekly["week_end_date"]).max().date()
        dates = month_ends_before(last, args.months)
    print("backfill month-ends:", ", ".join(d.isoformat() for d in dates), flush=True)
    t = time.time()
    for d in dates:
        run_one(d, args)
    print(f"backfill: {len(dates)} month-ends in {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()

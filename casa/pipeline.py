"""Monthly ALCO run: backtest, forecast, core split and SLS slotting for every
segment as of a month-end, written to the gold output tables."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import date, datetime

import numpy as np
import pandas as pd

from casa import backtest as bt
from casa.behavioural import core_split, sls_slotting
from casa.config import policy as load_policy
from casa.config import settings, table
from casa.model import forecast

logger = logging.getLogger(__name__)


def load_series(weekly: pd.DataFrame, as_of: date | None) -> tuple[dict, dict, dict, date]:
    df = weekly.copy()
    df["week_end_date"] = pd.to_datetime(df["week_end_date"]).dt.date
    if as_of is not None:
        df = df[df["week_end_date"] <= as_of]
    if df.empty:
        raise ValueError(f"no weekly balances on or before {as_of}")
    df = df.sort_values(["segment_id", "week_end_date"])
    series, week_ends, category = {}, {}, {}
    for seg, g in df.groupby("segment_id"):
        series[seg] = g["balance_inr_cr"].astype(float).to_numpy()
        week_ends[seg] = g["week_end_date"].to_numpy()
        category[seg] = g["irrbb_category"].iloc[-1]
    return series, week_ends, category, df["week_end_date"].max()


def model_revision(repo: str) -> str | None:
    try:
        from huggingface_hub import model_info

        return model_info(repo).sha
    except Exception:
        return None


def run_monthly(storage, model, as_of: date | None = None, triggered_by: str = "manual",
                model_id: str | None = None, write: bool = True) -> dict[str, pd.DataFrame]:
    pol = load_policy()
    horizon = int(pol["horizon_weeks"])
    weekly = storage.read("casa_weekly_balance")
    series, week_ends, category, last_week = load_series(weekly, as_of)
    as_of = as_of or last_week
    run_id = f"{as_of:%Y%m%d}-{uuid.uuid4().hex[:8]}"
    now = datetime.now().replace(microsecond=0)
    segs = sorted(series)
    logger.info("run %s: %d segments, %d weeks of history, last week %s",
                run_id, len(segs), max(len(v) for v in series.values()), last_week)

    b = pol["backtest"]
    bt_rows = bt.run_backtest(model, series, week_ends, b["horizon_weeks"], b["n_cutoffs"], b["min_history_weeks"])
    bt_summary = bt.summarise(bt_rows)
    logger.info("backtest: actual stayed above P10 in %s of segment-weeks, inside P10-P90 in %s",
                f"{bt_summary['p10_hit_rate']:.0%}" if bt_summary["p10_hit_rate"] is not None else "n/a",
                f"{bt_summary['band_coverage']:.0%}" if bt_summary["band_coverage"] is not None else "n/a")
    seg_hit = pd.DataFrame(bt_rows).groupby("segment_id")["p10_hit_rate"].mean().to_dict() if bt_rows else {}

    fc = forecast(model, [series[s] for s in segs], horizon)

    split, paths, sls = [], [], []
    for i, seg in enumerate(segs):
        cur, cat = float(series[seg][-1]), category[seg]
        last = pd.Timestamp(week_ends[seg][-1])
        common = {"as_of_date": as_of, "run_id": run_id, "segment_id": seg}
        split.append({**common, "irrbb_category": cat, **core_split(cur, fc.p10[i], cat, pol),
                      "backtest_p10_hit_rate": round(float(seg_hit[seg]), 4) if seg in seg_hit else None,
                      "created_at": now})
        for k in range(horizon):
            paths.append({**common, "horizon_week": k + 1,
                          "week_end_date": (last + pd.Timedelta(weeks=k + 1)).date(),
                          "mean": float(fc.mean[i, k]), "p10": float(fc.p10[i, k]),
                          "p50": float(fc.p50[i, k]), "p90": float(fc.p90[i, k])})
        for r in sls_slotting(cur, fc.p10[i], cat, pol):
            sls.append({**common, "irrbb_category": cat, **r})

    model_id = model_id or settings()["model"]["hf_repo"]
    out = {
        "casa_behavioural_split": pd.DataFrame(split),
        "casa_forecast_path": pd.DataFrame(paths),
        "casa_sls_buckets": pd.DataFrame(sls),
        "casa_backtest_metrics": pd.DataFrame([{"as_of_date": as_of, "run_id": run_id, **r} for r in bt_rows]),
        "casa_model_run": pd.DataFrame([{
            "as_of_date": as_of, "run_id": run_id, "run_ts": now, "model_id": model_id,
            "model_revision": model_revision(model_id) if "/" in model_id else None,
            "horizon_weeks": horizon, "n_segments": len(segs),
            "history_weeks": int(max(len(v) for v in series.values())), "last_week_end": last_week,
            "source_table": table("casa_weekly_balance"),
            "source_snapshot_id": storage.snapshot_id("casa_weekly_balance"),
            "backtest_p10_hit_rate": bt_summary["p10_hit_rate"],
            "backtest_band_coverage": bt_summary["band_coverage"],
            "backtest_mape_p50": bt_summary["mape_p50"],
            "policy_json": json.dumps(pol, sort_keys=True), "triggered_by": triggered_by,
        }]),
    }
    if write:
        for key, df in out.items():
            if not df.empty:
                storage.replace_as_of(key, df, as_of)
    return out


def portfolio_summary(split: pd.DataFrame) -> pd.DataFrame:
    g = split.groupby("irrbb_category")[["balance_inr_cr", "core_inr_cr", "non_core_inr_cr"]].sum()
    g.loc["TOTAL"] = g.sum()
    g["core_share"] = np.round(g["core_inr_cr"] / g["balance_inr_cr"], 4)
    return g.reset_index()

"""Core / non-core split and RBI SLS slotting from a P10 forecast path.

Demo method (see config/policy.yaml):
  core share (model) = lowest P10 over the horizon / today's balance
  core share         = min(model share, BCBS category cap)
SLS slotting of today's balance:
  run-off     drop to the running minimum of the P10 path, by the day it happens (Day 1 - 1 year)
  cap excess  stable per the model but above the cap, slotted in Day 1
  core        spread over the long buckets with the category weights
The three components always add back to today's balance.
"""

from __future__ import annotations

import numpy as np

from casa.config import policy as load_policy

CATEGORIES = ("retail_transactional", "retail_non_transactional", "wholesale")


def _policy(pol: dict | None) -> dict:
    return pol or load_policy()


def core_split(current: float, p10_path, category: str, pol: dict | None = None) -> dict:
    pol = _policy(pol)
    caps = pol["irrbb_caps"][category]
    min_p10 = float(np.min(p10_path)) if len(p10_path) else 0.0
    raw = max(0.0, min_p10) / current if current > 0 else 0.0
    share = min(raw, caps["max_core_share"])
    core = current * share
    return {
        "balance_inr_cr": round(current, 2),
        "min_p10_inr_cr": round(min_p10, 2),
        "core_share_model": round(raw, 4),
        "core_cap": caps["max_core_share"],
        "core_share": round(share, 4),
        "cap_binding": bool(raw > caps["max_core_share"]),
        "core_inr_cr": round(core, 2),
        "non_core_inr_cr": round(current - core, 2),
        "core_avg_maturity_years": round(core_avg_maturity(category, pol), 2),
        "max_avg_maturity_years": caps["max_avg_maturity_years"],
    }


def core_avg_maturity(category: str, pol: dict | None = None) -> float:
    pol = _policy(pol)
    weights = pol["core_distribution"][category]
    mids = pol["bucket_midpoint_years"]
    return float(sum(w * mids[b] for b, w in weights.items()))


def runoff_curve(current: float, p10_path) -> np.ndarray:
    """Cumulative run-off at the end of each forecast week (non-decreasing, >= 0)."""
    envelope = np.minimum.accumulate(np.minimum(current, np.maximum(np.asarray(p10_path, float), 0.0)))
    return current - envelope


def sls_slotting(current: float, p10_path, category: str, pol: dict | None = None) -> list[dict]:
    pol = _policy(pol)
    split = core_split(current, p10_path, category, pol)
    runoff = runoff_curve(current, p10_path)
    horizon_days = 7 * len(runoff)
    days = np.concatenate([[0.0], 7.0 * np.arange(1, len(runoff) + 1)])
    cum = np.concatenate([[0.0], runoff])

    def cum_runoff(day: float) -> float:
        return float(np.interp(min(day, horizon_days), days, cum))

    rows, prev_day = [], 0.0
    core_weights = pol["core_distribution"][category]
    for order, b in enumerate(pol["sls_buckets"], start=1):
        base = {"bucket_code": b["code"], "bucket_label": b["label"], "bucket_order": order}
        end_day = b["max_days"]
        if end_day is not None and prev_day < horizon_days:
            amt = cum_runoff(end_day) - cum_runoff(prev_day)
            if amt > 0:
                rows.append({**base, "component": "runoff", "amount_inr_cr": amt})
        if b["code"] == pol["cap_excess_bucket"]:
            cap_excess = (current - runoff[-1]) - split["core_inr_cr"] if len(runoff) else 0.0
            if cap_excess > 1e-9:
                rows.append({**base, "component": "cap_excess", "amount_inr_cr": cap_excess})
        if b["code"] in core_weights:
            rows.append({**base, "component": "core", "amount_inr_cr": split["core_inr_cr"] * core_weights[b["code"]]})
        prev_day = end_day if end_day is not None else prev_day
    for r in rows:
        r["amount_inr_cr"] = round(r["amount_inr_cr"], 4)
    return rows


def stress_history(history, weeks: int, cut: float) -> list[float]:
    """Scale the last `weeks` values by (1 - cut), e.g. a 10% run over 8 weeks."""
    h = [float(x) for x in history]
    for i in range(max(0, len(h) - weeks), len(h)):
        h[i] *= 1.0 - cut
    return h

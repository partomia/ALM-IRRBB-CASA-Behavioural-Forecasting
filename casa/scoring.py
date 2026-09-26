"""Request handling shared by the CAI model endpoint and the app's in-process mode.

Request:
  {"horizon_weeks": 52,                        # optional, 1-256
   "stress": {"weeks": 8, "cut": 0.10},        # optional: last 8 weeks cut by 10%
   "include_sls": true,                        # optional, default true
   "segments": [{"segment_id": "SA_RETAIL_URBAN",
                 "irrbb_category": "retail_transactional",
                 "weekly_balance_inr_cr": [...52+ values, oldest first...]}]}
Response:
  {"results": [{segment_id, core split fields, p10/p50/p90 paths, sls_buckets}],
   "horizon_weeks": 52, "stress": {...} | null}
"""

from __future__ import annotations

import math

from casa.behavioural import CATEGORIES, core_split, sls_slotting, stress_history
from casa.config import policy
from casa.model import forecast

MIN_HISTORY = 52
MAX_HORIZON = 256


def validate(req: dict) -> str | None:
    segs = req.get("segments") if isinstance(req, dict) else None
    if not segs or not isinstance(segs, list):
        return ("send {'segments': [{'segment_id', 'irrbb_category', "
                "'weekly_balance_inr_cr': [...]}]}")
    for s in segs:
        sid = s.get("segment_id", "?")
        if s.get("irrbb_category") not in CATEGORIES:
            return f"{sid}: irrbb_category must be one of {list(CATEGORIES)}"
        hist = s.get("weekly_balance_inr_cr")
        if not isinstance(hist, list) or len(hist) < MIN_HISTORY:
            return f"{sid}: send at least {MIN_HISTORY} weeks of history"
        try:
            if any(not math.isfinite(float(x)) for x in hist):
                return f"{sid}: weekly_balance_inr_cr must be finite numbers"
        except (TypeError, ValueError):
            return f"{sid}: weekly_balance_inr_cr must be numbers"
    try:
        h = int(req.get("horizon_weeks", policy()["horizon_weeks"]))
    except (TypeError, ValueError):
        return "horizon_weeks must be an integer"
    if not 1 <= h <= MAX_HORIZON:
        return f"horizon_weeks must be between 1 and {MAX_HORIZON}"
    stress = req.get("stress")
    if stress is not None:
        try:
            w, c = int(stress.get("weeks", 0)), float(stress.get("cut", 0))
        except (AttributeError, TypeError, ValueError):
            return "stress must look like {'weeks': 8, 'cut': 0.10}"
        if not (0 <= w <= MIN_HISTORY and 0 <= c < 1):
            return "stress.weeks must be 0-52 and stress.cut in [0, 1)"
    return None


def score(req: dict, model) -> dict:
    err = validate(req)
    if err:
        return {"error": err}
    pol = policy()
    horizon = int(req.get("horizon_weeks", pol["horizon_weeks"]))
    stress = req.get("stress")
    segs = req["segments"]
    histories = [[float(x) for x in s["weekly_balance_inr_cr"]] for s in segs]
    if stress:
        histories = [stress_history(h, int(stress.get("weeks", 0)), float(stress.get("cut", 0))) for h in histories]

    fc = forecast(model, histories, horizon)
    results = []
    for i, (s, h) in enumerate(zip(segs, histories)):
        cur, cat = h[-1], s["irrbb_category"]
        r = {"segment_id": s.get("segment_id"), "irrbb_category": cat, **core_split(cur, fc.p10[i], cat, pol),
             "p10": [round(float(x), 2) for x in fc.p10[i]],
             "p50": [round(float(x), 2) for x in fc.p50[i]],
             "p90": [round(float(x), 2) for x in fc.p90[i]]}
        if req.get("include_sls", True):
            r["sls_buckets"] = sls_slotting(cur, fc.p10[i], cat, pol)
        results.append(r)
    return {"results": results, "horizon_weeks": horizon, "stress": stress or None}

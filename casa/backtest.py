"""Rolling-origin backtest: hide the last N weeks at several cut-offs and
check how the forecast band held up. The P10 hit rate is the plain-language
trust number for treasury: how often did the actual balance stay above the
pessimistic path (a calibrated model gives about 90%)."""

from __future__ import annotations

import numpy as np

from casa.model import forecast


def cutoffs(n_weeks: int, horizon: int, n_cutoffs: int, min_history: int) -> list[int]:
    """Index of the first hidden week for each origin, latest first."""
    out = []
    for k in range(1, n_cutoffs + 1):
        c = n_weeks - k * horizon
        if c >= min_history:
            out.append(c)
    return out


def run_backtest(model, series: dict, week_ends: dict, horizon: int, n_cutoffs: int, min_history: int) -> list[dict]:
    """series: segment_id -> 1-D array of weekly balances (oldest first);
    week_ends: segment_id -> matching array of week_end dates."""
    jobs = []
    for seg, h in series.items():
        for c in cutoffs(len(h), horizon, n_cutoffs, min_history):
            jobs.append((seg, c))
    if not jobs:
        return []
    fc = forecast(model, [series[s][:c] for s, c in jobs], horizon)
    rows = []
    for i, (seg, c) in enumerate(jobs):
        actual = np.asarray(series[seg][c:c + horizon], float)
        p10, p50, p90 = fc.p10[i, :len(actual)], fc.p50[i, :len(actual)], fc.p90[i, :len(actual)]
        rows.append({
            "segment_id": seg,
            "cutoff_week_end": week_ends[seg][c - 1],
            "horizon_weeks": int(len(actual)),
            "p10_hit_rate": round(float(np.mean(actual >= p10)), 4),
            "band_coverage": round(float(np.mean((actual >= p10) & (actual <= p90))), 4),
            "mape_p50": round(float(np.mean(np.abs(actual - p50) / np.abs(actual))), 4),
        })
    return rows


def summarise(rows: list[dict]) -> dict:
    if not rows:
        return {"p10_hit_rate": None, "band_coverage": None, "mape_p50": None}
    return {k: round(float(np.mean([r[k] for r in rows])), 4) for k in ("p10_hit_rate", "band_coverage", "mape_p50")}

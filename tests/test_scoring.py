import numpy as np
import pytest

from casa.model import NaiveQuantileModel
from casa.scoring import score, validate


def _req(**kw):
    hist = list(1000 * np.exp(np.cumsum(np.full(120, 0.002))))
    req = {"segments": [{"segment_id": "S1", "irrbb_category": "retail_transactional",
                         "weekly_balance_inr_cr": hist}]}
    req.update(kw)
    return req


@pytest.mark.parametrize("bad, msg", [
    ({}, "segments"),
    ({"segments": [{"segment_id": "X", "irrbb_category": "nope", "weekly_balance_inr_cr": [1] * 60}]}, "irrbb_category"),
    ({"segments": [{"segment_id": "X", "irrbb_category": "wholesale", "weekly_balance_inr_cr": [1] * 10}]}, "52 weeks"),
    ({"segments": [{"segment_id": "X", "irrbb_category": "wholesale", "weekly_balance_inr_cr": ["a"] * 60}]}, "numbers"),
])
def test_validation_errors(bad, msg):
    assert msg in validate(bad)


def test_bad_horizon_and_stress():
    assert "horizon_weeks" in validate(_req(horizon_weeks=0))
    assert "stress" in validate(_req(stress={"weeks": 8, "cut": 1.5}))


def test_score_returns_paths_and_buckets():
    out = score(_req(horizon_weeks=26), NaiveQuantileModel())
    r = out["results"][0]
    assert len(r["p10"]) == 26 and len(r["p90"]) == 26
    assert sum(b["amount_inr_cr"] for b in r["sls_buckets"]) == pytest.approx(r["balance_inr_cr"], abs=0.05)


def test_stress_lowers_balance_and_core():
    base = score(_req(), NaiveQuantileModel())["results"][0]
    hit = score(_req(stress={"weeks": 8, "cut": 0.10}), NaiveQuantileModel())["results"][0]
    assert hit["balance_inr_cr"] == pytest.approx(base["balance_inr_cr"] * 0.9, rel=1e-3)
    assert hit["core_inr_cr"] < base["core_inr_cr"]

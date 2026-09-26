import numpy as np
import pytest

from casa.behavioural import CATEGORIES, core_avg_maturity, core_split, runoff_curve, sls_slotting, stress_history
from casa.config import policy


def test_cap_binds_when_model_share_above_cap():
    s = core_split(100.0, np.full(52, 95.0), "retail_transactional")
    assert s["core_share_model"] == 0.95
    assert s["core_share"] == 0.90 and s["cap_binding"]
    assert s["core_inr_cr"] + s["non_core_inr_cr"] == pytest.approx(100.0)


def test_model_share_used_below_cap():
    s = core_split(100.0, np.linspace(99, 40, 52), "wholesale")
    assert s["core_share"] == pytest.approx(0.40) and not s["cap_binding"]


def test_negative_p10_and_zero_balance_are_safe():
    assert core_split(100.0, np.full(52, -5.0), "wholesale")["core_share"] == 0.0
    assert core_split(0.0, np.full(52, 5.0), "wholesale")["core_share"] == 0.0


def test_runoff_curve_is_monotone_and_bounded():
    p10 = np.array([98, 101, 95, 97, 90, 92], float)
    r = runoff_curve(100.0, p10)
    assert np.all(np.diff(r) >= 0) and r[0] == 2 and r[-1] == 10


@pytest.mark.parametrize("category", CATEGORIES)
@pytest.mark.parametrize("horizon", [13, 52, 104])
def test_sls_buckets_add_up_to_balance(category, horizon):
    rng = np.random.default_rng(0)
    p10 = 1000 * np.exp(np.cumsum(rng.normal(-0.004, 0.01, horizon)))
    rows = sls_slotting(1000.0, p10, category)
    assert sum(r["amount_inr_cr"] for r in rows) == pytest.approx(1000.0, abs=0.01)
    assert all(r["amount_inr_cr"] >= 0 for r in rows)
    core = sum(r["amount_inr_cr"] for r in rows if r["component"] == "core")
    assert core == pytest.approx(core_split(1000.0, p10, category)["core_inr_cr"], abs=0.01)


def test_cap_excess_lands_in_day_one():
    rows = sls_slotting(100.0, np.full(52, 96.0), "retail_transactional")
    excess = [r for r in rows if r["component"] == "cap_excess"]
    assert len(excess) == 1 and excess[0]["bucket_code"] == "B01"
    assert excess[0]["amount_inr_cr"] == pytest.approx(6.0)


@pytest.mark.parametrize("category", CATEGORIES)
def test_core_distribution_respects_bcbs_maturity_cap(category):
    pol = policy()
    assert sum(pol["core_distribution"][category].values()) == pytest.approx(1.0)
    assert core_avg_maturity(category) <= pol["irrbb_caps"][category]["max_avg_maturity_years"]


def test_stress_history_only_touches_tail():
    h = stress_history([100.0] * 10, weeks=3, cut=0.1)
    assert h[:7] == [100.0] * 7 and h[7:] == pytest.approx([90.0] * 3)

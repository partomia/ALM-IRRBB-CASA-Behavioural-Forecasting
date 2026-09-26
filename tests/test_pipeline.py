from datetime import date

import numpy as np
import pandas as pd
import pytest

from casa import backtest as bt
from casa.model import NaiveQuantileModel, forecast
from casa.pipeline import portfolio_summary, run_monthly
from casa.storage import ParquetStorage


def _weekly_frame(n_weeks=200):
    rng = np.random.default_rng(1)
    weeks = pd.date_range("2022-01-07", periods=n_weeks, freq="W-FRI")
    rows = []
    for seg, cat, base in [("SA_A", "retail_transactional", 1000), ("SA_B", "retail_non_transactional", 500),
                           ("CA_C", "wholesale", 800)]:
        bal = base * np.exp(np.cumsum(rng.normal(0.001, 0.01, n_weeks)))
        rows += [{"segment_id": seg, "irrbb_category": cat, "week_end_date": w.date(),
                  "balance_inr_cr": float(b), "account_count": 100} for w, b in zip(weeks, bal)]
    return pd.DataFrame(rows)


@pytest.fixture
def storage(tmp_path):
    s = ParquetStorage(tmp_path)
    _weekly_frame().to_parquet(tmp_path / "casa_weekly_balance.parquet", index=False)
    return s


def test_forecast_shapes():
    fc = forecast(NaiveQuantileModel(), [np.ones(60), np.arange(1, 80)], 13)
    assert fc.p10.shape == (2, 13) and np.all(fc.p10 <= fc.p50) and np.all(fc.p50 <= fc.p90)


def test_backtest_cutoffs_respect_min_history():
    assert bt.cutoffs(200, 13, 4, 104) == [187, 174, 161, 148]
    assert bt.cutoffs(110, 13, 4, 104) == []


def test_monthly_run_writes_all_tables(storage):
    out = run_monthly(storage, NaiveQuantileModel(), model_id="naive")
    split = out["casa_behavioural_split"]
    assert len(split) == 3 and split["core_share"].le(split["core_cap"]).all()
    assert len(out["casa_forecast_path"]) == 3 * 52
    sls = out["casa_sls_buckets"].groupby("segment_id")["amount_inr_cr"].sum()
    assert sls.to_dict() == pytest.approx(split.set_index("segment_id")["balance_inr_cr"].to_dict(), abs=0.05)
    assert len(storage.read("casa_backtest_metrics")) == 3 * 4
    total = portfolio_summary(split).set_index("irrbb_category").loc["TOTAL"]
    assert total["core_inr_cr"] + total["non_core_inr_cr"] == pytest.approx(total["balance_inr_cr"])


def test_rerun_replaces_only_that_month(storage):
    run_monthly(storage, NaiveQuantileModel(), as_of=date(2025, 6, 30), model_id="naive")
    run_monthly(storage, NaiveQuantileModel(), as_of=date(2025, 7, 31), model_id="naive")
    run_monthly(storage, NaiveQuantileModel(), as_of=date(2025, 7, 31), model_id="naive")
    runs = storage.read("casa_model_run")
    assert sorted(pd.to_datetime(runs["as_of_date"]).dt.date) == [date(2025, 6, 30), date(2025, 7, 31)]
    assert len(storage.read("casa_behavioural_split", where_as_of=date(2025, 7, 31))) == 3

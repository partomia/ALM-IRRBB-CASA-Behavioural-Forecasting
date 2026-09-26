"""Headless render of every tab of the Streamlit app against a small parquet
lakehouse built with the naive model (no TimesFM download needed)."""

import pytest

pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest  # noqa: E402

from casa.config import ROOT  # noqa: E402
from casa.model import NaiveQuantileModel  # noqa: E402
from casa.pipeline import run_monthly  # noqa: E402
from casa.storage import ParquetStorage  # noqa: E402

from test_pipeline import _weekly_frame  # noqa: E402


@pytest.fixture
def parquet_env(tmp_path, monkeypatch):
    _weekly_frame().to_parquet(tmp_path / "casa_weekly_balance.parquet", index=False)
    storage = ParquetStorage(tmp_path)
    from datetime import date

    for d in (date(2025, 6, 30), date(2025, 7, 31)):
        run_monthly(storage, NaiveQuantileModel(), as_of=d, model_id="naive")
    monkeypatch.setenv("CASA_STORAGE_BACKEND", "parquet")
    monkeypatch.setenv("CASA_STORAGE_PARQUET_DIR", str(tmp_path))
    monkeypatch.setenv("CASA_ENDPOINT_URL", "")
    from casa import client, config

    config.settings.cache_clear()
    monkeypatch.setattr(client, "_local_model", NaiveQuantileModel())
    yield
    config.settings.cache_clear()


def test_app_renders_and_runs_stress(parquet_env):
    at = AppTest.from_file(str(ROOT / "app" / "streamlit_app.py"), default_timeout=60)
    at.run()
    assert not at.exception, at.exception
    assert any("Total CASA" in m.label for m in at.metric)
    at.button(key="run_stress").click().run()
    assert not at.exception, at.exception
    assert any("stressed" in m.label for m in at.metric)

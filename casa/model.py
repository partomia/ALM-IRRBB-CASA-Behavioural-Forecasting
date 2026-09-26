"""TimesFM 2.5 loading and batched quantile forecasts.

TimesFM returns point forecasts of shape (n, h) and quantiles of shape
(n, h, 10): index 0 is the mean, then P10 ... P90.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

from casa.config import settings

logger = logging.getLogger(__name__)

Q_MEAN, Q_P10, Q_P50, Q_P90 = 0, 1, 5, 9


@dataclass
class Forecast:
    mean: np.ndarray  # (n_series, horizon)
    p10: np.ndarray
    p50: np.ndarray
    p90: np.ndarray


def load_model(source: str | None = None, max_horizon: int | None = None):
    """Load and compile TimesFM 2.5 from a local folder or the Hugging Face Hub."""
    import timesfm

    cfg = settings()["model"]
    source = source or cfg["local_dir"] or cfg["hf_repo"]
    logger.info("Loading TimesFM from %s", source)
    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained(source, torch_compile=cfg["torch_compile"])
    model.compile(timesfm.ForecastConfig(
        max_context=cfg["max_context"],
        max_horizon=max_horizon or cfg["max_horizon"],
        per_core_batch_size=16,
        normalize_inputs=True,
        use_continuous_quantile_head=True,
        force_flip_invariance=True,
        infer_is_positive=True,
        fix_quantile_crossing=True,
    ))
    return model


def forecast(model, histories: list, horizon: int) -> Forecast:
    """histories: 1-D sequences of weekly balances, oldest first."""
    inputs = [np.asarray(h, dtype=float) for h in histories]  # fresh list: TimesFM pads it in place
    _, q = model.forecast(horizon=horizon, inputs=inputs)
    q = np.asarray(q)
    return Forecast(mean=q[:, :, Q_MEAN], p10=q[:, :, Q_P10], p50=q[:, :, Q_P50], p90=q[:, :, Q_P90])


class NaiveQuantileModel:
    """Stand-in with the TimesFM forecast() interface, for tests and offline
    smoke runs: last value plus a random-walk band from recent weekly changes."""

    def forecast(self, horizon: int, inputs: list):
        z = np.array([0.0, -1.2816, -0.8416, -0.5244, -0.2533, 0.0, 0.2533, 0.5244, 0.8416, 1.2816])
        points, quantiles = [], []
        for x in inputs:
            x = np.asarray(x, dtype=float)
            last = x[-1]
            steps = np.diff(np.log(np.maximum(x[-104:], 1e-9)))
            sd = float(np.std(steps)) if len(steps) > 1 else 0.0
            drift = float(np.mean(steps)) if len(steps) else 0.0
            k = np.arange(1, horizon + 1)
            centre = last * np.exp(drift * k)
            q = centre[:, None] * np.exp(z[None, :] * sd * np.sqrt(k)[:, None])
            q[:, 0] = centre
            points.append(centre)
            quantiles.append(q)
        return np.stack(points), np.stack(quantiles)

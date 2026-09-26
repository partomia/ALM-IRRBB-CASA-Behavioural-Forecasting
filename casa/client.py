"""Call the what-if scorer: the CAI model endpoint if configured, otherwise
in-process (TimesFM if installed, else the naive stand-in so the app still
runs on a light host)."""

from __future__ import annotations

import logging

import requests

from casa.config import settings
from casa.scoring import score

logger = logging.getLogger(__name__)
_local_model = None


def endpoint_configured() -> bool:
    return bool(settings()["endpoint"]["url"])


def call_endpoint(req: dict, timeout: int = 120) -> dict:
    ep = settings()["endpoint"]
    headers = {"Content-Type": "application/json"}
    if ep["api_key"]:
        headers["Authorization"] = f"Bearer {ep['api_key']}"
    resp = requests.post(ep["url"], json={"accessKey": ep["access_key"], "request": req},
                         headers=headers, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if isinstance(body, dict) and "response" in body:
        return body["response"]
    return body


def local_model():
    global _local_model
    if _local_model is None:
        try:
            from casa.model import load_model

            _local_model = load_model()
        except ImportError:
            from casa.model import NaiveQuantileModel

            logger.warning("timesfm/torch not installed: using the naive random-walk model")
            _local_model = NaiveQuantileModel()
    return _local_model


def what_if(req: dict) -> tuple[dict, str]:
    """Returns (response, source) where source is 'endpoint' or the local model name."""
    if endpoint_configured():
        return call_endpoint(req), "endpoint"
    model = local_model()
    return score(req, model), type(model).__name__

"""
CAI Model Deployment: what-if forecast and core split for one or more segments.

Model Deployments > New Model: File cai/model/predict.py, Function predict,
Python 3.11 runtime, 2 vCPU / 8 GB. The model build runs cdsw-build.sh.
Request/response format: see casa/scoring.py. TimesFM loads once per replica.
"""

import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

from casa.model import load_model  # noqa: E402
from casa.scoring import score  # noqa: E402

try:
    import cml.models_v1 as models

    cml_model = models.cml_model
except ImportError:  # outside CAI (local tests)
    def cml_model(fn):
        return fn

MODEL = load_model()


@cml_model
def predict(args):
    return score(args, MODEL)

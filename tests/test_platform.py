"""scripts/platform.py against stubbed CLIs: only this project's resources are touched, Spark
runs are killed before orchestration runs, dry runs act on nothing, and the shared services
need --yes-shared."""
import json

import pytest

from casa.config import ROOT
from ci.cai_jobs import APP, CAI_PROJECT_NAME, JOBS, MODEL
from tests.conftest import _load

pytest.importorskip("requests")
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
DEPLOY_DAG = (ROOT / "cde" / "scripts" / "deploy_dag.sh").read_text()


@pytest.fixture
def platform(monkeypatch):
    mod = _load(ROOT / "scripts" / "platform.py")
    monkeypatch.setattr(mod, "load_env", lambda *a: None)
    return mod


def test_config_names_match_the_deployed_resources():
    prj = CFG["project"]
    assert prj["cai_project"] == CAI_PROJECT_NAME
    assert prj["cai_apps"] == [APP["name"]] and prj["cai_models"] == [MODEL["name"]]
    assert all(j["name"].startswith(prj["cai_job_prefix"]) for j in JOBS)
    assert 'DAG_JOB_NAME="${DAG_JOB_NAME:-rsingh-casa-alb-orchestration}"' in DEPLOY_DAG
    assert prj["cde_dag_jobs"] == ["rsingh-casa-alb-orchestration"]
    assert sorted(CFG["stop_order"]) == sorted(CFG["start_order"]) == ["cai", "cde", "cdw"]
    assert CFG["start_order"] == CFG["stop_order"][::-1]


RUNS = [
    {"id": 1, "job": "rsingh-casa-alb-orchestration", "type": "airflow", "status": "running"},
    {"id": 2, "job": "rsingh-casa-alb-build-gold-weekly", "type": "spark", "status": "starting"},
    {"id": 3, "job": "rsingh-spend-orchestration", "type": "airflow", "status": "running"},
    {"id": 4, "job": "rsingh-casa-alb-gold", "type": "spark", "status": "succeeded"},
]
JOBS_LIVE = [
    {"name": "rsingh-casa-alb-orchestration", "type": "airflow",
     "schedule": {"enabled": True, "paused": False, "cronExpression": "0 6 1 * *"}},
    {"name": "rsingh-casa-alb-validate-bronze", "type": "spark", "schedule": {"enabled": False}},
    {"name": "rsingh-casa-albx-orchestration", "type": "airflow", "schedule": {"enabled": True, "paused": False}},
    {"name": "rsingh-mule-orchestration", "type": "airflow", "schedule": {"enabled": True, "paused": False}},
]


def _stub_cde(platform, monkeypatch, jobs=JOBS_LIVE):
    calls = []

    def cde(*args):
        calls.append(args)
        if args[:2] == ("run", "list"):
            return RUNS
        if args[:2] == ("job", "list"):
            return jobs
        return ""
    monkeypatch.setattr(platform, "cde", cde)
    return calls


def test_cde_stop_touches_only_this_project_spark_first(platform, monkeypatch):
    calls = _stub_cde(platform, monkeypatch)
    platform.cde_project("stop")
    acts = [c for c in calls if c[1] in ("schedule", "kill")]
    assert acts == [("job", "schedule", "pause", "--name", "rsingh-casa-alb-orchestration"),
                    ("run", "kill", "--id", "2"), ("run", "kill", "--id", "1")]


def test_cde_start_unpauses_only_this_project_schedule(platform, monkeypatch):
    paused = [dict(j, schedule=dict(j["schedule"], paused=True)) for j in JOBS_LIVE]
    calls = _stub_cde(platform, monkeypatch, paused)
    platform.cde_project("start")
    assert [c for c in calls if c[1] == "schedule"] == [
        ("job", "schedule", "unpause", "--name", "rsingh-casa-alb-orchestration")]


def test_dry_run_acts_on_nothing(platform, monkeypatch, capsys):
    calls = _stub_cde(platform, monkeypatch)
    monkeypatch.setattr(platform, "DRY", True)
    platform.cde_project("stop")
    assert all(c[:2] in (("run", "list"), ("job", "list")) for c in calls)
    assert "would kill CDE run rsingh-casa-alb-build-gold-weekly #2" in capsys.readouterr().out


def test_platform_scope_needs_yes_shared(platform, monkeypatch):
    monkeypatch.setattr(platform, "layer", lambda *a: pytest.fail("no layer may run"))
    with pytest.raises(SystemExit, match="--yes-shared"):
        platform.main(["stop", "--scope", "platform"])
    with pytest.raises(SystemExit, match="unknown layers"):
        platform.main(["stop", "--only", "kafka"])


def test_env_values_are_never_printed(platform, tmp_path, monkeypatch, capsys):
    env = tmp_path / ".env"
    env.write_text("# comment\nCASA_TEST_SECRET='s3cr3t'\nexport CASA_TEST_OTHER=x\n")
    monkeypatch.delenv("CASA_TEST_SECRET", raising=False)
    monkeypatch.delenv("CASA_TEST_OTHER", raising=False)
    _load(ROOT / "scripts" / "platform.py").load_env(env)
    import os
    assert os.environ.pop("CASA_TEST_SECRET") == "s3cr3t" and os.environ.pop("CASA_TEST_OTHER") == "x"
    assert "s3cr3t" not in capsys.readouterr().out

#!/usr/bin/env python3
"""
Stop, start or show this demo's live Cloudera resources, in dependency order (config/platform.json).

Two scopes:
  project   (default) this demo's workloads only: the CAI application and model deployment,
            active runs of rsingh-casa-alb-* CAI jobs; the rsingh-casa-alb-* CDE Airflow schedule
            and any running or starting rsingh-casa-alb-* CDE runs. Iceberg tables, the CDE
            repository and python-env, and the Data Visualization dashboard stay as they are.
  platform  the project scope plus the environment's shared services: the CDW Impala virtual
            warehouse, the CDE virtual cluster and the CAI workspace. Sibling demos use the same
            services, so stop/start needs --yes-shared.

Stop runs in stop_order (CAI, CDE, CDW); start in start_order (CDW first, CAI last), each layer's
service before its workloads. --only limits the layers. Data Visualization has no suspend in the
CDP CLI and is only reported. No Kafka, NiFi or Flink: this project has no streaming.

Credentials: CASA_CAI_HOST and CASA_CAI_API_KEY from the git-ignored .env (read by this script
if not already exported); the cdp and cde CLIs use their own profiles (~/.cdp, ~/.cde).

  python scripts/platform.py status [--scope platform]
  python scripts/platform.py stop  [--only cai,cde] [--dry-run]
  python scripts/platform.py start [--dry-run]
  python scripts/platform.py stop --scope platform --yes-shared
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

# this file shadows the stdlib `platform` for libraries that import it
sys.path[:] = [p for p in sys.path if Path(p or ".").resolve() != Path(__file__).resolve().parent]

import requests  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
PRJ, PLT = CFG["project"], CFG["platform"]
LAYERS = ("cdw", "cde", "cai")
ACTIVE_CAI = ("ENGINE_SCHEDULING", "ENGINE_STARTING", "ENGINE_RUNNING")
DRY = False
ACTED = 0


def say(msg: str) -> None:
    print(msg, flush=True)


def act(what: str, fn, *a, **kw):
    global ACTED
    ACTED += 1
    if DRY:
        say(f"  would {what}")
        return None
    say(f"  {what}")
    return fn(*a, **kw)


def wait_for(what: str, fn, ok: set, timeout: int = 2400, poll: int = 20) -> str:
    if DRY:
        return ""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        st = str(fn())
        if st != last:
            say(f"    {what}: {st} ({time.time() - t0:.0f} s)")
            last = st
        if st.lower() in {s.lower() for s in ok}:
            return st
        time.sleep(poll)
    raise SystemExit(f"{what}: still {last} after {timeout} s")


def load_env(path: Path = ROOT / ".env") -> None:
    """KEY=VALUE lines of .env into os.environ, without overriding what is exported. Never printed."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.removeprefix("export ").strip()
        os.environ.setdefault(key, value.strip().strip("'\""))


def mine(name: str, prefix: str) -> bool:
    return bool(name) and name.startswith(prefix)


# ------------------------------------------------------------------ CLIs
def cdp(*args: str) -> dict:
    exe = os.environ.get("CASA_CDP_CLI") or shutil.which("cdp") or next(iter(glob.glob(
        os.path.expanduser("~/Library/Python/*/bin/cdp"))), None)
    if not exe:
        raise SystemExit("cdp CLI not found: pip install cdpcli, or set CASA_CDP_CLI")
    out = subprocess.run([exe, *args], capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError(f"cdp {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def cde(*args: str):
    out = subprocess.run(["cde", *args], capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError(f"cde {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout) if out.stdout.strip().startswith(("{", "[")) else out.stdout


class Cai:
    def __init__(self):
        for key in ("CASA_CAI_HOST", "CASA_CAI_API_KEY"):
            if not os.environ.get(key):
                raise SystemExit(f"{key} is not set (.env)")
        self.base = os.environ["CASA_CAI_HOST"].rstrip("/") + "/api/v2"
        self.h = {"Authorization": f"Bearer {os.environ['CASA_CAI_API_KEY']}"}
        found = self("GET", "/projects", params={"search_filter": json.dumps({"name": PRJ["cai_project"]}),
                                                  "page_size": 100})
        pid = next((p["id"] for p in found.get("projects", []) if p["name"] == PRJ["cai_project"]), None)
        if pid is None:
            raise RuntimeError(f"CAI project {PRJ['cai_project']} not found")
        self.pid = pid

    def __call__(self, method: str, path: str, **kw) -> dict:
        r = requests.request(method, self.base + path, headers=self.h, timeout=60, **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path}: HTTP {r.status_code} {r.text[:200]}")
        return r.json() if r.text.strip() else {}

    def p(self, path: str) -> str:
        return f"/projects/{self.pid}{path}"

    def apps(self) -> list[dict]:
        return [a for a in self("GET", self.p("/applications"), params={"page_size": 100})["applications"]
                if a["name"] in PRJ["cai_apps"]]

    def jobs(self) -> list[dict]:
        return [j for j in self("GET", self.p("/jobs"), params={"page_size": 200})["jobs"]
                if mine(j["name"], PRJ["cai_job_prefix"])]

    def active_runs(self, job: dict) -> list[dict]:
        runs = self("GET", self.p(f"/jobs/{job['id']}/runs"), params={"page_size": 20}).get("job_runs", [])
        return [r for r in runs if r["status"] in ACTIVE_CAI]

    def deployments(self) -> list[tuple[dict, dict, dict]]:
        """(model, build, deployment) for every deployment of the project's models, newest first."""
        out = []
        for m in self("GET", self.p("/models"), params={"page_size": 100})["models"]:
            if m["name"] not in PRJ["cai_models"]:
                continue
            for b in self("GET", self.p(f"/models/{m['id']}/builds"), params={"page_size": 100})["model_builds"]:
                for d in self("GET", self.p(f"/models/{m['id']}/builds/{b['id']}/deployments"))["model_deployments"]:
                    out.append((m, b, d))
        return sorted(out, key=lambda t: t[2]["created_at"], reverse=True)


# ------------------------------------------------------------------ project workloads
def cai_project(action: str) -> None:
    c = Cai()
    deps = c.deployments()
    if action == "status":
        for a in c.apps():
            say(f"  app {a['name']}: {a['status']} ({a['cpu']} vCPU / {a['memory']} GB)")
        for name in PRJ["cai_models"]:
            live = [(b, d) for m, b, d in deps if m["name"] == name and d["status"] == "deployed"]
            newest = next((d for m, b, d in deps if m["name"] == name), None)
            say(f"  model {name}: " + (f"deployed (build {live[0][0]['id'][:8]})" if live else
                                       f"not deployed (newest deployment {newest['status']})" if newest else
                                       "no deployment"))
        for j in c.jobs():
            runs = c.active_runs(j)
            say(f"  job {j['name']}: schedule {j.get('schedule') or '-'}{' PAUSED' if j.get('paused') else ''}"
                f"{', ' + str(len(runs)) + ' active run(s)' if runs else ''}")
        return
    if action == "stop":
        for a in c.apps():
            if a["status"] != "APPLICATION_STOPPED":
                act(f"stop application {a['name']}", c, "POST", c.p(f"/applications/{a['id']}:stop"))
        for m, b, d in deps:
            if d["status"] in ("deployed", "deploying", "pending"):
                act(f"stop model {m['name']} deployment {d['id'][:8]}", c, "POST",
                    c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:stop"))
        for j in c.jobs():
            if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("schedule") and not j.get("paused"):
                act(f"pause schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": True})
            for r in c.active_runs(j):
                act(f"stop job run {j['name']} {r['id']}", c, "POST", c.p(f"/jobs/{j['id']}/runs/{r['id']}:stop"))
        return
    # start: schedules, the newest deployment of each model, then the applications
    for j in c.jobs():
        if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("paused"):
            act(f"resume schedule of {j['name']}", c, "PATCH", c.p(f"/jobs/{j['id']}"), json={"paused": False})
    for name in PRJ["cai_models"]:
        own = [(m, b, d) for m, b, d in deps if m["name"] == name]
        if own and own[0][2]["status"] != "deployed":
            m, b, d = own[0]
            act(f"restart model {name} deployment {d['id'][:8]}", c, "POST",
                c.p(f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:restart"))
    for a in c.apps():
        if a["status"] != "APPLICATION_RUNNING":
            act(f"start application {a['name']}", c, "POST", c.p(f"/applications/{a['id']}:restart"))


def cde_runs() -> list[dict]:
    prefix = PRJ["cde_job_prefix"]
    runs = cde("run", "list", "--filter", "status[in]running,starting", "--filter", f"job[rlike]^{prefix}") or []
    return [r for r in runs if mine(r.get("job", ""), prefix) and r.get("status") in ("running", "starting")]


def cde_dags() -> list[dict]:
    prefix = PRJ["cde_job_prefix"]
    jobs = cde("job", "list", "--filter", f"name[rlike]^{prefix}") or []
    return [j for j in jobs if mine(j["name"], prefix) and j.get("type") == "airflow"
            and j["name"] in PRJ["cde_dag_jobs"] and (j.get("schedule") or {}).get("enabled")]


def cde_project(action: str) -> None:
    runs, dags = cde_runs(), cde_dags()
    if action == "status":
        say("  active runs: " + (", ".join(f"{r['job']} #{r['id']} {r['status']}" for r in runs) or "none"))
        for j in dags:
            s = j["schedule"]
            say(f"  schedule {j['name']} ({s.get('cronExpression')}): {'paused' if s.get('paused') else 'active'}")
        return
    if action == "stop":
        for j in dags:
            if not j["schedule"].get("paused"):
                act(f"pause CDE schedule {j['name']}", cde, "job", "schedule", "pause", "--name", j["name"])
        # Spark children first, so an orchestration run does not retry them
        for r in sorted(runs, key=lambda r: r.get("type") == "airflow"):
            act(f"kill CDE run {r['job']} #{r['id']} ({r['status']})", cde, "run", "kill", "--id", str(r["id"]))
        return
    for j in dags:
        if j["schedule"].get("paused"):
            # catchup=False: Airflow runs the latest closed interval at once if it has no run yet
            act(f"unpause CDE schedule {j['name']} (if the latest closed interval has no run, it starts now)",
                cde, "job", "schedule", "unpause", "--name", j["name"])


PROJECT = {"cai": cai_project, "cde": cde_project}


# ------------------------------------------------------------------ shared services
def dw_cluster() -> str:
    env_crn = cdp("environments", "describe-environment", "--environment-name", CFG["environment"])["environment"]["crn"]
    return next(c["id"] for c in cdp("dw", "list-clusters")["clusters"] if c["environmentCrn"] == env_crn)


def cdw(action: str) -> None:
    cid = dw_cluster()
    vws = {v["name"]: v for v in cdp("dw", "list-vws", "--cluster-id", cid)["vws"]}
    for name in PLT["cdw_vws"]:
        vw = vws[name]
        status = lambda v=vw: cdp("dw", "describe-vw", "--cluster-id", cid, "--vw-id", v["id"])["vw"]["status"]  # noqa: E731
        if action == "status":
            say(f"  CDW virtual warehouse {name}: {vw['status']}")
        elif action == "stop" and vw["status"].lower() not in ("stopped", "suspended"):
            act(f"suspend CDW virtual warehouse {name}", cdp, "dw", "suspend-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Stopped", "Suspended"})
        elif action == "start" and vw["status"].lower() != "running":
            act(f"start CDW virtual warehouse {name}", cdp, "dw", "start-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Running"})
    if action == "status":
        dv = {d["name"]: d["status"] for d in cdp("dw", "list-data-visualizations", "--cluster-id", cid)
              .get("dataVisualizations", [])}
        for name in PLT.get("cdw_dataviz", []):
            say(f"  Data Visualization {name}: {dv.get(name, 'not found')} (no suspend in the CDP CLI, left as is)")


def cde_vc(action: str) -> None:
    svc = next(s for s in cdp("de", "list-services")["services"]
               if s["name"] == PLT["cde_service"] and s["status"] == "ClusterCreationCompleted")
    vc = next(v for v in cdp("de", "list-vcs", "--cluster-id", svc["clusterId"])["vcs"] if v["vcName"] == PLT["cde_vc"])
    ids = ("--cluster-id", svc["clusterId"], "--vc-id", vc["vcId"])
    status = lambda: cdp("de", "describe-vc", *ids)["vc"]["status"]  # noqa: E731
    st = vc["status"]
    if action == "status":
        say(f"  CDE virtual cluster {PLT['cde_vc']}: {st}")
    elif action == "stop" and "suspend" not in st.lower():
        act(f"suspend CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "suspend-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppSuspended", "Suspended"})
    elif action == "start" and "suspend" in st.lower():
        act(f"resume CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "resume-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppInstalled", "Running"})


def cai_workspace(action: str) -> None:
    def ws():
        return next(w for w in cdp("ml", "list-workspaces")["workspaces"]
                    if w["instanceName"] == PLT["cai_workspace"] and w["environmentName"] == CFG["environment"])
    w = ws()
    st = w["instanceStatus"]
    if action == "status":
        say(f"  CAI workspace {PLT['cai_workspace']}: {st}")
    elif action == "stop" and "suspend" not in st:
        act(f"suspend CAI workspace {PLT['cai_workspace']}", cdp, "ml", "suspend-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"suspend:finished"}, timeout=3600)
    elif action == "start" and "suspend" in st:
        act(f"resume CAI workspace {PLT['cai_workspace']}", cdp, "ml", "resume-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"resume:finished", "installation:finished"},
                 timeout=3600)


SHARED = {"cdw": cdw, "cde": cde_vc, "cai": cai_workspace}


def layer(name: str, action: str, scope: str) -> None:
    say(f"[{name}]")
    steps = []
    if action == "start" and scope == "platform":
        steps.append(SHARED[name])
    if name in PROJECT:
        steps.append(PROJECT[name])
    if action != "start" and scope == "platform":
        steps.append(SHARED[name])
    if not steps:
        say("  shared service only, no project workloads (--scope platform)")
        return
    before = ACTED
    for step in steps:
        try:
            step(action)
        except Exception as e:  # noqa: BLE001  one layer's failure is reported; the others still run
            say(f"  ERROR {type(e).__name__}: {e}")
            if action != "status":
                raise
    if action != "status" and ACTED == before:
        say(f"  nothing to {action}: already {'stopped' if action == 'stop' else 'running'}")


def main(argv: list[str] | None = None) -> int:
    global DRY
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "stop", "start"])
    ap.add_argument("--scope", choices=["project", "platform"], default="project")
    ap.add_argument("--only", default="", help=f"comma-separated layers from {', '.join(LAYERS)}")
    ap.add_argument("--yes-shared", action="store_true", help="required to stop or start the shared services")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    DRY = a.dry_run
    if a.scope == "platform" and a.action != "status" and not (a.yes_shared or a.dry_run):
        raise SystemExit("--scope platform stops/starts services other demos share: add --yes-shared")
    only = {x.strip() for x in a.only.split(",") if x.strip()}
    if only - set(LAYERS):
        raise SystemExit(f"unknown layers {sorted(only - set(LAYERS))}")
    load_env()
    order = CFG["start_order"] if a.action == "start" else CFG["stop_order"]
    for name in order:
        if not only or name in only:
            layer(name, a.action, a.scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
The Cloudera Data Visualization dashboard of this project, as code: "CASA ALCO" (the
month-end ALCO pack: core split, SLS, forecast, history and trust).

Datasets, visuals and sheets are declared below; this script turns them into a Data
Visualization export file (dataviz/casa_alco_dashboard.json) and imports it through the
migration REST API of the instance at CASA_CDV_URL. UUIDs are fixed per artefact, so an
import updates the dashboard in place. Datasets read the views of
sql/dataviz_views.sql in rsingh_casa_alb_gold; see docs/DATAVIZ.md.

  set -a; source .env; set +a
  python dataviz/build_dashboard.py              # views, connection, workspace (if missing), file, import
  python dataviz/build_dashboard.py --no-import  # views and the file only
  python dataviz/build_dashboard.py --verify     # every visual's query through the Data API

Needs CASA_CDV_URL and CASA_CDV_API_KEY (or CASA_VIZ_API_KEY), sent as
"Authorization: apikey ..."; CASA_IMPALA_USER / CASA_IMPALA_PASSWORD for the views, the
column types and a new connection. The password goes to the Data Visualization
connection only; it is never printed or written to the file.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

OUT = ROOT / "dataviz" / "casa_alco_dashboard.json"
VIEWS_SQL = ROOT / "sql" / "dataviz_views.sql"
DB = "rsingh_casa_alb_gold"
CONNECTION = "rsingh-casa-alb-impala"
WORKSPACE = "rsingh-casa-alb"
IMPALA_HOST = "coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site"
NS = uuid.UUID("3c6f2a8e-51d4-4b7a-9e0c-2d8f6a1b7c35")
DATASET_PK0 = 9300
VISUAL_PK0 = 9400

DATASETS = {                          # key: (name, view, integer columns that are dimensions)
    "split": ("rsingh-casa-alb - Core split", "v_alco_split", {"is_latest", "cap_binds"}),
    "sls": ("rsingh-casa-alb - SLS buckets", "v_sls", {"is_latest", "bucket_order"}),
    "runs": ("rsingh-casa-alb - ALCO runs", "v_model_run", {"is_latest", "history_weeks"}),
    "path": ("rsingh-casa-alb - Forecast path", "v_forecast_path", {"is_latest", "horizon_week"}),
    "weekly": ("rsingh-casa-alb - Weekly balance", "v_weekly_balance", set()),
}

LATEST = "[is_latest] = 1"
CORE_SHARE = "sum([core_inr_cr]) / sum([balance_inr_cr])"

# A visual: dims are (column, alias); measures are (expression, alias); filters are expressions;
# pos is (column, row, width, height) on a 64-column grid.
ALCO = [
    ("ALCO overview", [
        dict(type="kpi", ds="split", title="Total CASA (INR crore)", measures=[("round(sum([balance_inr_cr]), 0)", "Total CASA")],
             filters=[LATEST], pos=(1, 1, 16, 10)),
        dict(type="kpi", ds="split", title="Core (INR crore)", measures=[("round(sum([core_inr_cr]), 0)", "Core")],
             filters=[LATEST], pos=(17, 1, 16, 10)),
        dict(type="kpi", ds="split", title="Core share (%)", measures=[(f"round(100 * {CORE_SHARE}, 1)", "Core %")],
             filters=[LATEST], pos=(33, 1, 16, 10)),
        dict(type="kpi", ds="split", title="Segments where the BCBS cap binds",
             measures=[("sum([cap_binds])", "Cap binds")], filters=[LATEST], pos=(49, 1, 16, 10)),
        dict(type="trellis-bars", ds="split", title="Core and non-core by IRRBB category (INR crore)",
             x=[("irrbb_category", "IRRBB category")],
             measures=[("sum([core_inr_cr])", "Core"), ("sum([non_core_inr_cr])", "Non-core")],
             filters=[LATEST], pos=(1, 11, 32, 22)),
        dict(type="trellis-bars", ds="split", title="Model core share against the cap, by segment",
             x=[("segment_id", "Segment")],
             measures=[("max([core_share_model])", "Model core share"), ("max([core_cap])", "BCBS cap")],
             filters=[LATEST], pos=(33, 11, 32, 22)),
        dict(type="table", ds="split", title="Core split by segment (latest ALCO run)",
             dims=[("segment_id", "Segment"), ("irrbb_category", "IRRBB category"), ("cap_binds", "Cap binds")],
             measures=[("round(sum([balance_inr_cr]), 1)", "Balance (cr)"),
                       ("round(max([core_share_model]), 4)", "Model core share"),
                       ("round(max([core_share]), 4)", "Core share"),
                       ("round(sum([core_inr_cr]), 1)", "Core (cr)"), ("round(sum([non_core_inr_cr]), 1)", "Non-core (cr)"),
                       ("round(max([backtest_p10_hit_rate]), 3)", "Backtest P10 hit")],
             filters=[LATEST], sort_dim="segment_id", pos=(1, 33, 64, 26)),
    ]),
    ("Structural liquidity", [
        dict(type="trellis-bars", ds="sls", title="RBI SLS buckets by component (INR crore, latest run)",
             x=[("bucket", "Bucket")], measures=[("sum([amount_inr_cr])", "Amount (cr)")],
             color=[("component", "Component")], filters=[LATEST], sort_dim="bucket", pos=(1, 1, 64, 26)),
        dict(type="trellis-bars", ds="sls", title="SLS buckets by IRRBB category (INR crore, latest run)",
             x=[("bucket", "Bucket")], measures=[("sum([amount_inr_cr])", "Amount (cr)")],
             color=[("irrbb_category", "IRRBB category")], filters=[LATEST], sort_dim="bucket", pos=(1, 27, 64, 26)),
    ]),
    ("Forecast", [
        dict(type="trellis-lines", ds="path", title="Total CASA, 52-week forecast band (INR crore, latest run)",
             x=[("week_end_date", "Week")],
             measures=[("sum([p10])", "P10"), ("sum([p50])", "P50"), ("sum([p90])", "P90")],
             filters=[LATEST], pos=(1, 1, 64, 24)),
        dict(type="trellis-lines", ds="weekly", title="Weekly CASA balance by IRRBB category (INR crore)",
             x=[("week_end_date", "Week")], measures=[("sum([balance_inr_cr])", "Balance (cr)")],
             color=[("irrbb_category", "IRRBB category")], pos=(1, 25, 64, 24)),
    ]),
    ("History and trust", [
        dict(type="trellis-lines", ds="split", title="Core share by ALCO run and segment",
             x=[("as_of_date", "As of")], measures=[(CORE_SHARE, "Core share")], color=[("segment_id", "Segment")],
             pos=(1, 1, 32, 24)),
        dict(type="trellis-bars", ds="split", title="Core and non-core by ALCO run (INR crore)",
             x=[("as_of_date", "As of")],
             measures=[("sum([core_inr_cr])", "Core"), ("sum([non_core_inr_cr])", "Non-core")], pos=(33, 1, 32, 24)),
        dict(type="table", ds="runs", title="ALCO runs: trust numbers and lineage",
             dims=[("as_of_date", "As of"), ("triggered_by", "Triggered by"), ("run_id", "Run"),
                   ("model_revision", "Model revision"), ("source_snapshot_id", "Gold snapshot")],
             measures=[("max([backtest_p10_hit_rate])", "Above P10"), ("max([backtest_band_coverage])", "Inside P10-P90"),
                       ("max([backtest_mape_p50])", "MAPE P50"), ("max([history_weeks])", "History weeks")],
             sort_dim="as_of_date", sort_asc=False, pos=(1, 25, 64, 24)),
    ]),
]

DASHBOARDS = [
    dict(key="alco", pk=9200, ds="split", title="rsingh-casa-alb - CASA ALCO", sheets=ALCO,
         subtitle="Month-end CASA behavioural split, SLS slotting and TimesFM forecast (federal CDW)"),
]

SHELVES = {
    "kpi": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("compare_shelf", 1, 2), ("label_shelf", 1, 2),
            ("tooltip_shelf", 1, 2), ("x_shelf", 1, 1), ("y_shelf", 1, 1), ("filters_shelf", 2, 3)],
    "table": [("dimensions_shelf", 1, 1), ("aggregates_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-bars": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                     ("drill_shelf", 1, 1), ("label_shelf", 1, 2), ("filters_shelf", 2, 3)],
    "trellis-lines": [("x_shelf", 1, 3), ("y_shelf", 1, 3), ("color_shelf", 1, 3), ("tooltip_shelf", 1, 2),
                      ("filters_shelf", 2, 3)],
}


def uid(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def impala():
    from casa.storage import get_storage
    return get_storage("impala")


def create_views() -> int:
    statements = [s.strip() for s in "\n".join(l for l in VIEWS_SQL.read_text().splitlines()
                                               if not l.lstrip().startswith("--")).split(";") if s.strip()]
    storage = impala()
    for sql in statements:
        storage.execute(sql)
    print(f"views: {len(statements)} statements from {VIEWS_SQL.relative_to(ROOT)}")
    return len(statements)


def column_types(ds_key: str) -> dict[str, str]:
    view = DATASETS[ds_key][1]
    df = impala().query(f"DESCRIBE {DB}.{view}")
    return {r["name"]: r["type"].upper() for _, r in df.iterrows()}


def is_dim(ds_key: str, col: str, typ: str) -> bool:
    return col in DATASETS[ds_key][2] or not any(t in typ for t in ("INT", "DOUBLE", "FLOAT", "DECIMAL"))


def dataset_record(key: str, pk: int, types: dict[str, str], conn_id: int) -> dict:
    name, view, _ = DATASETS[key]
    used_by = [d["pk"] for d in DASHBOARDS if any(v["ds"] == key for _, items in d["sheets"] for v in items)]
    table = f"{DB}.{view}"
    cols = [{"alias": c, "type": t, "name": c, "isdim": is_dim(key, c, t)} for c, t in types.items()]
    return {"model": "datasets.dataset", "pk": pk, "fields": {
        "dataconnection": conn_id, "dataset_name": name, "dataset_type": "singletable", "dataset_detail": table,
        "dataset_description": f"{table} (sql/dataviz_views.sql)",
        "dataset_info": json.dumps([{"tablename": table, "columns": cols}]),
        "dataset_tablenames": json.dumps([table]), "uuid": uid("dataset", key), "imported_uuid": None,
        "cache_sequence": 0, "dataset_settings": "{}", "search_enabled": False, "dashboards": used_by,
        "version_id": pk, "version_group_id": pk, "is_active_version": True,
        "version_name": "casa-alco", "is_named_version": False}}


def dim_item(col: str, alias: str, typ: str) -> dict:
    return {"dataset_colname": col, "dataset_coltype": typ, "expression_for_trigger": f"[{col}]", "col_alias": alias}


def measure_item(expr: str, alias: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "expr_hasagg": True, "col_alias": alias,
            "dataset_colname": alias, "dataset_coltype": "DOUBLE"}


def filter_item(expr: str) -> dict:
    return {"custom_expr": expr, "expression_for_trigger": expr, "filter_input": {}, "filter_data": [],
            "dataset_colname": "", "dataset_coltype": "STRING", "filter_column": ""}


def visual_record(v: dict, pk: int, dash: dict, sheet: str, types: dict[str, str], dataset_pk: int,
                  workspace: int) -> dict:
    kind = v["type"]
    shelves = {name: [] for name, _, _ in SHELVES[kind]}
    sources = {}

    def add_dims(shelf, pairs):
        for col, alias in pairs:
            shelves[shelf].append(dim_item(col, alias, types[col]))
            sources[f"[{col}] as 'sub:{alias}'"] = shelf

    def add_measures(shelf, pairs):
        for expr, alias in pairs:
            shelves[shelf].append(measure_item(expr, alias))
            sources[f"{expr} as 'sub:{alias}'"] = shelf

    if kind in ("kpi", "table"):
        add_dims("dimensions_shelf", v.get("dims", []))
        add_measures("aggregates_shelf", v["measures"])
    else:
        add_dims("x_shelf", v["x"])
        add_measures("y_shelf", v["measures"])
        add_dims("color_shelf", v.get("color", []))
    for expr in v.get("filters", []):
        shelves["filters_shelf"].append(filter_item(expr))
        sources[expr] = "filters_shelf"
    if v.get("sort_dim"):
        shelf = "dimensions_shelf" if kind == "table" else "x_shelf"
        item = next(i for i in shelves[shelf] if i["dataset_colname"] == v["sort_dim"])
        item["order"] = {"priority": 1, "ascending": v.get("sort_asc", True)}
    report = {
        "report_title": v["title"], "report_subtitle": "", "dashboard_id": dash["pk"],
        "limit": v.get("limit", 1000), "sample_pct": "Off", "selected_segments": [], "report_derived_data": [],
        "click_behaviors": {}, "sort_orders_asc": {}, "user_settings": {}, **shelves,
        "core": {"viz_type": kind, "saved_shelf_sources": sources,
                 "shelves": [{"name": n, "shelf_type": s, "column_type": c} for n, s, c in SHELVES[kind]]},
    }
    return {"model": "reports.report", "pk": pk, "fields": {
        "report_name": "", "report_description": f"{dash['title']} / {sheet}", "dataset": dataset_pk,
        "workspace": workspace, "report_type": kind, "report_mode": "", "dashboard_url_name": "",
        "report_data": json.dumps({"report_data": report, "report_type": kind}), "shared_visual_dashboards": None,
        "parent_report": None, "uuid": uid("visual", dash["key"], sheet, v["title"]), "imported_uuid": None,
        "has_css_styles": False, "report_search_text": ""}}


def widgets(pairs: list[tuple[int, tuple]]) -> list[dict]:
    return [{"col": c, "row": r, "size_x": w, "size_y": h, "id": f"uri-{i}-widget-{pk}"}
            for i, (pk, (c, r, w, h)) in enumerate(pairs, 1)]


def build(conn_id: int, version: dict, workspace: int) -> dict:
    ds_pk = {k: DATASET_PK0 + i for i, k in enumerate(DATASETS)}
    types = {k: column_types(k) for k in DATASETS}
    visuals, dashboards, pk = [], [], VISUAL_PK0
    for d in DASHBOARDS:
        sheets = []
        for order, (sheet, items) in enumerate(d["sheets"], 1):
            placed = []
            for v in items:
                pk += 1
                visuals.append(visual_record(v, pk, d, sheet, types[v["ds"]], ds_pk[v["ds"]], workspace))
                placed.append((pk, v["pos"]))
            sheets.append({"sheet_id": order, "order": order, "sheet_handle_title": sheet, "behaviors": {},
                           "visual_widgets": widgets(placed), "control_widgets": []})
        body = {"report_title": d["title"], "numColumns": 64, "report_subtitle": d["subtitle"],
                "dashboard_widgets": sheets[0]["visual_widgets"], "dashboard_sheets": sheets,
                "user_settings": {"dashboard_width": "1280", "display_filters": "true",
                                  "permit_csv_download_dashboard": "true"},
                "global_control_widgets": [], "control_widgets": [], "click_behavior": {}}
        dashboards.append({"model": "reports.report", "pk": d["pk"], "fields": {
            "report_name": d["title"], "report_description": "docs/DATAVIZ.md",
            "dataset": ds_pk[d["ds"]], "workspace": workspace, "report_type": "dashboard", "report_mode": None,
            "dashboard_url_name": "", "report_data": json.dumps(body), "shared_visual_dashboards": "[]",
            "parent_report": None, "uuid": uid("dashboard", d["key"]), "imported_uuid": None,
            "has_css_styles": False, "report_search_text": None}})
    return {"segments": [], "staticasset": [], "dashboards": dashboards, "appgroupmembership": [],
            "reportannotation": [], "events": [], "customcss": [], "reportimage": [], "dateranges": [],
            "visuals": visuals, "colorpalette": [], "appgroups": [],
            "datasets": [dataset_record(k, ds_pk[k], types[k], conn_id) for k in DATASETS], "version": version}


class DataViz:
    """The Data Visualization instance at CASA_CDV_URL, authenticated with an API key."""

    def __init__(self):
        self.url = os.environ["CASA_CDV_URL"].split("/arc/")[0].rstrip("/")
        key = os.environ.get("CASA_CDV_API_KEY") or os.environ["CASA_VIZ_API_KEY"]
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"apikey {key}"

    def get(self, path: str, **params):
        r = self.s.get(self.url + path, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def connection(self) -> int:
        found = next((c for c in self.get("/arc/adminapi/v1/connections") if c["name"] == CONNECTION), None)
        if found:
            print(f"connection {CONNECTION}: exists ({found['id']})")
            return found["id"]
        params = {"HOST": os.environ.get("CASA_IMPALA_HOST", IMPALA_HOST),
                  "PORT": "443", "USERNAME": os.environ["CASA_IMPALA_USER"], "MODE": "http",
                  "HS2_HTTP_SQLPATH": "cliservice", "SOCK": "ssl", "AUTH": "ldap", "SOCKET_TIMEOUT": 600,
                  "IMPERSONATION": False, "APP_NAME": "viz", "CONCURRENCY": 100, "CONCURRENCY_USER": 5,
                  "QUERY_TIMEOUT": 120, "QUERY_LOADING_WARNING_SECONDS": 20, "CACHE": {"ENABLED": 1, "RETENTION": 1800}}
        body = {"name": CONNECTION, "type": "impyla", "info": {"PARAMS": params},
                "password": os.environ["CASA_IMPALA_PASSWORD"]}
        r = self.s.post(self.url + "/arc/adminapi/v1/connections", data={"data": json.dumps([body])}, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"connection {CONNECTION}: HTTP {r.status_code}")
        conn_id = r.json()[0]["id"]
        print(f"connection {CONNECTION}: created ({conn_id})")
        return conn_id

    def workspace(self) -> int:
        found = next((w for w in self.get("/arc/adminapi/v1/workspaces") if w["name"] == WORKSPACE), None)
        if found:
            print(f"workspace {WORKSPACE}: exists ({found['id']})")
            return found["id"]
        body = {"name": WORKSPACE, "desc": "CASA ALCO dashboards (rsingh-casa-alb)", "editable": True}
        r = self.s.post(self.url + "/arc/adminapi/v1/workspaces", data={"data": json.dumps([body])}, timeout=60)
        if r.status_code != 200:
            raise SystemExit(f"workspace {WORKSPACE}: HTTP {r.status_code} {r.text[:200]}")
        ws_id = r.json()[0]["id"]
        print(f"workspace {WORKSPACE}: created ({ws_id})")
        return ws_id

    def version(self) -> dict:
        return self.get("/arc/migration/api/export/", dashboards="[]", filename="version", dry_run="False")["version"]

    def verify(self) -> int:
        """Run every visual's query through the Data API (Data Visualization -> its connection -> Impala)."""
        ids = {d["name"]: d["id"] for d in self.get("/arc/adminapi/v1/datasets")}
        failed = 0
        for sheet, items in [s for d in DASHBOARDS for s in d["sheets"]]:
            for v in items:
                dims = v.get("dims", []) + v.get("x", []) + v.get("color", [])
                dsreq = {"version": 1, "type": "SQL", "limit": v.get("limit", 1000),
                         "dimensions": [{"type": "SIMPLE", "expr": f"[{c}] as '{a}'"} for c, a in dims],
                         "aggregates": [{"expr": f"{e} as '{a}'"} for e, a in v["measures"]],
                         "filters": v.get("filters", []), "dataset_id": ids[DATASETS[v["ds"]][0]]}
                r = self.s.post(self.url + "/arc/api/data", data={"version": 1, "dsreq": json.dumps(dsreq)},
                                timeout=300)
                rows = json.loads(r.json()["rows"]) if r.status_code == 200 else None
                ok = rows is not None
                failed += not ok
                head = rows[0] if rows else (None if ok else r.text[:200])
                print(f"{'ok  ' if ok else 'FAIL'} {sheet} / {v['title']}: "
                      f"{len(rows) if ok else r.status_code} rows, first {head}")
        return failed

    def import_file(self, path: Path) -> None:
        # the import ignores the workspace in the file: without the workspace name the
        # visuals land in the importing user's Private workspace
        with path.open("rb") as f:
            r = self.s.post(self.url + "/arc/migration/api/import/", files={"import_file": f},
                            data={"dry_run": "False", "dataconnection_name": CONNECTION, "workspace": WORKSPACE},
                            timeout=300)
        print(f"import: HTTP {r.status_code} {r.text[:500]}")
        if r.status_code != 200:
            raise SystemExit(1)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--no-import", action="store_true", help="views and the export file only")
    p.add_argument("--verify", action="store_true", help="only run every visual's query through the Data API")
    args = p.parse_args(argv)
    viz = DataViz()
    if args.verify:
        return 1 if viz.verify() else 0
    create_views()
    conn_id = viz.connection()
    ws_id = viz.workspace()
    doc = build(conn_id, viz.version(), ws_id)
    OUT.write_text(json.dumps(doc, indent=1) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(doc['datasets'])} datasets, {len(doc['visuals'])} visuals, "
          f"{len(doc['dashboards'])} dashboards")
    if not args.no_import:
        viz.import_file(OUT)
        print(f"open {viz.url}/arc/apps/ -> Visuals -> " + ", ".join(d["title"] for d in DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())

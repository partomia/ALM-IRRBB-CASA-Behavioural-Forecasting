"""The Data Visualization dashboard is generated from one declaration, reads only the
reporting views, carries the project prefix, and its export file has no credentials."""

import json
import re

import pytest

from tests.conftest import ROOT, _load

BUILD = _load(ROOT / "dataviz" / "build_dashboard.py")
VIEWS_SQL = (ROOT / "sql" / "dataviz_views.sql").read_text()
EXPORT_PATH = ROOT / "dataviz" / "casa_alco_dashboard.json"
VISUALS = [(d, sheet, v) for d in BUILD.DASHBOARDS for sheet, items in d["sheets"] for v in items]
CREATED = set(re.findall(rf"CREATE VIEW {BUILD.DB}\.(\w+) AS", VIEWS_SQL))


def test_names_carry_the_project_prefix():
    assert BUILD.DB.startswith("rsingh_casa_alb_")
    assert BUILD.CONNECTION.startswith("rsingh-casa-alb") and BUILD.WORKSPACE.startswith("rsingh-casa-alb")
    names = [n for n, _, _ in BUILD.DATASETS.values()] + [d["title"] for d in BUILD.DASHBOARDS]
    assert all(n.startswith("rsingh-casa-alb") for n in names), names
    # views only in this project's databases
    assert set(re.findall(r"(?:VIEW|FROM|JOIN)\s+(?:IF EXISTS\s+)?(\w+)\.", VIEWS_SQL)) == {BUILD.DB}


def test_every_dataset_is_a_reporting_view():
    assert {view for _, view, _ in BUILD.DATASETS.values()} == CREATED


def test_visuals_only_use_columns_of_their_dataset():
    for d, sheet, v in VISUALS:
        view = BUILD.DATASETS[v["ds"]][1]
        body = VIEWS_SQL.split(f"CREATE VIEW {BUILD.DB}.{view} AS", 1)[1].split(";", 1)[0]
        used = {c for c, _ in v.get("dims", []) + v.get("x", []) + v.get("color", [])}
        used |= set(re.findall(r"\[(\w+)\]", " ".join([e for e, _ in v["measures"]] + v.get("filters", []))))
        missing = {c for c in used if not re.search(rf"\b{c}\b", body)}
        assert not missing, (d["title"], sheet, v["title"], missing)


def test_visuals_fit_the_64_column_grid_without_overlap():
    for d in BUILD.DASHBOARDS:
        for sheet, items in d["sheets"]:
            cells = set()
            for v in items:
                c, r, w, h = v["pos"]
                assert c >= 1 and c + w - 1 <= 64, (sheet, v["title"])
                box = {(x, y) for x in range(c, c + w) for y in range(r, r + h)}
                assert not cells & box, (d["title"], sheet, v["title"])
                cells |= box


@pytest.mark.skipif(not EXPORT_PATH.exists(), reason="export file not built yet")
def test_the_export_file_matches_the_declaration_and_has_no_credentials():
    export = json.loads(EXPORT_PATH.read_text())
    assert len(export["visuals"]) == len(VISUALS)
    assert [d["fields"]["report_name"] for d in export["dashboards"]] == [d["title"] for d in BUILD.DASHBOARDS]
    assert {d["fields"]["dataset_detail"] for d in export["datasets"]} == {f"{BUILD.DB}.{v}" for v in CREATED}
    uuids = [a["fields"]["uuid"] for k in ("datasets", "visuals", "dashboards") for a in export[k]]
    assert len(set(uuids)) == len(uuids)
    text = json.dumps(export).lower()
    assert not any(w in text for w in ("password", "apikey", "bearer"))


def test_nothing_in_the_pipeline_reads_the_views():
    users = [p for p in [*ROOT.glob("cde/**/*.py"), *ROOT.glob("cai/**/*.py"), *ROOT.glob("casa/**/*.py"),
                         *ROOT.glob("app/**/*.py")] if re.search(r"\bv_(alco_split|sls|model_run|forecast_path|weekly_balance)\b", p.read_text())]
    assert users == []

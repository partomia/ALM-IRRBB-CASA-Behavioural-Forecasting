"""CASA behavioural forecasting for ALM and IRRBB: ALCO demo app.

  streamlit run app/streamlit_app.py            # storage from config/casa.yaml
  CASA_STORAGE_BACKEND=parquet streamlit run app/streamlit_app.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parent.parent
except NameError:
    ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from app import data  # noqa: E402
from casa.client import endpoint_configured, what_if  # noqa: E402
from casa.config import policy, settings, table  # noqa: E402
from casa.pipeline import portfolio_summary  # noqa: E402

st.set_page_config(page_title="CASA Behavioural Forecasting", page_icon=":bank:", layout="wide")

CATEGORY_LABEL = {"retail_transactional": "Retail transactional", "retail_non_transactional": "Retail non-transactional",
                  "wholesale": "Wholesale"}
COMPONENT_COLOR = {"runoff": "#e4572e", "cap_excess": "#f3a712", "core": "#2e86ab"}
COMPONENT_LABEL = {"runoff": "Run-off (P10)", "cap_excess": "Cap excess", "core": "Core"}


def crore(x: float) -> str:
    return f"₹{x:,.0f} cr"


def fan_chart(hist: pd.DataFrame, path: pd.DataFrame, core_amount: float | None = None, title: str = "") -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=hist["week_end_date"], y=hist["balance_inr_cr"], name="Actual weekly balance",
                             line=dict(color="#1b1b1e", width=1.6)))
    fig.add_trace(go.Scatter(x=path["week_end_date"], y=path["p90"], line=dict(width=0), showlegend=False,
                             hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=path["week_end_date"], y=path["p10"], fill="tonexty", name="P10-P90 band",
                             line=dict(width=0), fillcolor="rgba(46,134,171,0.22)"))
    fig.add_trace(go.Scatter(x=path["week_end_date"], y=path["p50"], name="P50", line=dict(color="#2e86ab", dash="dot")))
    fig.add_trace(go.Scatter(x=path["week_end_date"], y=path["p10"], name="P10 (pessimistic)",
                             line=dict(color="#e4572e", width=2)))
    if core_amount is not None:
        fig.add_hline(y=core_amount, line=dict(color="#2e86ab", dash="dash"),
                      annotation_text=f"Core (after cap) {crore(core_amount)}", annotation_position="bottom left")
    fig.update_layout(title=title, height=430, margin=dict(l=10, r=10, t=40, b=10), yaxis_title="INR crore",
                      legend=dict(orientation="h", y=-0.15), hovermode="x unified")
    return fig


def sls_chart(sls: pd.DataFrame, title: str = "") -> go.Figure:
    labels = [b["label"] for b in policy()["sls_buckets"]]
    fig = go.Figure()
    for comp in ("runoff", "cap_excess", "core"):
        part = sls[sls["component"] == comp].groupby("bucket_label")["amount_inr_cr"].sum()
        fig.add_trace(go.Bar(x=labels, y=[part.get(lbl, 0.0) for lbl in labels], name=COMPONENT_LABEL[comp],
                             marker_color=COMPONENT_COLOR[comp]))
    fig.update_layout(barmode="stack", title=title, height=380, margin=dict(l=10, r=10, t=40, b=10),
                      yaxis_title="INR crore", legend=dict(orientation="h", y=-0.3))
    return fig


# ---------------------------------------------------------------- sidebar
try:
    runs = data.read("casa_model_run").sort_values("as_of_date", ascending=False)
except Exception as e:  # no data yet / connection problem
    st.error(f"Could not read {table('casa_model_run')} via {settings()['storage']['backend']}: {e}")
    st.info("Run the monthly job first (cai/jobs/monthly_forecast.py), or set CASA_STORAGE_BACKEND=parquet "
            "with exported tables in data/parquet.")
    st.stop()

st.sidebar.title("ALCO run")
as_of = st.sidebar.selectbox("As-of date", runs["as_of_date"].tolist(), format_func=lambda d: d.strftime("%d %b %Y"))
run = runs[runs["as_of_date"] == as_of].iloc[0]
st.sidebar.caption(f"Run `{run.run_id}`  \nModel `{run.model_id}`  \nStorage: {data.storage().name}")
if run.backtest_p10_hit_rate is not None and not pd.isna(run.backtest_p10_hit_rate):
    st.sidebar.metric("Backtest trust number", f"{run.backtest_p10_hit_rate:.0%}",
                      help="Share of hidden segment-weeks where the actual balance stayed above the P10 path "
                           "(rolling 13-week backtests). A calibrated model gives about 90%.")
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()
st.sidebar.divider()
st.sidebar.caption("Demo of the data and modelling pipeline, not a validated regulatory model. "
                   "The bank's model risk and ALCO process set the final assumptions.")

split = data.for_run("casa_behavioural_split", as_of).sort_values(["irrbb_category", "segment_id"])
paths = data.for_run("casa_forecast_path", as_of)
sls_all = data.for_run("casa_sls_buckets", as_of)

st.title("CASA behavioural forecasting for ALM & IRRBB")
st.caption(f"As of {as_of:%d %b %Y}. Zero-shot TimesFM 2.5 forecasts of weekly segment balances, 52 weeks ahead. "
           "Core share = lowest P10 ÷ today's balance, capped at the BCBS category limit.")

tab_overview, tab_segment, tab_sls, tab_stress, tab_history = st.tabs(
    ["ALCO overview", "Segment forecast", "Structural liquidity (SLS)", "Stress what-if", "History & lineage"])

# ---------------------------------------------------------------- overview
with tab_overview:
    tot = portfolio_summary(split).set_index("irrbb_category")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total CASA", crore(tot.loc["TOTAL", "balance_inr_cr"]))
    c2.metric("Core", crore(tot.loc["TOTAL", "core_inr_cr"]), f"{tot.loc['TOTAL', 'core_share']:.1%} of balance",
              delta_color="off")
    c3.metric("Non-core", crore(tot.loc["TOTAL", "non_core_inr_cr"]), "slotted overnight for IRRBB", delta_color="off")
    c4.metric("Segments where the cap binds", f"{int(split['cap_binding'].sum())} of {len(split)}")

    left, right = st.columns([3, 2])
    with left:
        view = split[["segment_id", "irrbb_category", "balance_inr_cr", "core_share_model", "core_cap", "core_share",
                      "cap_binding", "core_inr_cr", "non_core_inr_cr", "backtest_p10_hit_rate"]].copy()
        view["irrbb_category"] = view["irrbb_category"].map(CATEGORY_LABEL)
        st.dataframe(view, hide_index=True, width="stretch", column_config={
            "segment_id": "Segment", "irrbb_category": "IRRBB category",
            "balance_inr_cr": st.column_config.NumberColumn("Balance (cr)", format="%.0f"),
            "core_share_model": st.column_config.ProgressColumn("Model core share", min_value=0, max_value=1,
                                                                format="percent"),
            "core_cap": st.column_config.NumberColumn("Cap", format="percent"),
            "core_share": st.column_config.NumberColumn("Core share", format="percent"),
            "cap_binding": st.column_config.CheckboxColumn("Cap binds"),
            "core_inr_cr": st.column_config.NumberColumn("Core (cr)", format="%.0f"),
            "non_core_inr_cr": st.column_config.NumberColumn("Non-core (cr)", format="%.0f"),
            "backtest_p10_hit_rate": st.column_config.NumberColumn("Backtest > P10", format="percent"),
        })
    with right:
        cat = tot.drop(index="TOTAL")
        fig = go.Figure([
            go.Bar(y=[CATEGORY_LABEL[c] for c in cat.index], x=cat["core_inr_cr"], name="Core", orientation="h",
                   marker_color=COMPONENT_COLOR["core"]),
            go.Bar(y=[CATEGORY_LABEL[c] for c in cat.index], x=cat["non_core_inr_cr"], name="Non-core",
                   orientation="h", marker_color=COMPONENT_COLOR["runoff"])])
        fig.update_layout(barmode="stack", height=300, margin=dict(l=10, r=10, t=30, b=10), title="By IRRBB category",
                          xaxis_title="INR crore", legend=dict(orientation="h", y=-0.3))
        st.plotly_chart(fig, width="stretch")
        binding = split[split["cap_binding"]].sort_values("core_share_model", ascending=False)
        if not binding.empty:
            b = binding.iloc[0]
            st.info(f"**{b.segment_id}**: the data says {b.core_share_model:.0%} is stable, the BCBS cap for "
                    f"{CATEGORY_LABEL[b.irrbb_category].lower()} deposits limits core to {b.core_cap:.0%}.")

# ---------------------------------------------------------------- segment
with tab_segment:
    seg = st.selectbox("Segment", split["segment_id"].tolist(), key="seg_forecast")
    s = split[split["segment_id"] == seg].iloc[0]
    hist = data.history(seg, as_of, weeks=156)
    path = paths[paths["segment_id"] == seg].sort_values("horizon_week")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Balance", crore(s.balance_inr_cr))
    c2.metric("Lowest P10 (52w)", crore(s.min_p10_inr_cr))
    c3.metric("Model core share", f"{s.core_share_model:.1%}")
    c4.metric("Core share (after cap)", f"{s.core_share:.1%}", "cap binds" if s.cap_binding else "below cap",
              delta_color="inverse" if s.cap_binding else "off")
    st.plotly_chart(fan_chart(hist, path, s.core_inr_cr, f"{seg}: last 3 years and 52-week forecast"),
                    width="stretch")
    bt = data.for_run("casa_backtest_metrics", as_of)
    bt = bt[bt["segment_id"] == seg].sort_values("cutoff_week_end")
    if not bt.empty:
        st.markdown("**Backtest** (13 weeks hidden at each cut-off)")
        st.dataframe(bt[["cutoff_week_end", "p10_hit_rate", "band_coverage", "mape_p50"]], hide_index=True,
                     column_config={"cutoff_week_end": "Cut-off week",
                                    "p10_hit_rate": st.column_config.NumberColumn("Actual > P10", format="percent"),
                                    "band_coverage": st.column_config.NumberColumn("Inside P10-P90", format="percent"),
                                    "mape_p50": st.column_config.NumberColumn("P50 abs. error", format="percent")})

# ---------------------------------------------------------------- SLS
with tab_sls:
    scope = st.selectbox("Scope", ["All segments"] + split["segment_id"].tolist(), key="sls_scope")
    part = sls_all if scope == "All segments" else sls_all[sls_all["segment_id"] == scope]
    st.plotly_chart(sls_chart(part, f"SLS slotting of CASA: {scope.lower() if scope == 'All segments' else scope}"),
                    width="stretch")
    pivot = (part.pivot_table(index=["bucket_order", "bucket_label"], columns="component", values="amount_inr_cr",
                              aggfunc="sum", fill_value=0.0).reset_index().drop(columns="bucket_order"))
    for comp in COMPONENT_LABEL:
        if comp not in pivot:
            pivot[comp] = 0.0
    pivot["total"] = pivot[list(COMPONENT_LABEL)].sum(axis=1)
    st.dataframe(pivot.rename(columns={"bucket_label": "Bucket", **COMPONENT_LABEL, "total": "Total"}).round(1),
                 hide_index=True, width="stretch")
    st.caption("Run-off: drop from today's balance to the running minimum of the P10 path, by the week it happens. "
               "Cap excess: stable per the model but above the BCBS cap, slotted in Day 1. Core: spread over the "
               "long buckets using the category weights in config/policy.yaml.")

# ---------------------------------------------------------------- stress
with tab_stress:
    st.markdown("Cut the most recent weeks of history (a deposit run) and re-forecast live. "
                + ("Calls the **CAI model endpoint**." if endpoint_configured()
                   else "No endpoint configured: scoring runs in this app."))
    c1, c2, c3 = st.columns([2, 1, 1])
    segs = c1.multiselect("Segments", split["segment_id"].tolist(), default=split["segment_id"].tolist()[:1])
    weeks = c2.slider("Weeks affected", 1, 26, 8)
    cut = c3.slider("Balance cut", 0.0, 0.40, 0.10, 0.01, format="%.2f")
    if st.button("Run stress scenario", type="primary", disabled=not segs, key="run_stress"):
        req_segs = [{"segment_id": sid, "irrbb_category": split.set_index("segment_id").loc[sid, "irrbb_category"],
                     "weekly_balance_inr_cr": [round(float(x), 2) for x in data.history(sid, as_of)["balance_inr_cr"]]}
                    for sid in segs]
        with st.spinner("Forecasting..."):
            try:
                base, src = what_if({"horizon_weeks": 52, "segments": req_segs})
                hit, _ = what_if({"horizon_weeks": 52, "segments": req_segs, "stress": {"weeks": weeks, "cut": cut}})
            except Exception as e:
                st.error(f"Scoring failed: {e}")
                st.stop()
        if "error" in base or "error" in hit:
            st.error(base.get("error") or hit.get("error"))
            st.stop()
        st.caption(f"Scored by: {src}")
        rows = []
        for b, h in zip(base["results"], hit["results"]):
            rows.append({"segment_id": b["segment_id"], "balance_base": b["balance_inr_cr"],
                         "balance_stress": h["balance_inr_cr"], "core_share_base": b["core_share"],
                         "core_share_stress": h["core_share"], "cap_binds_base": b["cap_binding"],
                         "cap_binds_stress": h["cap_binding"], "core_base": b["core_inr_cr"],
                         "core_stress": h["core_inr_cr"]})
        comp = pd.DataFrame(rows)
        c1, c2 = st.columns(2)
        c1.metric("Core, base", crore(comp["core_base"].sum()))
        c2.metric("Core, stressed", crore(comp["core_stress"].sum()),
                  f"{comp['core_stress'].sum() - comp['core_base'].sum():,.0f} cr")
        st.dataframe(comp, hide_index=True, width="stretch", column_config={
            "core_share_base": st.column_config.NumberColumn("Core share base", format="percent"),
            "core_share_stress": st.column_config.NumberColumn("Core share stressed", format="percent")})
        sls_b = pd.DataFrame([x for r in base["results"] for x in r["sls_buckets"]])
        sls_h = pd.DataFrame([x for r in hit["results"] for x in r["sls_buckets"]])
        l, r = st.columns(2)
        l.plotly_chart(sls_chart(sls_b, "SLS, base"), width="stretch")
        r.plotly_chart(sls_chart(sls_h, f"SLS, last {weeks} weeks cut {cut:.0%}"), width="stretch")
        first = hit["results"][0]
        h0 = data.history(first["segment_id"], as_of, weeks=104).copy()
        h0.loc[h0.index[-weeks:], "balance_inr_cr"] *= 1 - cut
        last = pd.Timestamp(h0["week_end_date"].max())
        p = pd.DataFrame({"week_end_date": [(last + pd.Timedelta(weeks=k + 1)).date() for k in range(len(first["p10"]))],
                          "p10": first["p10"], "p50": first["p50"], "p90": first["p90"]})
        st.plotly_chart(fan_chart(h0, p, first["core_inr_cr"], f"{first['segment_id']} under stress"),
                        width="stretch")

# ---------------------------------------------------------------- history
with tab_history:
    all_split = data.read("casa_behavioural_split")
    st.markdown("**Core share by ALCO run**. Every monthly run is kept, keyed by as-of date.")
    chosen = st.multiselect("Segments", sorted(all_split["segment_id"].unique()),
                            default=[s for s in ("SA_HNI", "CA_BANKS_FI", "SA_RETAIL_URBAN")
                                     if s in set(all_split["segment_id"])])
    fig = go.Figure()
    for sid in chosen:
        d = all_split[all_split["segment_id"] == sid].sort_values("as_of_date")
        fig.add_trace(go.Scatter(x=d["as_of_date"], y=d["core_share_model"], name=f"{sid} (model)", mode="lines+markers"))
        fig.add_trace(go.Scatter(x=d["as_of_date"], y=d["core_share"], name=f"{sid} (after cap)",
                                 mode="lines", line=dict(dash="dot")))
    fig.update_layout(height=380, yaxis_tickformat=".0%", margin=dict(l=10, r=10, t=20, b=10),
                      legend=dict(orientation="h", y=-0.2))
    st.plotly_chart(fig, width="stretch")

    dates = sorted(all_split["as_of_date"].unique())
    if len(dates) > 1:
        c1, c2 = st.columns(2)
        a = c1.selectbox("Compare run", dates, index=max(0, len(dates) - 4), format_func=lambda d: f"{d:%d %b %Y}")
        b = c2.selectbox("with run", dates, index=len(dates) - 1, format_func=lambda d: f"{d:%d %b %Y}")
        cols = ["segment_id", "balance_inr_cr", "core_share", "core_inr_cr"]
        diff = (all_split[all_split["as_of_date"] == a][cols]
                .merge(all_split[all_split["as_of_date"] == b][cols], on="segment_id", suffixes=(f" {a}", f" {b}")))
        diff["core change (cr)"] = diff[f"core_inr_cr {b}"] - diff[f"core_inr_cr {a}"]
        st.dataframe(diff.round(4), hide_index=True, width="stretch")

    st.markdown("**Lineage**: each run records the model, its Hugging Face revision and the Iceberg snapshot of the "
                "gold table it read.")
    st.dataframe(runs[["as_of_date", "run_id", "run_ts", "model_id", "model_revision", "history_weeks",
                       "source_table", "source_snapshot_id", "backtest_p10_hit_rate", "backtest_band_coverage",
                       "triggered_by"]], hide_index=True, width="stretch")
    snap = run.source_snapshot_id if isinstance(run.source_snapshot_id, str) and run.source_snapshot_id else "<snapshot_id>"
    st.markdown("Reproduce the exact input of this run in Hue (CDW Impala):")
    st.code(f"DESCRIBE HISTORY {table('casa_weekly_balance')};\n\n"
            f"SELECT * FROM {table('casa_weekly_balance')}\n  FOR SYSTEM_VERSION AS OF {snap}\n"
            f"  WHERE segment_id = 'SA_RETAIL_URBAN' ORDER BY week_end_date DESC LIMIT 10;", language="sql")

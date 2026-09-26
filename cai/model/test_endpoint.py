#!/usr/bin/env python3
"""
Smoke-test the what-if scorer with real history from gold.

  python cai/model/test_endpoint.py --local                 # in-process predict()
  python cai/model/test_endpoint.py                         # CAI endpoint (CASA_ENDPOINT_URL etc.)
  python cai/model/test_endpoint.py --segment SA_HNI --stress-weeks 8 --stress-cut 0.10
  python cai/model/test_endpoint.py --print-request > sample_request.json
"""

import argparse
import json
import os
import sys
from pathlib import Path


def _repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        return Path(os.getcwd())


sys.path.insert(0, str(_repo_root()))

import pandas as pd  # noqa: E402

from casa.storage import get_storage  # noqa: E402


def build_request(backend, segment, weeks, stress_weeks, stress_cut):
    df = get_storage(backend).read("casa_weekly_balance")
    df = df[df["segment_id"] == segment].sort_values("week_end_date")
    if df.empty:
        sys.exit(f"segment {segment} not found in gold")
    req = {"horizon_weeks": 52, "segments": [{
        "segment_id": segment, "irrbb_category": df["irrbb_category"].iloc[-1],
        "weekly_balance_inr_cr": [round(float(x), 2) for x in df["balance_inr_cr"].tail(weeks)]}]}
    if stress_cut:
        req["stress"] = {"weeks": stress_weeks, "cut": stress_cut}
    return req


def summary(resp):
    if "error" in resp:
        return f"ERROR: {resp['error']}"
    lines = []
    for r in resp["results"]:
        lines.append(f"{r['segment_id']} ({r['irrbb_category']}): balance {r['balance_inr_cr']:,.1f} cr, "
                     f"model core {r['core_share_model']:.1%}, core {r['core_share']:.1%}"
                     f"{' (cap binding)' if r['cap_binding'] else ''}, P10 week 52 {r['p10'][-1]:,.1f}")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--local", action="store_true", help="call predict() in-process instead of the endpoint")
    p.add_argument("--backend", default=None, choices=["impala", "parquet"])
    p.add_argument("--segment", default="SA_RETAIL_URBAN")
    p.add_argument("--weeks", type=int, default=260)
    p.add_argument("--stress-weeks", type=int, default=8)
    p.add_argument("--stress-cut", type=float, default=0.0)
    p.add_argument("--print-request", action="store_true")
    args = p.parse_args()

    req = build_request(args.backend, args.segment, args.weeks, args.stress_weeks, args.stress_cut)
    if args.print_request:
        print(json.dumps(req, indent=1))
        return
    if args.local:
        from cai.model.predict import predict

        base = predict({k: v for k, v in req.items() if k != "stress"})
        resp = predict(req) if args.stress_cut else None
    else:
        from casa.client import call_endpoint, endpoint_configured

        if not endpoint_configured():
            sys.exit("CASA_ENDPOINT_URL is not set (use --local to test in-process)")
        base = call_endpoint({k: v for k, v in req.items() if k != "stress"})
        resp = call_endpoint(req) if args.stress_cut else None
    print("Base:   " + summary(base))
    if resp:
        print(f"Stress (last {args.stress_weeks} weeks -{args.stress_cut:.0%}): " + summary(resp))
    print(pd.DataFrame(base["results"][0].get("sls_buckets", [])).to_string(index=False))


if __name__ == "__main__":
    main()

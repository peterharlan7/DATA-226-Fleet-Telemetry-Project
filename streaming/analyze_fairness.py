"""
Is detection equally reliable on every route? (B27, fairness across routes.)

A detector can only be as good as the data it gets. If some routes report less
often, or leave out fields the detectors need, riders on those routes get worse
monitoring without anyone noticing. This checks, per agency and per route:

  report_gap_s      median seconds between a vehicle's consecutive reports
  no_direction      share of in-service reports without direction_id
                    (the bunching detector skips these entirely)
  no_route          share of reports without route_id (not in service, or unknown)
  close_pairs       close pairs the bunching detector looked at (24 h replay)
  no_history        share of those it could NOT judge (too few recent reports)
  bunching          share it kept as bunching

and how unequal the routes are (best vs worst, spread). Inputs: the recording and
the alerts saved by the 24 h bunching replay.

Run from the repo root:
    python streaming/analyze_fairness.py
Writes benchmarks/results/fairness_by_route.csv and fairness_summary.json.
"""

import argparse
import glob
import json
import os
from collections import defaultdict

import pandas as pd


def load_alerts(path):
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        return pd.DataFrame(columns=["source", "route_id", "category", "is_bunching"])
    return pd.concat((pd.read_parquet(f, columns=["source", "route_id", "category", "is_bunching"])
                      for f in files), ignore_index=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--recording", default="data/recording_24h.jsonl")
    p.add_argument("--alerts", default="data/alerts/bunching_replay")
    p.add_argument("--min-pairs", type=int, default=30, help="only rank routes with at least this many close pairs")
    p.add_argument("--out", default="benchmarks/results")
    args = p.parse_args()

    # ---- Feed quality per route, from the recording ------------------------------------
    last_ts = {}                                   # (source, vehicle) -> last event_ts
    gaps = defaultdict(list)                       # (source, route) -> gaps in seconds
    counts = defaultdict(lambda: [0, 0])           # (source, route) -> [reports, without direction]
    no_route = defaultdict(lambda: [0, 0])         # source -> [reports, without route]
    with open(args.recording) as f:
        for line in f:
            try:
                ev = json.loads(line).get("event", {})
            except json.JSONDecodeError:
                continue
            if "lat" not in ev or ev.get("event_ts") is None:
                continue
            src, route = ev.get("source"), ev.get("route_id")
            no_route[src][0] += 1
            if not route:
                no_route[src][1] += 1
                continue
            key = (src, route)
            counts[key][0] += 1
            counts[key][1] += ev.get("direction_id") is None
            vkey = (src, ev.get("vehicle_id"))
            prev = last_ts.get(vkey)
            if prev is not None and ev["event_ts"] > prev:
                gap = (ev["event_ts"] - prev) / 1000
                if gap <= 900:                     # ignore gaps over 15 min (vehicle left service)
                    gaps[key].append(gap)
            if prev is None or ev["event_ts"] > prev:
                last_ts[vkey] = ev["event_ts"]

    rows = []
    for (src, route), (n, no_dir) in counts.items():
        g = sorted(gaps[(src, route)])
        rows.append({"source": src, "route_id": route, "reports": n,
                     "report_gap_s": g[len(g) // 2] if g else None,
                     "no_direction": round(no_dir / n, 4)})
    routes = pd.DataFrame(rows)

    # ---- Detector outcomes per route, from the 24 h bunching replay ------------------
    alerts = load_alerts(args.alerts)
    if len(alerts):
        per = (alerts.assign(no_history=alerts.category.eq("no_history"))
                     .groupby(["source", "route_id"])
                     .agg(close_pairs=("category", "size"), no_history=("no_history", "mean"),
                          bunching=("is_bunching", "mean"))
                     .reset_index())
        routes = routes.merge(per, on=["source", "route_id"], how="left")
    routes = routes.sort_values(["source", "reports"], ascending=[True, False])

    print("== By agency")
    for src in sorted(no_route):
        r = routes[routes.source == src]
        n_all, n_noroute = no_route[src]
        line = (f"  {src}: {len(r)} routes | median report gap {r.report_gap_s.median():.0f} s | "
                f"no route {n_noroute / n_all:.1%} | no direction "
                f"{(r.no_direction * r.reports).sum() / r.reports.sum():.1%}")
        if "close_pairs" in r and r.close_pairs.sum():
            w = r.close_pairs.fillna(0)
            line += (f" | close pairs {int(w.sum()):,}: no_history "
                     f"{(r.no_history.fillna(0) * w).sum() / w.sum():.1%}, bunching "
                     f"{(r.bunching.fillna(0) * w).sum() / w.sum():.1%}")
        print(line)

    ranked = routes[routes.get("close_pairs", pd.Series(dtype=float)).fillna(0) >= args.min_pairs] \
        if "close_pairs" in routes else routes.iloc[0:0]
    summary = {"routes": len(routes)}
    if len(ranked):
        cols = ["source", "route_id", "reports", "report_gap_s", "no_direction", "close_pairs",
                "no_history", "bunching"]
        print(f"\n== Routes the detector could judge LEAST (highest no_history, {args.min_pairs}+ close pairs)")
        print(ranked.sort_values("no_history", ascending=False)[cols].head(8).to_string(index=False))
        print("\n== Routes it could judge MOST")
        print(ranked.sort_values("no_history")[cols].head(8).to_string(index=False))
        q = ranked.no_history.quantile([0.1, 0.5, 0.9])
        corr = ranked[["report_gap_s", "no_history"]].corr().iloc[0, 1]
        print(f"\nSpread across {len(ranked)} routes: no_history 10th pct {q[0.1]:.0%}, median {q[0.5]:.0%}, "
              f"90th pct {q[0.9]:.0%}")
        print(f"Correlation between report gap and no_history: {corr:+.2f} "
              f"(positive = routes that report less often are judged less)")
        summary.update({"ranked_routes": len(ranked),
                        "no_history_p10": round(q[0.1], 4), "no_history_median": round(q[0.5], 4),
                        "no_history_p90": round(q[0.9], 4), "gap_vs_no_history_corr": round(corr, 3)})

    os.makedirs(args.out, exist_ok=True)
    routes.to_csv(os.path.join(args.out, "fairness_by_route.csv"), index=False)
    summary["agencies"] = {s: {"reports": a, "no_route_share": round(b / a, 4)} for s, (a, b) in no_route.items()}
    with open(os.path.join(args.out, "fairness_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSaved {args.out}/fairness_by_route.csv and fairness_summary.json")


if __name__ == "__main__":
    main()

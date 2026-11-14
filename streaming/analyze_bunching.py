"""
Why does the live bunching detector fire so often? (B12 analysis, before refining it.)

Reads the alerts the live detector saved (Parquet) and the raw recording
(ingestion/record_stream.py output), looks up both vehicles' reports around
each alert, and sorts every alert into one category:

  same_trip   both vehicles carry the same trip_id -> cars of ONE coupled train
  layover     both vehicles barely moved in the minutes before -> parked at a
              terminal or yard, not bunching in service
  one_parked  one moved, the other sat still -> a bus passing a parked one
  coupled     both moving, and EVERY time they reported together in the minutes
              before they were within --coupled meters -> likely cars of one train
              (different trip_ids per car), not two vehicles bunching
  same_stop   both moving in service and stopped at the same stop -> classic bunching
              (two buses serving one stop together)
  moving      both moving on different trips, not always together -> bunching candidates
  no_data     no recording data for that time (e.g. alerts before recording began)

Prints counts per category and agency, distances, top routes and examples,
and saves a categorized CSV + summary JSON to benchmarks/results/ for the slides.

Run from the repo root (fine while the recording is still being written):
    python streaming/analyze_bunching.py
    python streaming/analyze_bunching.py --alerts data/alerts/bunching_live --recording data/recording_24h.jsonl
"""

import argparse
import glob
import json
import os
import sys
import time
from collections import Counter, defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from streaming.detect_bunching import haversine_m  # noqa: E402

CATEGORIES = ["same_trip", "layover", "one_parked", "coupled", "same_stop", "moving", "no_data"]


def load_alerts(path):
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        sys.exit(f"No Parquet alert files under {path}")
    df = pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
    for c in ("window_start", "window_end"):
        df[c] = pd.to_datetime(df[c], utc=True)          # Spark wrote them in UTC
        df[c + "_ms"] = (df[c] - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1)
    return df


def load_reports(recording, wanted, t_min, t_max):
    """Position reports for only the vehicles in the alerts, within the alert time range."""
    reports = defaultdict(list)          # (source, vehicle_id) -> list of event dicts
    lines = bad = 0
    with open(recording) as f:
        for line in f:
            lines += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                bad += 1                 # last line may be half-written while recording runs
                continue
            ev = rec.get("event", rec)
            if "lat" not in ev or ev.get("event_ts") is None:
                continue                 # trip updates etc.
            key = (ev.get("source"), ev.get("vehicle_id"))
            if key in wanted and t_min <= ev["event_ts"] <= t_max:
                reports[key].append(ev)
    for evs in reports.values():
        evs.sort(key=lambda e: e["event_ts"])
    return reports, lines, bad


def moved_m(evs):
    """How far a vehicle got from its first position (meters); None if < 2 reports."""
    if len(evs) < 2:
        return None
    a = evs[0]
    return max(haversine_m(a["lat"], a["lon"], e["lat"], e["lon"]) for e in evs)


def nearest(evs, t):
    return min(evs, key=lambda e: abs(e["event_ts"] - t)) if evs else None


def is_stopped(e):
    s = e.get("current_status") if e else None
    return s in ("STOPPED_AT", 1, "1")


def pair_distances(ev_a, ev_b, max_gap_ms=60_000):
    """Distance between the two vehicles at each time both reported (within max_gap_ms)."""
    out = []
    for a in ev_a:
        b = nearest(ev_b, a["event_ts"])
        if b and abs(b["event_ts"] - a["event_ts"]) <= max_gap_ms:
            out.append(haversine_m(a["lat"], a["lon"], b["lat"], b["lon"]))
    return out


def categorize(alert, reports, context_ms, still_m, coupled_m):
    t0, t1 = alert["window_start_ms"], alert["window_end_ms"]
    mid = (t0 + t1) // 2
    out = {}
    sides = []
    for v in ("vehicle_a", "vehicle_b"):
        evs = reports.get((alert["source"], alert[v]), [])
        # Look BACK only (minutes before the window), the same information a live detector has.
        around = [e for e in evs if t0 - context_ms <= e["event_ts"] <= t1]
        sides.append((around, nearest(around, mid)))
    (ev_a, a), (ev_b, b) = sides
    if a is None or b is None:
        return "no_data", out

    move_a, move_b = moved_m(ev_a), moved_m(ev_b)
    out.update(trip_a=a.get("trip_id"), trip_b=b.get("trip_id"),
               stop_a=a.get("stop_id"), stop_b=b.get("stop_id"),
               status_a=a.get("current_status"), status_b=b.get("current_status"),
               speed_a=a.get("speed_mps"), speed_b=b.get("speed_mps"),
               moved_a_m=None if move_a is None else round(move_a),
               moved_b_m=None if move_b is None else round(move_b),
               reports_a=len(ev_a), reports_b=len(ev_b))

    if a.get("trip_id") and a.get("trip_id") == b.get("trip_id"):
        return "same_trip", out
    still_a = move_a is not None and move_a < still_m
    still_b = move_b is not None and move_b < still_m
    if still_a and still_b:
        return "layover", out
    if still_a or still_b:
        return "one_parked", out
    dists = pair_distances(ev_a, ev_b)
    out["times_compared"] = len(dists)
    out["max_pair_distance_m"] = round(max(dists)) if dists else None
    if len(dists) >= 3 and max(dists) < coupled_m:
        return "coupled", out
    if a.get("stop_id") and a.get("stop_id") == b.get("stop_id") and is_stopped(a) and is_stopped(b):
        return "same_stop", out
    return "moving", out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--alerts", default="data/alerts/bunching_live")
    p.add_argument("--recording", default="data/recording_24h.jsonl")
    p.add_argument("--context", type=int, default=300,
                   help="seconds before the alert window used to judge if a vehicle moved")
    p.add_argument("--still", type=float, default=50,
                   help="a vehicle that moved less than this many meters counts as parked")
    p.add_argument("--coupled", type=float, default=40,
                   help="always closer than this (m) while both moving -> counted as one coupled train")
    p.add_argument("--windows", type=int, default=None,
                   help="route-windows checked by the detector (printed when you stopped it), for the alert rate")
    p.add_argument("--out", default=None, help="CSV path (default benchmarks/results/bunching_analysis_<time>.csv)")
    args = p.parse_args()

    alerts = load_alerts(args.alerts)
    ctx = args.context * 1000
    wanted = set(zip(alerts["source"], alerts["vehicle_a"])) | set(zip(alerts["source"], alerts["vehicle_b"]))
    reports, lines, bad = load_reports(args.recording, wanted,
                                       alerts["window_start_ms"].min() - ctx,
                                       alerts["window_end_ms"].max())
    print(f"Alerts: {len(alerts):,} | recording lines read: {lines:,} (unreadable: {bad}) | "
          f"vehicles with reports: {len(reports):,} of {len(wanted):,}")

    cats, extras = [], []
    for alert in alerts.to_dict("records"):
        c, extra = categorize(alert, reports, ctx, args.still, args.coupled)
        cats.append(c)
        extras.append(extra)
    alerts["category"] = cats
    alerts = pd.concat([alerts, pd.DataFrame(extras)], axis=1)

    with_data = alerts[alerts.category != "no_data"]
    n = len(with_data)
    print(f"\n== Categories ({n:,} alerts with recording data; {len(alerts) - n:,} without)")
    counts = Counter(with_data.category)
    for c in CATEGORIES[:-1]:
        k = counts.get(c, 0)
        d = with_data[with_data.category == c].distance_m
        dist = f"median {d.median():.0f} m, range {d.min():.0f}-{d.max():.0f} m" if k else ""
        print(f"  {c:<11} {k:>6,}  {k / n:6.1%}  {dist}" if n else f"  {c:<11} 0")

    if n:
        print("\n== By agency")
        print(pd.crosstab(with_data.source, with_data.category).reindex(
            columns=[c for c in CATEGORIES if c in counts]).to_string())

        moving = with_data[with_data.category == "moving"]
        if args.windows:
            print(f"\nAlert rate: {len(alerts):,} alerts / {args.windows:,} route-windows before; "
                  f"{len(moving):,} 'moving' candidates if the others are filtered out")
        coupled = with_data[with_data.category == "coupled"]
        print("\n== Routes with 'coupled' alerts (rail lines here would confirm the coupled-train idea)")
        print(coupled.groupby(["source", "route_id"]).size().sort_values(ascending=False).head(10).to_string()
              if len(coupled) else "  none")
        real = with_data[with_data.category.isin(["same_stop", "moving"])]
        print(f"\nBunching after filtering (same_stop + moving): {len(real):,} of {n:,} alerts "
              f"({len(real) / n:.0%})")
        print("\n== Routes with the most 'moving' alerts (real bunching candidates)")
        print(moving.groupby(["source", "route_id"]).size().sort_values(ascending=False).head(10).to_string()
              if len(moving) else "  none")

        cols = ["source", "route_id", "direction_id", "window_start", "distance_m",
                "moved_a_m", "moved_b_m", "max_pair_distance_m", "status_a", "status_b"]
        for c in CATEGORIES[:-1]:
            ex = with_data[with_data.category == c]
            if len(ex):
                print(f"\n-- example {c}:")
                print(ex.reindex(columns=cols).head(3).to_string(index=False))

    out = args.out or f"benchmarks/results/bunching_analysis_{time.strftime('%Y%m%d_%H%M')}.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    alerts.drop(columns=[c for c in alerts.columns if c.endswith("_ms")]).to_csv(out, index=False)
    summary = {"alerts": len(alerts), "with_recording_data": n, "route_windows": args.windows,
               "context_s": args.context, "still_m": args.still, "coupled_m": args.coupled,
               "categories": {c: counts.get(c, 0) for c in CATEGORIES[:-1]},
               "no_data": int(len(alerts) - n)}
    with open(out.replace(".csv", ".json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSaved {out} and {out.replace('.csv', '.json')}")


if __name__ == "__main__":
    main()

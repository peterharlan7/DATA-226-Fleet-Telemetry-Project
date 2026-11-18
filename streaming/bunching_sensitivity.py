"""
Sensitivity check for the bunching detector's thresholds (B12 / B28).

Are the results (22% of close pairs are real bunching) an artifact of the thresholds
we picked? This re-runs the detector's own rule (bunching_pairs / classify_pair from
detect_bunching.py) over the 24 h recording, offline, with different settings:

  min_move_m      how far a bus must move in the look-back to count as moving   (default 50 m)
  min_history_s   how much look-back history a bus needs before we judge it     (default 30 s;
                  buses report about every 2 min, so values under ~120 s change nothing)
  lookback_s      how far back the detector looks                               (default 300 s)

One setting changes at a time; the others stay at the defaults. Close pairs are found
once (200 m, same as the detector); only the labelling changes between settings.
Offline means no watermark, so this is the exact scan (like B18): it can find slightly
more close pairs than the streaming run, which dropped a few late reports.

Run from the repo root (plain Python, no Spark or Kafka; a few minutes on 24 h):
    python streaming/bunching_sensitivity.py
    python streaming/bunching_sensitivity.py --recording data/recording_24h.jsonl
Writes benchmarks/results/bunching_sensitivity.csv
"""

import argparse
import json
import os
import sys
import time
from bisect import bisect_left
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from streaming.config import BUNCHING  # noqa: E402
from streaming.detect_bunching import (BUNCHING_CATEGORIES, _closest_pairs,  # noqa: E402
                                       classify_pair)

DEF_MOVE, DEF_HIST, DEF_LOOK = BUNCHING["min_move_m"], BUNCHING["min_history_s"], BUNCHING["lookback_s"]
SETTINGS = ([("min_move_m", v, v, DEF_HIST, DEF_LOOK) for v in (25, 50, 75, 100)] +
            [("min_history_s", v, DEF_MOVE, v, DEF_LOOK) for v in (15, 60, 120, 180)] +
            [("lookback_s", v, DEF_MOVE, DEF_HIST, v) for v in (180, 600)])
CATS = ["tandem", "same_stop", "moving", "layover", "one_parked", "no_history"]


def load(path):
    """Vehicle reports grouped by (source, route, direction), sorted by event time."""
    groups = defaultdict(dict)
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            e = rec.get("event", rec)
            if e.get("lat") is None or e.get("event_ts") is None or not e.get("event_id"):
                continue
            if e.get("route_id") is None or e.get("direction_id") is None:
                continue
            key = (e.get("source"), e["route_id"], e["direction_id"])
            groups[key][(e["vehicle_id"], e["event_ts"])] = {
                k: e.get(k) for k in ("vehicle_id", "event_ts", "lat", "lon", "trip_id", "current_status", "stop_id")}
    return {k: sorted(v.values(), key=lambda r: r["event_ts"]) for k, v in groups.items()}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--recording", default="data/recording_24h.jsonl")
    p.add_argument("--out", default="benchmarks/results/bunching_sensitivity.csv")
    args = p.parse_args()
    max_d, max_gap, tandem = BUNCHING["max_distance_m"], BUNCHING["max_time_gap_s"], BUNCHING["tandem_m"]
    win_ms = BUNCHING["window_s"] * 1000
    max_look_ms = max(s[4] for s in SETTINGS) * 1000

    t0 = time.time()
    groups = load(args.recording)
    print(f"Loaded {sum(len(v) for v in groups.values()):,} reports in {len(groups)} route-directions "
          f"({time.time() - t0:.0f} s)", flush=True)

    counts = {s: defaultdict(int) for s in SETTINGS}
    pairs_total = 0
    for gi, (key, reps) in enumerate(groups.items()):
        ts = [r["event_ts"] for r in reps]
        first = ts[0] // win_ms * win_ms
        for w in range(first, ts[-1] + 1, win_ms):               # every minute is the current minute once
            lo, hi = bisect_left(ts, w), bisect_left(ts, w + win_ms)
            if hi - lo < 2:
                continue
            current = reps[lo:hi]
            pairs = _closest_pairs(current, max_d, max_gap)
            if not pairs:
                continue
            pairs_total += len(pairs)
            window = reps[bisect_left(ts, w - max_look_ms):hi]
            for s in SETTINGS:
                _, _, move, hist, look = s
                start = w - look * 1000
                hist_by = defaultdict(dict)
                for r in window:
                    if r["event_ts"] >= start:
                        hist_by[r["vehicle_id"]][r["event_ts"]] = r
                for va, vb, _d, _g, ra, rb in pairs:
                    ha = sorted(hist_by[va].values(), key=lambda r: r["event_ts"])
                    hb = sorted(hist_by[vb].values(), key=lambda r: r["event_ts"])
                    cat, _, _ = classify_pair(ha, hb, ra, rb, move, hist, tandem, max_gap)
                    counts[s][cat] += 1
        if gi % 25 == 0:
            print(f"  {gi + 1}/{len(groups)} route-directions, {pairs_total:,} close pairs so far", flush=True)

    rows = []
    print(f"\n{pairs_total:,} close pairs (within {max_d} m). Share counted as real bunching:\n")
    print(f"{'changed':<15}{'value':>7}   {'bunching':>9}   {'parked/passing':>15}   {'cannot tell':>12}")
    for s in SETTINGS:
        name, val = s[0], s[1]
        c = counts[s]
        n = sum(c.values())
        bunch = sum(c[k] for k in BUNCHING_CATEGORIES)
        parked = c["layover"] + c["one_parked"]
        default = val == {"min_move_m": DEF_MOVE, "min_history_s": DEF_HIST, "lookback_s": DEF_LOOK}[name]
        print(f"{name:<15}{val:>7}   {bunch / n:>8.1%}   {parked / n:>15.1%}   {c['no_history'] / n:>12.1%}"
              f"{'   <- our setting' if default else ''}")
        rows.append({"changed": name, "value": val, "min_move_m": s[2], "min_history_s": s[3], "lookback_s": s[4],
                     "close_pairs": n, "bunching_share": round(bunch / n, 4), "parked_share": round(parked / n, 4),
                     "no_history_share": round(c["no_history"] / n, 4), **{c_: c[c_] for c_ in CATS}})
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    import csv
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\nSaved {args.out}  ({time.time() - t0:.0f} s)")
    print("Streaming run for comparison (benchmarks/results/bunching_replay_summary.json): 22% of 20,926 pairs.")


if __name__ == "__main__":
    main()

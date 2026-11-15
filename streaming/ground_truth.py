"""
Offline exact ground-truth scan for the three detectors (B18).

The streaming detectors see data once, in arrival order, and must close each
window after a short watermark, so reports that arrive late are dropped. This
batch job reads the WHOLE archive at once and applies the same rules with no
watermark, so nothing is late. Its output is the "right answer" the streaming
alerts are scored against (streaming/compare_ground_truth.py).

Same rules, same code: the bunching and stall scans call the detectors' own
window and rule functions (route_windows / window_pairs in detect_bunching.py,
vehicle_windows / window_stalls / StallTracker in detect_stalled.py) and read
their settings from streaming/config.py. Only the watermark is gone.

  bunching           Parquet archive of vehicle events -> every close pair per route-window,
                     with the same category and is_bunching flag as the live detector
  stalled            Parquet archive -> stall events (one per stall, full duration)
  delay_propagation  trip updates -> knock-on delays: a train leaves a station
                     >= --late s late and the NEXT train on the same route at that
                     station (within --next-within min) is late too. Same rule as the
                     B15 analysis (analyze_delay_propagation.py). Trip updates are not in
                     the Parquet archive yet, so this reads a trip-update Parquet folder
                     if there is one, else the JSON-lines recording.

Output tables (Parquet, overwritten on each run):
    data/ground_truth/bunching
    data/ground_truth/stalled
    data/ground_truth/delay_propagation
plus a summary with the settings and the archive's time range:
    benchmarks/results/ground_truth_summary.json

Run from the repo root:
    python streaming/ground_truth.py
    python streaming/ground_truth.py --archive data/raw/vehicle_events --stall-minutes 1
    python streaming/ground_truth.py --only bunching,stalled
Use the same settings (--max-distance, --stall-minutes, ...) as the streaming run you
want to score; compare_ground_truth.py warns when they differ.
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from streaming.config import BUNCHING, STALLED  # noqa: E402

OUT_DIR = "data/ground_truth"
SUMMARY_PATH = "benchmarks/results/ground_truth_summary.json"
DETECTORS = ("bunching", "stalled", "delay_propagation")
DELAY = {"late_s": 300, "next_within_min": 30}       # B15 defaults

DELAY_COLUMNS = ["source", "route_id", "stop_id", "trip_a", "trip_b", "ts_a", "ts_b",
                 "delay_a_s", "delay_b_s"]


# ---- Delay propagation (pure Python, testable without Spark) -------------------------

def knock_on_events(updates, late_s=DELAY["late_s"], next_within_min=DELAY["next_within_min"]):
    """Knock-on delays in trip updates: same rule as analyze_delay_propagation.py, part 2.

    updates: dicts with source, trip_id, route_id, stop_id, delay_sec, event_ts.
    Each train's LAST prediction for a station is when it was there. For every station
    (source, route, stop), consecutive visits within next_within_min minutes where BOTH
    trains are at least late_s late are one event. Returns a list of dicts (DELAY_COLUMNS).
    """
    route_of, last_at_stop = {}, {}
    for ev in updates:
        if ev.get("delay_sec") is None or not ev.get("trip_id") or not ev.get("stop_id"):
            continue
        src, trip, ts, stop = ev["source"], ev["trip_id"], ev["event_ts"], ev["stop_id"]
        if ev.get("route_id"):
            route_of[(src, trip)] = ev["route_id"]
        key = (src, trip, stop)
        if key not in last_at_stop or ts >= last_at_stop[key][0]:
            last_at_stop[key] = (ts, int(ev["delay_sec"]))

    by_station = defaultdict(list)
    for (src, trip, stop), (ts, d) in last_at_stop.items():
        by_station[(src, route_of.get((src, trip), "all"), stop)].append((ts, d, trip))
    window_ms = next_within_min * 60_000
    events = []
    for (src, route, stop), visits in by_station.items():
        visits.sort()
        for (t1, d1, trip1), (t2, d2, trip2) in zip(visits, visits[1:]):
            if t2 - t1 <= window_ms and d1 >= late_s and d2 >= late_s:
                events.append(dict(zip(DELAY_COLUMNS, (src, route, stop, trip1, trip2, t1, t2, d1, d2))))
    return sorted(events, key=lambda e: (e["ts_b"], e["source"], e["route_id"], e["stop_id"]))


def read_trip_updates(path):
    """Trip updates from a Parquet folder (trip-update archive) or a JSON-lines recording."""
    if os.path.isdir(path):
        import pandas as pd
        df = pd.read_parquet(path, columns=["source", "trip_id", "route_id", "stop_id",
                                            "delay_sec", "event_ts"])
        yield from df.to_dict("records")
        return
    with open(path) as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue                        # half-written last line
            ev = rec.get("event", rec)
            if "delay_sec" in ev:               # skip vehicle positions
                yield ev


# ---- Stall events --------------------------------------------------------------------

def stall_events(rows, still_m):
    """Turn per-window stall rows into stall events, exactly like the live detector.

    rows: dicts from window_stalls (source, vehicle_id, window_end_ms, since_ts, ...).
    Feeds them to the detector's own StallTracker in event-time order (window end, then
    since_ts, as the live detector sees them). Adds detected_window_end_ms to each stall:
    the window that first reported it (used to tell which stalls a stream had not closed yet).
    """
    from streaming.detect_stalled import StallTracker

    tracker = StallTracker(still_m)
    for r in sorted(rows, key=lambda r: (r["window_end_ms"], r["since_ts"], r["source"], r["vehicle_id"])):
        new = tracker.update(r)
        if new is not None:
            new["detected_window_end_ms"] = r["window_end_ms"]
    return tracker.stalls


# ---- Spark scans ---------------------------------------------------------------------

def scan_bunching(spark, events, args, out):
    from streaming.detect_bunching import route_windows, window_pairs

    pairs = window_pairs(route_windows(events, args), args).persist()
    pairs.write.mode("overwrite").parquet(out)
    counts = {r["category"]: r["count"] for r in pairs.groupBy("category").count().collect()}
    n_bunching = pairs.where("is_bunching").count()
    pairs.unpersist()
    return {"close_pairs": sum(counts.values()), "alerts": n_bunching, "categories": counts}


def scan_stalled(spark, events, args, out):
    from pyspark.sql.functions import col, timestamp_millis

    from streaming.detect_stalled import OUT_SCHEMA, vehicle_windows, window_stalls

    rows = [r.asDict() for r in window_stalls(vehicle_windows(events, args), args).collect()]
    stalls = stall_events(rows, args.still)
    df = spark.createDataFrame(stalls, schema=OUT_SCHEMA + ", detected_window_end_ms long")
    df.withColumn("since", timestamp_millis(col("since_ts"))).write.mode("overwrite").parquet(out)
    return {"stall_windows": len(rows), "alerts": len(stalls)}


def scan_delay(args, out):
    import pandas as pd

    path = next((p for p in args.trip_updates if os.path.exists(p)), None)
    if path is None:
        print(f"  delay_propagation: no trip updates found ({', '.join(args.trip_updates)}), skipped")
        return None
    events = knock_on_events(read_trip_updates(path), args.late, args.next_within)
    os.makedirs(out, exist_ok=True)
    for f in os.listdir(out):                         # overwrite, like Spark's mode("overwrite")
        os.remove(os.path.join(out, f))
    pd.DataFrame(events, columns=DELAY_COLUMNS).to_parquet(os.path.join(out, "part-0.parquet"), index=False)
    return {"input": path, "alerts": len(events)}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--archive", default="data/raw/vehicle_events", help="Parquet archive of vehicle events")
    p.add_argument("--trip-updates", default="data/raw/trip_updates,data/recording_24h.jsonl",
                   help="trip-update Parquet folder or JSON-lines recording; first that exists is used")
    p.add_argument("--out", default=OUT_DIR)
    p.add_argument("--summary", default=SUMMARY_PATH)
    p.add_argument("--only", default=",".join(DETECTORS), help="comma-separated subset of " + ", ".join(DETECTORS))
    # Bunching (same flags and defaults as detect_bunching.py)
    p.add_argument("--max-distance", type=float, default=BUNCHING["max_distance_m"])
    p.add_argument("--max-time-gap", type=float, default=BUNCHING["max_time_gap_s"])
    p.add_argument("--window", type=int, default=BUNCHING["window_s"])
    p.add_argument("--lookback", type=int, default=BUNCHING["lookback_s"])
    p.add_argument("--min-move", type=float, default=BUNCHING["min_move_m"])
    p.add_argument("--min-history", type=float, default=BUNCHING["min_history_s"])
    p.add_argument("--tandem", type=float, default=BUNCHING["tandem_m"])
    # Stalled (same flags and defaults as detect_stalled.py)
    p.add_argument("--stall-minutes", type=float, default=STALLED["stall_minutes"])
    p.add_argument("--still", type=float, default=STALLED["still_m"])
    p.add_argument("--min-reports", type=int, default=STALLED["min_reports"])
    # Delay propagation (same as analyze_delay_propagation.py)
    p.add_argument("--late", type=int, default=DELAY["late_s"])
    p.add_argument("--next-within", type=int, default=DELAY["next_within_min"])
    args = p.parse_args()
    args.trip_updates = [s for s in args.trip_updates.split(",") if s]
    only = [d.strip() for d in args.only.split(",") if d.strip()]
    unknown = set(only) - set(DETECTORS)
    if unknown:
        p.error(f"unknown detector(s): {', '.join(sorted(unknown))}")

    started = time.time()
    summary = {"archive": args.archive, "created": time.strftime("%Y-%m-%d %H:%M:%S"),
               "settings": {
                   "bunching": {k: getattr(args, k) for k in ("max_distance", "max_time_gap", "window",
                                                             "lookback", "min_move", "min_history", "tandem")},
                   "stalled": {k: getattr(args, k) for k in ("stall_minutes", "still", "min_reports")},
                   "delay_propagation": {"late": args.late, "next_within": args.next_within}},
               "detectors": {}}

    if {"bunching", "stalled"} & set(only):
        from pyspark.sql.functions import max as max_, min as min_

        from streaming.stream_to_parquet import build_spark

        if not os.path.isdir(args.archive):
            sys.exit(f"No Parquet archive at {args.archive}. Run streaming/stream_to_parquet.py first.")
        spark = build_spark(use_kafka=False)
        events = spark.read.parquet(args.archive).persist()
        rng = events.agg(min_("event_ts"), max_("event_ts")).first()
        n = events.count()
        summary.update(archive_rows=n, first_event_ts=rng[0], last_event_ts=rng[1])
        print(f"Archive {args.archive}: {n:,} vehicle events")
        for name, scan in (("bunching", scan_bunching), ("stalled", scan_stalled)):
            if name in only:
                t0 = time.time()
                res = scan(spark, events, SimpleNamespace(**vars(args)), os.path.join(args.out, name))
                res["seconds"] = round(time.time() - t0, 1)
                summary["detectors"][name] = res
                print(f"  {name}: {res['alerts']:,} ground-truth alerts ({res['seconds']} s)")
        events.unpersist()
        spark.stop()

    if "delay_propagation" in only:
        res = scan_delay(args, os.path.join(args.out, "delay_propagation"))
        if res:
            summary["detectors"]["delay_propagation"] = res
            print(f"  delay_propagation: {res['alerts']:,} knock-on delays from {res['input']}")

    summary["seconds"] = round(time.time() - started, 1)
    os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
    with open(args.summary, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"Ground truth saved under {args.out}/ | summary {args.summary}")


if __name__ == "__main__":
    main()

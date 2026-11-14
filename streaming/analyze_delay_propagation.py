"""
Delay propagation in BART trip updates (B15, offline analysis of the 24 h recording).

BART's GTFS-realtime feed sends, for every running train, the predicted delay at
each of its upcoming stations. Two questions:

  1. WITHIN a trip: does a delay grow as the train moves down the line
     (it propagates), shrink (the train recovers time), or stay flat?
     Compared on each snapshot: first upcoming station vs last.
  2. ACROSS trips (knock-on delays): when a train leaves a station 5+ minutes
     late, is the NEXT train on the same route at that station also late?
     Compared with how often any train is late there (the baseline). A lift
     above 1 means delays spread from train to train.

Notes on the data:
  - BART sends no stop_sequence, so the order of stations is the order inside
    each update (the recording keeps it).
  - Delays are PREDICTIONS made by BART, not measured arrivals; we use each
    train's last prediction for a station (made just before it got there).
  - "When the train was at the station" = the last time that station appeared
    in its updates.

Run from the repo root:
    python streaming/analyze_delay_propagation.py
Writes benchmarks/results/delay_propagation.json and delay_by_hour.csv.
"""

import argparse
import json
import os
import statistics
from collections import defaultdict
from datetime import datetime


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--recording", default="data/recording_24h.jsonl")
    p.add_argument("--late", type=int, default=300, help="'late' = delay of at least this many seconds")
    p.add_argument("--change", type=int, default=60, help="a delay change smaller than this counts as flat")
    p.add_argument("--next-within", type=int, default=30, help="next train must reach the station within N minutes")
    p.add_argument("--out", default="benchmarks/results")
    args = p.parse_args()

    snapshots = defaultdict(list)          # (source, trip_id, event_ts) -> [(stop_id, delay)] in feed order
    route_of = {}
    last_at_stop = {}                      # (source, trip_id, stop_id) -> (event_ts, delay)
    n_lines = 0
    with open(args.recording) as f:
        for line in f:
            try:
                ev = json.loads(line).get("event", {})
            except json.JSONDecodeError:
                continue
            if ev.get("delay_sec") is None or not ev.get("trip_id") or not ev.get("stop_id"):
                continue
            n_lines += 1
            src, trip, ts, stop, d = ev["source"], ev["trip_id"], ev["event_ts"], ev["stop_id"], ev["delay_sec"]
            snapshots[(src, trip, ts)].append((stop, d))
            if ev.get("route_id"):
                route_of[(src, trip)] = ev["route_id"]
            key = (src, trip, stop)
            if key not in last_at_stop or ts >= last_at_stop[key][0]:
                last_at_stop[key] = (ts, d)

    # ---- 1. Within a trip ------------------------------------------------------------
    grows = shrinks = flat = 0
    growth = []
    trips_seen = set()
    for (src, trip, ts), stops in snapshots.items():
        if len(stops) < 2:
            continue
        trips_seen.add((src, trip))
        change = stops[-1][1] - stops[0][1]
        growth.append(change)
        if change >= args.change:
            grows += 1
        elif change <= -args.change:
            shrinks += 1
        else:
            flat += 1
    n_snap = grows + shrinks + flat
    print(f"Trip-update records: {n_lines:,} | snapshots with 2+ stations: {n_snap:,} "
          f"from {len(trips_seen):,} trips")
    print("\n== 1. Within a trip (first vs last upcoming station)")
    if n_snap:
        print(f"  delay grows down the line (propagates): {grows / n_snap:5.1%}")
        print(f"  delay shrinks (train recovers time):    {shrinks / n_snap:5.1%}")
        print(f"  flat (change under {args.change} s):            {flat / n_snap:5.1%}")
        big = [g for g in growth if abs(g) >= args.change]
        if big:
            print(f"  when it changes: median {statistics.median(big):+.0f} s, "
                  f"largest growth {max(big):+} s, largest recovery {min(big):+} s")

    # ---- 2. Across trips at the same station -----------------------------------------
    by_station = defaultdict(list)         # (source, route, stop) -> [(ts, delay, trip)]
    for (src, trip, stop), (ts, d) in last_at_stop.items():
        by_station[(src, route_of.get((src, trip), "all"), stop)].append((ts, d, trip))
    pairs = late_pairs = late_after_late = late_any = 0
    window = args.next_within * 60_000
    for visits in by_station.values():
        visits.sort()
        for (t1, d1, _), (t2, d2, _) in zip(visits, visits[1:]):
            if t2 - t1 > window:
                continue
            pairs += 1
            late_any += d2 >= args.late
            if d1 >= args.late:
                late_pairs += 1
                late_after_late += d2 >= args.late
    print(f"\n== 2. Across trips (same route and station, next train within {args.next_within} min)")
    base = late_any / pairs if pairs else 0
    cond = late_after_late / late_pairs if late_pairs else 0
    if pairs:
        print(f"  any next train late (>= {args.late // 60} min):          {base:5.1%}  ({late_any:,} of {pairs:,})")
        if late_pairs:
            print(f"  next train late when this one was late: {cond:5.1%}  ({late_after_late:,} of {late_pairs:,})")
            print(f"  lift: {cond / base:.1f}x" if base else "  lift: n/a (no late trains at all)")
            ontime_pairs = pairs - late_pairs
            if ontime_pairs:
                after_ontime = (late_any - late_after_late) / ontime_pairs
                print(f"  next train late when this one was on time: {after_ontime:5.1%}")
        else:
            print("  no train was late enough to test knock-on delays")

    # ---- Delay by hour (for a chart) --------------------------------------------------
    hourly = defaultdict(list)
    for (src, trip, stop), (ts, d) in last_at_stop.items():
        hourly[datetime.fromtimestamp(ts / 1000).hour].append(d)
    print("\n== Average delay at arrival by hour (your local time)")
    rows = []
    for h in sorted(hourly):
        ds = hourly[h]
        late_share = sum(d >= args.late for d in ds) / len(ds)
        rows.append((h, len(ds), round(statistics.mean(ds), 1), round(late_share, 4)))
        print(f"  {h:02d}:00  mean {statistics.mean(ds):6.0f} s | late {late_share:5.1%} | {len(ds):,} arrivals")

    os.makedirs(args.out, exist_ok=True)
    result = {"settings": vars(args), "trip_update_records": n_lines, "snapshots": n_snap,
              "trips": len(trips_seen),
              "within_trip": {"grows": grows, "shrinks": shrinks, "flat": flat},
              "across_trips": {"pairs": pairs, "baseline_late_share": round(base, 4),
                               "late_after_late_share": round(cond, 4), "late_pairs": late_pairs,
                               "lift": round(cond / base, 2) if base and late_pairs else None}}
    with open(os.path.join(args.out, "delay_propagation.json"), "w") as f:
        json.dump(result, f, indent=2)
    with open(os.path.join(args.out, "delay_by_hour.csv"), "w") as f:
        f.write("hour,arrivals,mean_delay_s,late_share\n")
        for r in rows:
            f.write(",".join(map(str, r)) + "\n")
    print(f"\nSaved {args.out}/delay_propagation.json and delay_by_hour.csv")


if __name__ == "__main__":
    main()

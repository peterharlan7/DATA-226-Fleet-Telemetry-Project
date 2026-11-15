"""
What are the stalls B16 found? (B16 analysis, like analyze_bunching.py for B12.)

Reads the stall report written by detect_stalled.py and checks the main suspicion:
are most "stalls" really buses on layover at terminals that report IN_TRANSIT_TO?
A terminal shows up as ONE spot where many DIFFERENT vehicles stall, again and again.
A breakdown or incident is a one-off spot.

Groups stalls into ~150 m spots and prints:
  - how many stalls happen at "recurring" spots (>= --min-vehicles different vehicles)
  - the top spots with their routes (check a few on a map: terminals?)
  - durations, agencies, hour of day
and saves a CSV of stalls with a recurring_spot flag to benchmarks/results/.

Run from the repo root:
    python streaming/analyze_stalls.py --report benchmarks/results/stalled_replay.json
"""

import argparse
import json
import os
import time

import pandas as pd


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--report", default="benchmarks/results/stalled_replay.json")
    p.add_argument("--grid", type=float, default=0.0015, help="spot size in degrees (~150 m)")
    p.add_argument("--min-vehicles", type=int, default=3,
                   help="a spot where at least this many different vehicles stalled is 'recurring' (terminal-like)")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    with open(args.report) as f:
        rep = json.load(f)
    df = pd.DataFrame(rep["stalls"])
    if df.empty:
        print("No stalls in the report.")
        return
    df["spot"] = list(zip((df.lat / args.grid).round().astype(int), (df.lon / args.grid).round().astype(int)))
    df["hour"] = pd.to_datetime(df.since_ts, unit="ms", utc=True).dt.tz_convert("America/Los_Angeles").dt.hour

    spots = (df.groupby("spot")
               .agg(stalls=("vehicle_id", "size"), vehicles=("vehicle_id", "nunique"),
                    source=("source", "first"), routes=("route_id", lambda r: ",".join(sorted(set(map(str, r))))[:40]),
                    median_min=("minutes", "median"), lat=("lat", "mean"), lon=("lon", "mean"))
               .sort_values("stalls", ascending=False))
    recurring = set(spots[spots.vehicles >= args.min_vehicles].index)
    df["recurring_spot"] = df.spot.isin(recurring)

    n = len(df)
    r = int(df.recurring_spot.sum())
    print(f"Stalls: {n:,} by {df.vehicle_id.nunique():,} vehicles at {len(spots):,} spots (~150 m)")
    print(f"At recurring spots (>= {args.min_vehicles} different vehicles): {r:,} stalls ({r / n:.0%}) "
          f"at {len(recurring)} spots -> terminal / layover candidates")
    print(f"One-off spots: {n - r:,} stalls ({(n - r) / n:.0%}) -> breakdown / incident / traffic candidates")

    print("\n== By agency")
    print(pd.crosstab(df.source, df.recurring_spot.map({True: "recurring", False: "one_off"})).to_string())

    print("\n== Duration (minutes)")
    for name, part in (("recurring", df[df.recurring_spot]), ("one_off", df[~df.recurring_spot])):
        if len(part):
            q = part.minutes.quantile([0.5, 0.9]).round(1).tolist()
            print(f"  {name:<9} median {q[0]}, 90th pct {q[1]}, longest {part.minutes.max():.0f}")

    print("\n== Top 10 spots (paste lat,lon into Google Maps to see if it's a terminal)")
    top = spots.head(10).copy()
    top["lat,lon"] = top.lat.round(5).astype(str) + "," + top.lon.round(5).astype(str)
    print(top[["source", "stalls", "vehicles", "median_min", "routes", "lat,lon"]].to_string(index=False))

    print("\n== One-off stalls by hour of day (Pacific)")
    print(df[~df.recurring_spot].groupby("hour").size().to_string())

    out = args.out or f"benchmarks/results/stalls_analysis_{time.strftime('%Y%m%d_%H%M')}.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)
    df.drop(columns=["spot"]).to_csv(out, index=False)
    print(f"\nSaved {out}")


if __name__ == "__main__":
    main()

"""
Check the stalled-vehicle detector against stalls planted by fake_feed.py (B16 testing).

A planted stall is caught if the detector reported the same vehicle stalled, with
a stall that overlaps the planted time. Reports recall and any other stalls
(false alarms: the fake feed has no other stalls).

Run from the repo root, after stopping the detector:
    python streaming/evaluate_stalled.py

Stalls that end in the last --settle seconds before the feed stopped are "pending":
their windows can only close when newer data pushes the watermark past them.
"""

import argparse
import json
import os

import pandas as pd


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--planted", default="data/planted_stalls.jsonl")
    p.add_argument("--alerts", default="data/alerts/stalled")
    p.add_argument("--settle", type=int, default=120, help="seconds before feed stop that can't be judged yet")
    args = p.parse_args()

    planted, feed_stopped = [], None
    with open(args.planted) as f:
        for line in f:
            rec = json.loads(line)
            if rec["type"] == "planted_stall":
                planted.append(rec)
            elif rec["type"] == "feed_stopped":
                feed_stopped = rec["ts"]

    alerts = pd.read_parquet(args.alerts) if os.path.isdir(args.alerts) else \
        pd.DataFrame(columns=["vehicle_id", "since_ts", "last_ts", "minutes"])

    caught, missed, pending, matched = [], [], [], set()
    for ev in planted:
        hits = alerts[(alerts["vehicle_id"] == ev["vehicle_id"])
                      & (alerts["since_ts"] <= ev["end_ts"])
                      & (alerts["last_ts"] >= ev["start_ts"])]
        if len(hits):
            caught.append(ev)
            matched.update(hits.index)
        elif feed_stopped and ev["end_ts"] > feed_stopped - args.settle * 1000:
            pending.append(ev)
        else:
            missed.append(ev)

    judged = len(caught) + len(missed)
    print(f"Planted stalls: {len(planted)}  (judged {judged}, pending {len(pending)})")
    print(f"Caught: {len(caught)}   Missed: {len(missed)}")
    if judged:
        print(f"Recall on planted stalls: {len(caught) / judged:.0%}")
    for ev in missed:
        print(f"  MISSED vehicle {ev['vehicle_id']} route {ev['route_id']}")
    other = len(alerts) - len(matched)
    print(f"Stall alerts: {len(alerts)}  |  matched to planted stalls: {len(matched)}  |  false alarms: {other}")


if __name__ == "__main__":
    main()

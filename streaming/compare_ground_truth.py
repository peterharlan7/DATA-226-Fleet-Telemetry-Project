"""
Score the streaming detectors against the offline ground truth (B18).

Reads the ground-truth tables from streaming/ground_truth.py and the alerts the
streaming detectors saved, and reports precision and recall per detector:

    precision = streaming alerts that are in the ground truth / all streaming alerts
    recall    = ground-truth alerts the stream also raised   / all ground-truth alerts

How alerts are matched:
  bunching           same source, route, direction, minute (window_start) and pair of buses.
                     Both sides use the same windows, so this is an exact match.
  stalled            same source and vehicle, and the two stalls overlap in time.
  delay_propagation  same source, route, station and pair of trains. There is no streaming
                     delay detector yet, so this part only runs if --delay-alerts exists.

Ground-truth alerts whose window ends within --watermark seconds of the last event in
the archive are "pending": the stream could not have closed those windows yet. They are
left out of recall. Use --since / --until to score only the time the streaming job ran.

For every miss and false alarm the script also says why, when it can tell:
  bunching  the stream saw the same close pair but labelled it differently (e.g. no_history
            because late look-back reports were dropped), or never saw the pair at all.

Run from the repo root, after streaming/ground_truth.py:
    python streaming/compare_ground_truth.py
    python streaming/compare_ground_truth.py --bunching-alerts data/alerts/bunching_replay \\
        --stalled-alerts data/alerts/stalled_replay --since 2026-11-12T08:00
Writes benchmarks/results/ground_truth_comparison.json.
"""

import argparse
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from streaming.config import BUNCHING, STALLED  # noqa: E402

BUNCH_KEY = ["source", "route_id", "direction_id", "window_start_ms", "vehicle_a", "vehicle_b"]
DELAY_KEY = ["source", "route_id", "stop_id", "trip_a", "trip_b"]


def to_ms(series):
    ts = pd.to_datetime(series, utc=True)
    return (ts - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1)


def parse_time(s):
    """Epoch ms, or an ISO time (UTC unless it has an offset)."""
    if s is None:
        return None
    if str(s).isdigit():
        return int(s)
    ts = pd.Timestamp(s)
    return int((ts if ts.tzinfo else ts.tz_localize("UTC")).timestamp() * 1000)


def read_table(path):
    return pd.read_parquet(path) if path and os.path.isdir(path) and os.listdir(path) else None


def scores(tp_stream, n_stream, tp_truth, n_truth):
    """Precision from the stream's side, recall from the ground truth's side (they can differ
    for stalls, where one long stall may match two shorter ones)."""
    return {"streaming_alerts": n_stream, "ground_truth_alerts": n_truth,
            "true_positives": tp_stream, "false_positives": n_stream - tp_stream,
            "false_negatives": n_truth - tp_truth,
            "precision": round(tp_stream / n_stream, 4) if n_stream else None,
            "recall": round(tp_truth / n_truth, 4) if n_truth else None}


def in_range(ms, since, until):
    keep = pd.Series(True, index=ms.index)
    if since is not None:
        keep &= ms >= since
    if until is not None:
        keep &= ms <= until
    return keep


# ---- Bunching ------------------------------------------------------------------------

def bunching_frame(df):
    """Normalise an alerts table so streaming and ground-truth rows compare equal."""
    df = df.copy()
    df["window_start_ms"] = to_ms(df["window_start"])
    df["window_end_ms"] = to_ms(df["window_end"])
    df["direction_id"] = df["direction_id"].astype("Int64").astype(str)
    for c in ("source", "route_id", "vehicle_a", "vehicle_b"):
        df[c] = df[c].astype(str)
    # Same pair in either order (the detector already sorts them; this is just a guard).
    swap = df["vehicle_a"] > df["vehicle_b"]
    df.loc[swap, ["vehicle_a", "vehicle_b"]] = df.loc[swap, ["vehicle_b", "vehicle_a"]].values
    if "is_bunching" not in df.columns:              # raw detector output: every close pair counts
        df["is_bunching"], df["category"] = True, "raw"
    return df.drop_duplicates(BUNCH_KEY)


def compare_bunching(truth, stream, cutoff_ms, since, until, examples):
    truth, stream = bunching_frame(truth), bunching_frame(stream)
    truth = truth[in_range(truth["window_start_ms"], since, until)]
    stream = stream[in_range(stream["window_start_ms"], since, until)]
    pending = truth["window_end_ms"] > cutoff_ms
    truth_all, truth = truth, truth[~pending]

    t_alerts = truth[truth["is_bunching"]]
    s_alerts = stream[stream["is_bunching"]]
    t_keys = set(map(tuple, t_alerts[BUNCH_KEY].values))
    s_keys = set(map(tuple, s_alerts[BUNCH_KEY].values))
    tp = len(t_keys & s_keys)
    res = scores(tp, len(s_keys), tp, len(t_keys))
    res["pending"] = int((pending & truth_all["is_bunching"]).sum())

    # Why: look the key up on the other side among ALL close pairs (not only bunching ones).
    s_cat = dict(zip(map(tuple, stream[BUNCH_KEY].values), stream["category"]))
    t_cat = dict(zip(map(tuple, truth_all[BUNCH_KEY].values), truth_all["category"]))
    fn_why, fp_why = {}, {}
    for k in t_keys - s_keys:
        why = f"stream labelled it {s_cat[k]}" if k in s_cat else "stream never saw the pair close"
        fn_why[why] = fn_why.get(why, 0) + 1
    for k in s_keys - t_keys:
        why = f"ground truth labels it {t_cat[k]}" if k in t_cat else "not a close pair in ground truth"
        fp_why[why] = fp_why.get(why, 0) + 1
    res["false_negative_reasons"], res["false_positive_reasons"] = fn_why, fp_why
    res["by_category_recall"] = {
        cat: round(sum(tuple(k) in s_keys for k in grp[BUNCH_KEY].values) / len(grp), 4)
        for cat, grp in t_alerts.groupby("category")}
    res["missed_examples"] = [dict(zip(BUNCH_KEY, k), why=(f"stream labelled it {s_cat[k]}" if k in s_cat
                                                           else "stream never saw the pair close"))
                              for k in sorted(t_keys - s_keys)[:examples]]
    res["false_alarm_examples"] = [dict(zip(BUNCH_KEY, k)) for k in sorted(s_keys - t_keys)[:examples]]
    return res


# ---- Stalled -------------------------------------------------------------------------

def match_intervals(left, right):
    """For each row of `left`, does `right` have a stall of the same vehicle overlapping it?"""
    by_vehicle = {}
    for r in right.itertuples():
        by_vehicle.setdefault((r.source, r.vehicle_id), []).append((r.since_ts, r.last_ts))
    return [any(s <= row.last_ts and e >= row.since_ts for s, e in by_vehicle.get((row.source, row.vehicle_id), ()))
            for row in left.itertuples()]


def compare_stalled(truth, stream, cutoff_ms, since, until, examples):
    for df in (truth, stream):
        df["source"], df["vehicle_id"] = df["source"].astype(str), df["vehicle_id"].astype(str)
    truth = truth[in_range(truth["since_ts"], since, until)]
    stream = stream[in_range(stream["since_ts"], since, until)]
    pending = truth["detected_window_end_ms"] > cutoff_ms
    truth_all, truth = truth, truth[~pending]

    caught = match_intervals(truth, stream)
    correct = match_intervals(stream, truth_all)        # a pending stall can still be a correct alert
    res = scores(sum(correct), len(stream), sum(caught), len(truth))
    res["pending"] = int(pending.sum())
    cols = ["source", "vehicle_id", "route_id", "since_ts", "last_ts", "minutes"]
    res["missed_examples"] = truth[[not c for c in caught]][cols].head(examples).to_dict("records")
    res["false_alarm_examples"] = stream[[not c for c in correct]][cols].head(examples).to_dict("records")
    return res


# ---- Delay propagation ---------------------------------------------------------------

def compare_delay(truth, stream, since, until, examples):
    truth = truth[in_range(truth["ts_b"], since, until)]
    stream = stream[in_range(stream["ts_b"], since, until)]
    t_keys = set(map(tuple, truth[DELAY_KEY].astype(str).values))
    s_keys = set(map(tuple, stream[DELAY_KEY].astype(str).values))
    tp = len(t_keys & s_keys)
    res = scores(tp, len(s_keys), tp, len(t_keys))
    res["missed_examples"] = [dict(zip(DELAY_KEY, k)) for k in sorted(t_keys - s_keys)[:examples]]
    res["false_alarm_examples"] = [dict(zip(DELAY_KEY, k)) for k in sorted(s_keys - t_keys)[:examples]]
    return res


def fmt(x):
    return "  n/a" if x is None else f"{x:5.1%}"


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--truth", default="data/ground_truth", help="folder written by ground_truth.py")
    p.add_argument("--summary", default="benchmarks/results/ground_truth_summary.json")
    p.add_argument("--bunching-alerts", default="data/alerts/bunching")
    p.add_argument("--stalled-alerts", default="data/alerts/stalled")
    p.add_argument("--delay-alerts", default="data/alerts/delay_propagation")
    p.add_argument("--bunching-watermark", type=int, default=BUNCHING["watermark_s"])
    p.add_argument("--stalled-watermark", type=int, default=STALLED["watermark_s"])
    p.add_argument("--since", default=None, help="score alerts from this time (epoch ms or ISO, UTC)")
    p.add_argument("--until", default=None, help="score alerts up to this time (epoch ms or ISO, UTC)")
    p.add_argument("--examples", type=int, default=10, help="misses / false alarms to list per detector")
    p.add_argument("--out", default="benchmarks/results/ground_truth_comparison.json")
    args = p.parse_args()

    summary = {}
    if os.path.exists(args.summary):
        with open(args.summary) as f:
            summary = json.load(f)
    last_ts = summary.get("last_event_ts")
    since, until = parse_time(args.since), parse_time(args.until)
    result = {"ground_truth": args.truth, "ground_truth_settings": summary.get("settings"),
              "since_ms": since, "until_ms": until, "detectors": {}}

    jobs = [("bunching", args.bunching_alerts), ("stalled", args.stalled_alerts),
            ("delay_propagation", args.delay_alerts)]
    for name, alerts_path in jobs:
        truth = read_table(os.path.join(args.truth, name))
        if truth is None:
            print(f"{name}: no ground truth in {args.truth}/{name} (run streaming/ground_truth.py), skipped")
            continue
        stream = read_table(alerts_path)
        if stream is None:
            note = ("no streaming delay-propagation detector yet; ground truth only"
                    if name == "delay_propagation" else f"no streaming alerts in {alerts_path}")
            print(f"{name}: {note} ({len(truth):,} ground-truth alerts)")
            result["detectors"][name] = {"ground_truth_alerts": len(truth), "note": note}
            continue
        if name == "bunching":
            cutoff = (last_ts - args.bunching_watermark * 1000) if last_ts else float("inf")
            res = compare_bunching(truth, stream, cutoff, since, until, args.examples)
        elif name == "stalled":
            cutoff = (last_ts - args.stalled_watermark * 1000) if last_ts else float("inf")
            res = compare_stalled(truth, stream, cutoff, since, until, args.examples)
        else:
            res = compare_delay(truth, stream, since, until, args.examples)
        res["streaming_alerts_path"] = alerts_path
        result["detectors"][name] = res

    scored = {k: v for k, v in result["detectors"].items() if "precision" in v}
    if scored:
        print(f"\n{'detector':<18} {'stream':>7} {'truth':>7} {'TP':>6} {'FP':>6} {'FN':>6} "
              f"{'pending':>7} {'precision':>9} {'recall':>7}")
        for name, r in scored.items():
            print(f"{name:<18} {r['streaming_alerts']:>7,} {r['ground_truth_alerts']:>7,} "
                  f"{r['true_positives']:>6,} {r['false_positives']:>6,} {r['false_negatives']:>6,} "
                  f"{r.get('pending', 0):>7,} {fmt(r['precision']):>9} {fmt(r['recall']):>7}")
        b = scored.get("bunching")
        if b and (b["false_negative_reasons"] or b["false_positive_reasons"]):
            print("\nbunching misses:      " + "; ".join(f"{n} {w}" for w, n in b["false_negative_reasons"].items()))
            print("bunching false alarms: " + "; ".join(f"{n} {w}" for w, n in b["false_positive_reasons"].items()))
        if b and b["by_category_recall"]:
            print("bunching recall by category: " + ", ".join(f"{c} {fmt(v).strip()}"
                                                              for c, v in b["by_category_recall"].items()))

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\nSaved {args.out}")


if __name__ == "__main__":
    main()

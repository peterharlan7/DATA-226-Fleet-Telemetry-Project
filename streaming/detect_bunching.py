"""
Bus-bunching detector (B12), Spark Structured Streaming.

Bus bunching: two buses on the same route, going the same direction, end up
right next to each other instead of evenly spaced. Riders then wait a long time
for a bus, and two arrive at once.

How it works:
  1. Read vehicle events (files or Kafka, same input as stream_to_parquet.py).
  2. Group reports into event-time windows per (source, route, direction).
     Each window is the current minute PLUS the 5 minutes before it (a sliding
     window), so we know how far each vehicle moved recently.
     A watermark says how long to wait for late reports before closing a window.
  3. When a window closes, compare every pair of buses in the current minute,
     using reports taken close together in time. Closer than max_distance_m -> a close pair.
  4. Label each close pair (refinement from the live-data analysis):
       layover     both vehicles parked (moved < min_move_m in the look-back)
       one_parked  one vehicle parked, e.g. a bus passing one waiting at a terminal
       no_history  not enough reports yet to tell whether they moved
       tandem      both moving and close at every shared report time: worst bunching
       same_stop   both moving and stopped at the same stop: classic bunching
       moving      both moving, close: bunching
     Only tandem / same_stop / moving count as bunching (is_bunching = true).
     --min-move 0 turns the refinement off (the original raw detector).
  5. Write every close pair to Parquet (data/alerts/bunching) and print the bunching ones.
     With --mongo, also insert the bunching ones into the MongoDB alerts collection (B11).

Run from the repo root (start it before the feed):
    python streaming/detect_bunching.py
    python streaming/detect_bunching.py --source kafka
    python streaming/detect_bunching.py --max-distance 150 --window 120
    python streaming/detect_bunching.py --min-move 0                    # raw detector, no refinement

Check results against planted events from fake_feed.py --bunch-every:
    python streaming/evaluate_bunching.py
"""

import argparse
import json
import math
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (TOPIC_VEHICLE_POSITIONS,  # noqa: E402
                              VEHICLE_EVENT_FIELDS, spark_schema)
from streaming.config import BUNCHING  # noqa: E402

BUNCHING_CATEGORIES = ("tandem", "same_stop", "moving")
from streaming.stream_to_parquet import build_spark, read_stream  # noqa: E402

OUTPUT_PATH = "data/alerts/bunching"
CHECKPOINT_PATH = "data/checkpoints/bunching"


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in meters between two points on Earth (great-circle distance)."""
    r = 6_371_000
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def moved_m(reports):
    """How far a vehicle got from its first position (meters)."""
    a = reports[0]
    return max(haversine_m(a["lat"], a["lon"], r["lat"], r["lon"]) for r in reports)


def classify_pair(rep_a, rep_b, closest_a, closest_b, min_move_m, min_history_s, tandem_m,
                  max_time_gap_s):
    """Label one close pair using the look-back reports of both vehicles (sorted by time).

    Returns (category, moved_a_m, moved_b_m). Pure Python, testable without Spark.
    """
    moved = []
    for reps in (rep_a, rep_b):
        span_s = (reps[-1]["event_ts"] - reps[0]["event_ts"]) / 1000
        moved.append(moved_m(reps) if len(reps) >= 2 and span_s >= min_history_s else None)
    move_a, move_b = moved
    if min_move_m > 0:
        if move_a is None or move_b is None:
            return "no_history", move_a, move_b
        still_a, still_b = move_a < min_move_m, move_b < min_move_m
        if still_a and still_b:
            return "layover", move_a, move_b
        if still_a or still_b:
            return "one_parked", move_a, move_b
    # Tandem: close every time both reported in the look-back (at least 3 times).
    dists = []
    for a in rep_a:
        b = min(rep_b, key=lambda r: abs(r["event_ts"] - a["event_ts"]))
        if abs(b["event_ts"] - a["event_ts"]) / 1000 <= max_time_gap_s:
            dists.append(haversine_m(a["lat"], a["lon"], b["lat"], b["lon"]))
    if len(dists) >= 3 and max(dists) < tandem_m:
        return "tandem", move_a, move_b
    if (closest_a.get("stop_id") and closest_a.get("stop_id") == closest_b.get("stop_id")
            and closest_a.get("current_status") == "STOPPED_AT"
            and closest_b.get("current_status") == "STOPPED_AT"):
        return "same_stop", move_a, move_b
    return "moving", move_a, move_b


def bunching_pairs(reports, current_start_ms, max_distance_m, max_time_gap_s,
                   min_move_m=0, min_history_s=30, tandem_m=40):
    """Close pairs in the current minute, each labelled with a category.

    reports: every report for one route and direction in the window (look-back + current minute).
    Only reports at or after current_start_ms are compared for closeness (same rule as
    close_pairs); the earlier ones are only used to judge movement.
    """
    by_bus = {}
    for r in reports:
        by_bus.setdefault(r["vehicle_id"], {})[r["event_ts"]] = r     # also drops duplicates
    history = {v: sorted(rs.values(), key=lambda r: r["event_ts"]) for v, rs in by_bus.items()}
    current = [r for r in reports if r["event_ts"] >= current_start_ms]
    out = []
    for va, vb, dist, gap, ra, rb in _closest_pairs(current, max_distance_m, max_time_gap_s):
        cat, ma, mb = classify_pair(history[va], history[vb], ra, rb, min_move_m,
                                    min_history_s, tandem_m, max_time_gap_s)
        out.append((va, vb, dist, gap, cat, cat in BUNCHING_CATEGORIES,
                    None if ma is None else round(ma, 1), None if mb is None else round(mb, 1),
                    ra.get("trip_id"), rb.get("trip_id"), ra.get("current_status"),
                    rb.get("current_status"), ra.get("stop_id"), rb.get("stop_id")))
    return out


def close_pairs(reports, max_distance_m, max_time_gap_s):
    """Original B12 rule: (vehicle_a, vehicle_b, distance_m, time_gap_s) for close pairs."""
    return [p[:4] for p in _closest_pairs(reports, max_distance_m, max_time_gap_s)]


def _closest_pairs(reports, max_distance_m, max_time_gap_s):
    """Find pairs of buses that came closer than max_distance_m during the window.

    reports: every position report for one route and direction in one window.
    Compares each report of bus A with each report of bus B taken within
    max_time_gap_s of it, so bunching that starts and ends inside the window is
    still caught. Returns one row per bunched pair: their closest approach.
    Pure Python, so it can be tested without Spark.
    """
    by_bus = {}
    for r in reports:
        # A dict keyed by timestamp also drops duplicate reports.
        by_bus.setdefault(r["vehicle_id"], {})[r["event_ts"]] = r
    buses = sorted(by_bus)
    pairs = []
    for i in range(len(buses)):
        for j in range(i + 1, len(buses)):
            closest = None
            for a in by_bus[buses[i]].values():
                for b in by_bus[buses[j]].values():
                    gap_s = abs(a["event_ts"] - b["event_ts"]) / 1000
                    if gap_s > max_time_gap_s:
                        continue
                    d = haversine_m(a["lat"], a["lon"], b["lat"], b["lon"])
                    if closest is None or d < closest[0]:
                        closest = (d, gap_s, a, b)
            if closest and closest[0] < max_distance_m:     # bunched = too CLOSE
                pairs.append((buses[i], buses[j], round(closest[0], 1), round(closest[1], 1),
                              closest[2], closest[3]))
    return pairs


SINK_MODES = ("append", "idempotent")


def batch_path(output, batch_id):
    """Where the idempotent sink keeps one micro-batch's alerts: <output>/batch_id=<n>."""
    return os.path.join(output, f"batch_id={batch_id}")


def write_alerts(alerts, batch_id, output, sink_mode):
    """Write one micro-batch's alerts (B25 compares the two modes).

    append      add new files every time (at-least-once). foreachBatch is not covered by
                Spark's exactly-once file sink: if the job dies after this write but before
                Spark commits the batch, the restarted job runs the same batch again and
                its alerts are written twice.
    idempotent  overwrite <output>/batch_id=<n> (exactly-once). A replayed batch has the
                same batch_id, the same input offsets and the same window state, so it
                replaces its own earlier output instead of adding to it. Same idea as the
                deterministic _id in the MongoDB alert sink (B11).
    """
    if sink_mode == "idempotent":
        alerts.write.mode("overwrite").parquet(batch_path(output, batch_id))
    else:
        alerts.write.mode("append").parquet(output)


def crash_now(batch_id):
    """Fault injection (B25): SIGKILL this job (driver + JVM) right after a sink write,
    before Spark can commit the batch: the at-least-once window. Only for a job started
    in its own process group (the B25 harness does that)."""
    print(f"CRASH injected after writing micro-batch {batch_id}", flush=True)
    os.killpg(os.getpgrp(), signal.SIGKILL)


def progress_record(p):
    """The parts of a StreamingQueryProgress (a dict) that B25 needs, as one JSON-able dict."""
    if not isinstance(p, dict):                     # PySpark 4 returns progress objects
        p = json.loads(p.json)
    ops = p.get("stateOperators") or [{}]
    src = (p.get("sources") or [{}])[0]
    return {"batch_id": p["batchId"], "timestamp": p["timestamp"],
            "input_rows": p.get("numInputRows", 0),
            "trigger_ms": p.get("durationMs", {}).get("triggerExecution"),
            "watermark": p.get("eventTime", {}).get("watermark"),
            "state_rows": ops[0].get("numRowsTotal"),
            "state_bytes": ops[0].get("memoryUsedBytes"),
            "dropped_by_watermark": ops[0].get("numRowsDroppedByWatermark", 0),
            "end_offset": src.get("endOffset")}


def route_windows(events, args):
    """Group reports into sliding windows per (source, route, direction).

    Shared by the streaming job and the offline ground-truth scan (B18), so both
    use exactly the same windows. `events` may be a streaming or a batch DataFrame.
    """
    from pyspark.sql.functions import col, collect_list, count, struct, timestamp_millis, window

    events = (events.where(col("event_id").isNotNull()
                           & col("route_id").isNotNull()
                           & col("direction_id").isNotNull())
              .withColumn("event_time", timestamp_millis(col("event_ts"))))
    if events.isStreaming:
        events = events.withWatermark("event_time", f"{args.watermark} seconds")

    # Sliding window: (look-back + current minute) long, moving forward one minute at a time.
    # Every minute is the "current minute" of exactly one window.
    span = args.lookback + args.window if args.min_move > 0 else args.window
    return (events
            .groupBy(window(col("event_time"), f"{span} seconds", f"{args.window} seconds"),
                     "source", "route_id", "direction_id")
            .agg(collect_list(struct("vehicle_id", "event_ts", "lat", "lon", "trip_id",
                                     "current_status", "stop_id")).alias("reports"),
                 count("*").alias("n_reports")))


def window_pairs(windows, args):
    """Apply the bunching rule to closed windows: one row per close pair (shared with B18)."""
    from pyspark.sql.functions import col, explode, lit, timestamp_millis, udf
    from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, StringType,
                                   StructField, StructType)

    s, d, b = StringType(), DoubleType(), BooleanType()
    pair_type = ArrayType(StructType([StructField(n, t) for n, t in [
        ("vehicle_a", s), ("vehicle_b", s), ("distance_m", d), ("time_gap_s", d),
        ("category", s), ("is_bunching", b), ("moved_a_m", d), ("moved_b_m", d),
        ("trip_a", s), ("trip_b", s), ("status_a", s), ("status_b", s),
        ("stop_a", s), ("stop_b", s)]]))
    max_d, max_gap, min_move = args.max_distance, args.max_time_gap, args.min_move
    min_hist, tandem, win_ms = args.min_history, args.tandem, args.window * 1000
    find_pairs = udf(lambda reports, end_ms: bunching_pairs(
        [r.asDict() for r in reports], end_ms - win_ms, max_d, max_gap, min_move, min_hist, tandem), pair_type)

    end_ms = col("window.end").cast("long") * 1000
    return (windows
            .withColumn("pair", explode(find_pairs(col("reports"), end_ms)))
            .select("source", "route_id", "direction_id",
                    # Report the current minute, not the whole look-back window.
                    timestamp_millis(end_ms - lit(win_ms)).alias("window_start"),
                    col("window.end").alias("window_end"),
                    "pair.*"))


def build_query(spark, args):
    from pyspark.sql.functions import current_timestamp

    windows = route_windows(read_stream(spark, args, spark_schema(VEHICLE_EVENT_FIELDS)), args)

    stats = {"windows": 0, "alerts": 0, "close_pairs": 0, "categories": {}}

    def handle_batch(batch_df, batch_id):
        """Runs once per micro-batch on the windows that just closed."""
        alerts = (window_pairs(batch_df, args)
                  .withColumn("detected_at", current_timestamp())
                  .persist())
        n_windows = batch_df.count()
        rows = alerts.collect()
        if rows:
            write_alerts(alerts, batch_id, args.output, args.sink_mode)
        alerts.unpersist()
        if rows and args.crash_at_batch is not None and batch_id >= args.crash_at_batch:
            crash_now(batch_id)

        bunched = [r for r in rows if r["is_bunching"]]
        if bunched and args.mongo:
            # Only real bunching goes to the operational alerts collection; filtered-out
            # close pairs (layover, one_parked, no_history) stay in Parquet for analysis.
            # Aliased: a bare `import write_alerts` here would shadow the Parquet
            # write_alerts above for the whole function (UnboundLocalError).
            from storage.mongo_sink import write_alerts as mongo_write_alerts
            new = mongo_write_alerts([r.asDict() for r in bunched], alert_type="bunching")
            print(f"  mongo alerts: {new} new of {len(bunched)}", flush=True)
        stats["windows"] += n_windows
        stats["close_pairs"] += len(rows)
        stats["alerts"] += len(bunched)
        for r in rows:
            stats["categories"][r["category"]] = stats["categories"].get(r["category"], 0) + 1
        if n_windows:
            last = batch_df.agg({"window.end": "max"}).first()[0] if args.quiet else None
            upto = f" | event time up to {last:%Y-%m-%d %H:%M} (your local time)" if last else ""
            print(f"micro-batch {batch_id}: {n_windows} route-windows closed, "
                  f"{len(rows)} close pair(s), {len(bunched)} bunching alert(s){upto}", flush=True)
        for r in ([] if args.quiet else bunched):
            print(f"  ALERT {r['category']:<9} {r['source']} route {r['route_id']} dir {r['direction_id']} | "
                  f"{r['window_start']:%H:%M:%S}-{r['window_end']:%H:%M:%S} | "
                  f"buses {r['vehicle_a'][:8]} & {r['vehicle_b'][:8]} {r['distance_m']:.0f} m apart",
                  flush=True)

    query = (windows.writeStream
             .outputMode("append")            # emit each window once, after the watermark passes it
             .foreachBatch(handle_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger} seconds")
             .start())
    return query, stats


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/stream_in", help="folder to watch (files source)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_POSITIONS", TOPIC_VEHICLE_POSITIONS))
    p.add_argument("--output", default=OUTPUT_PATH)
    p.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    p.add_argument("--trigger", type=int, default=10, help="seconds between micro-batches")
    p.add_argument("--max-distance", type=float, default=BUNCHING["max_distance_m"])
    p.add_argument("--max-time-gap", type=float, default=BUNCHING["max_time_gap_s"])
    p.add_argument("--window", type=int, default=BUNCHING["window_s"])
    p.add_argument("--watermark", type=int, default=BUNCHING["watermark_s"])
    p.add_argument("--lookback", type=int, default=BUNCHING["lookback_s"],
                   help="seconds of history used to judge whether each vehicle moved")
    p.add_argument("--min-move", type=float, default=BUNCHING["min_move_m"],
                   help="moved less than this (m) in the look-back = parked; 0 = raw detector")
    p.add_argument("--min-history", type=float, default=BUNCHING["min_history_s"])
    p.add_argument("--tandem", type=float, default=BUNCHING["tandem_m"])
    p.add_argument("--max-files", type=int, default=50,
                   help="files per micro-batch (files source); use 1 when replaying a recording")
    p.add_argument("--quiet", action="store_true",
                   help="don't print every alert; show event-time progress instead (long replays)")
    p.add_argument("--summary", default=None, help="write category counts to this JSON file at stop")
    p.add_argument("--mongo", action="store_true",
                   help="also write each alert to the MongoDB alerts collection (B11, MONGO_URI)")
    # Experiment options (B25 design comparison). Defaults keep the normal behaviour.
    p.add_argument("--sink-mode", choices=SINK_MODES, default="append",
                   help="append = at-least-once alert writes; idempotent = overwrite per batch_id (exactly-once)")
    p.add_argument("--max-offsets", type=int, default=None,
                   help="Kafka source: at most this many records per micro-batch (pace a replay)")
    p.add_argument("--progress-log", default=None,
                   help="append one JSON line per micro-batch (state size, watermark drops, offsets)")
    p.add_argument("--crash-at-batch", type=int, default=None,
                   help="fault injection: SIGKILL the job after the first micro-batch >= N that wrote alerts")
    args = p.parse_args()

    spark = build_spark(use_kafka=(args.source == "kafka"))
    query, stats = build_query(spark, args)
    refine = (f"refined: parked < {args.min_move:.0f} m in {args.lookback} s look-back"
              if args.min_move > 0 else "raw (no refinement)")
    print(f"Bunching detector: < {args.max_distance:.0f} m, {args.window} s windows, "
          f"{args.watermark} s watermark, {refine}, reading {args.source} (Ctrl+C to stop)")
    print("Alerts appear after each window closes (window + watermark later).")

    stop = {"requested": False}
    signal.signal(signal.SIGINT, lambda s, f: stop.update(requested=True))
    logged = {"last": -1}

    def log_progress():
        if not args.progress_log:
            return
        new = [p for p in query.recentProgress if p["batchId"] > logged["last"]]
        if new:
            with open(args.progress_log, "a") as f:
                for p in new:
                    f.write(json.dumps(progress_record(p)) + "\n")
            logged["last"] = new[-1]["batchId"]

    while query.isActive and not stop["requested"]:
        log_progress()
        time.sleep(0.5)
    print("Stopping...")
    query.stop()
    log_progress()
    print(f"Stopped. Route-windows checked: {stats['windows']} | close pairs: {stats['close_pairs']} | "
          f"bunching alerts: {stats['alerts']}")
    for c, n in sorted(stats["categories"].items(), key=lambda x: -x[1]):
        tag = "bunching" if c in BUNCHING_CATEGORIES else "filtered out"
        print(f"  {c:<11} {n:>6}  ({tag})")
    if args.summary:
        os.makedirs(os.path.dirname(args.summary) or ".", exist_ok=True)
        with open(args.summary, "w") as f:
            json.dump({"settings": {k: v for k, v in vars(args).items() if k not in ("bootstrap",)},
                       **stats}, f, indent=2, default=str)
        print(f"Summary saved to {args.summary}")


if __name__ == "__main__":
    main()

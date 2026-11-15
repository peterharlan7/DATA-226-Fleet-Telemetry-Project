"""
Stalled-vehicle detector (B16), Spark Structured Streaming.

A stalled vehicle is in service (it has a route and trip) but has not moved for
a long time while NOT waiting at a stop. Vehicles waiting at a stop or terminal
report current_status = STOPPED_AT, so they are not counted as stalls. A stall
can mean a breakdown, an incident blocking the street, or very heavy traffic.

How it works:
  1. Read vehicle events (files or Kafka, same input as the other jobs).
  2. Sliding event-time windows per vehicle: (stall_minutes + 2) minutes long,
     one minute apart. A watermark closes each window after late reports had time to arrive.
  3. In each closed window, walk back from the vehicle's latest report while it
     stays within still_m of that spot and does not report STOPPED_AT. If that
     stretch lasts at least stall_minutes with at least min_reports reports, the
     vehicle is stalled. (A bus that first waited at a stop and then stayed stuck
     counts from when it stopped reporting STOPPED_AT.)
  4. A stall that continues is reported once (when it starts) and its duration is
     tracked; the final report lists every stall and how long it lasted.

Limitation: the stall tracker lives in driver memory and is NOT saved in the Spark
checkpoint. After a restart with the same --checkpoint, a stall already in progress
is reported again as new, and the final report lists only stalls since the restart.

Run from the repo root (start it before the feed):
    python streaming/detect_stalled.py
    python streaming/detect_stalled.py --source kafka --bootstrap localhost:29092
    python streaming/detect_stalled.py --stall-minutes 1       # quick test with fake_feed.py --stall-every

Check results against planted stalls from fake_feed.py --stall-every:
    python streaming/evaluate_stalled.py
"""

import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (TOPIC_VEHICLE_POSITIONS,  # noqa: E402
                              VEHICLE_EVENT_FIELDS, spark_schema)
from streaming.config import STALLED  # noqa: E402
from streaming.detect_bunching import haversine_m  # noqa: E402
from streaming.stream_to_parquet import build_spark, read_stream  # noqa: E402

OUTPUT_PATH = "data/alerts/stalled"
OUT_SCHEMA = ("source string, vehicle_id string, route_id string, trip_id string, since_ts long, "
              "last_ts long, lat double, lon double, status string, stop_id string, minutes double")
CHECKPOINT_PATH = "data/checkpoints/stalled"


def find_stall(reports, stall_s, still_m, min_reports):
    """Is this vehicle stalled at the end of its reports? Pure Python, testable without Spark.

    reports: one vehicle's reports in the window. Returns None, or
    (since_ts, last_ts, minutes, lat, lon, route_id, trip_id, status, stop_id).
    """
    if not reports:
        return None
    by_ts = {r["event_ts"]: r for r in reports}                 # drop duplicate reports
    reps = [by_ts[t] for t in sorted(by_ts)]
    last = reps[-1]
    if not last.get("route_id") or not last.get("trip_id"):
        return None                                              # not in service
    still = []
    for r in reversed(reps):        # walk back while it stays put and is not waiting at a stop
        if (r.get("current_status") == "STOPPED_AT"
                or haversine_m(last["lat"], last["lon"], r["lat"], r["lon"]) > still_m):
            break
        still.append(r)
    if not still:
        return None                                              # waiting at a stop right now
    since = still[-1]["event_ts"]
    duration_s = (last["event_ts"] - since) / 1000
    if duration_s < stall_s or len(still) < min_reports:
        return None
    return (since, last["event_ts"], round(duration_s / 60, 1), last["lat"], last["lon"],
            last.get("route_id"), last.get("trip_id"), last.get("current_status"), last.get("stop_id"))


class StallTracker:
    """Turns per-window results into stall EVENTS: report a stall once, then track its duration."""

    def __init__(self, still_m):
        self.still_m = still_m
        self.ongoing = {}      # (source, vehicle_id) -> stall dict
        self.stalls = []       # every stall seen, in start order

    def update(self, row):
        """Returns the stall dict if this row starts a NEW stall, else None."""
        key = (row["source"], row["vehicle_id"])
        cur = self.ongoing.get(key)
        if (cur and haversine_m(cur["lat"], cur["lon"], row["lat"], row["lon"]) <= self.still_m
                and row["since_ts"] <= cur["last_ts"]):
            cur["last_ts"] = max(cur["last_ts"], row["last_ts"])
            cur["minutes"] = round((cur["last_ts"] - cur["since_ts"]) / 60_000, 1)
            return None
        stall = {k: row[k] for k in ("source", "vehicle_id", "route_id", "trip_id", "since_ts",
                                     "last_ts", "lat", "lon", "status", "stop_id")}
        stall["minutes"] = row["minutes"]
        self.ongoing[key] = stall
        self.stalls.append(stall)
        return stall


def build_query(spark, args, tracker):
    from pyspark.sql.functions import col, collect_list, struct, timestamp_millis, udf, window
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    events = (read_stream(spark, args, spark_schema(VEHICLE_EVENT_FIELDS))
              .where(col("event_id").isNotNull() & col("vehicle_id").isNotNull())
              .withColumn("event_time", timestamp_millis(col("event_ts")))
              .withWatermark("event_time", f"{args.watermark} seconds"))

    span_s = args.stall_minutes * 60 + 120
    windows = (events
               .groupBy(window(col("event_time"), f"{span_s} seconds", "60 seconds"),
                        "source", "vehicle_id")
               .agg(collect_list(struct("event_ts", "lat", "lon", "route_id", "trip_id",
                                        "current_status", "stop_id")).alias("reports")))

    s, d, lg = StringType(), DoubleType(), LongType()
    stall_type = StructType([StructField(n, t) for n, t in [
        ("since_ts", lg), ("last_ts", lg), ("minutes", d), ("lat", d), ("lon", d),
        ("route_id", s), ("trip_id", s), ("status", s), ("stop_id", s)]])
    stall_s, still_m, min_reports = args.stall_minutes * 60, args.still, args.min_reports
    check = udf(lambda reports: find_stall([r.asDict() for r in reports], stall_s, still_m, min_reports), stall_type)

    stats = {"vehicle_windows": 0}

    def handle_batch(batch_df, batch_id):
        found = (batch_df.withColumn("stall", check(col("reports")))
                 .where(col("stall").isNotNull())
                 .select("source", "vehicle_id", "stall.*")
                 .orderBy("since_ts")
                 .collect())
        n_windows = batch_df.count()
        stats["vehicle_windows"] += n_windows
        new = [s for s in (tracker.update(r.asDict()) for r in found) if s]
        if new:
            out = (spark.createDataFrame(new, schema=OUT_SCHEMA)
                   .withColumn("since", timestamp_millis(col("since_ts"))))
            out.write.mode("append").parquet(args.output)
        if args.quiet and n_windows:
            last = batch_df.agg({"window.end": "max"}).first()[0]
            print(f"micro-batch {batch_id}: {n_windows} vehicle-windows closed, {len(new)} new stall(s), "
                  f"{len(tracker.stalls)} so far | event time up to {last:%Y-%m-%d %H:%M} (your local time)", flush=True)
        for st in ([] if args.quiet else new):
            print(f"  STALL {st['source']} route {st['route_id']} vehicle {st['vehicle_id'][:8]} | "
                  f"not moving for {st['minutes']} min since "
                  f"{time.strftime('%H:%M:%S', time.localtime(st['since_ts'] / 1000))} | "
                  f"status {st['status']} | ({st['lat']:.5f}, {st['lon']:.5f})", flush=True)

    query = (windows.writeStream
             .outputMode("append")
             .foreachBatch(handle_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger} seconds")
             .start())
    return query, stats


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/stream_in", help="folder to watch (files source)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_POSITIONS", TOPIC_VEHICLE_POSITIONS))
    p.add_argument("--output", default=OUTPUT_PATH)
    p.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    p.add_argument("--trigger", type=int, default=10, help="seconds between micro-batches")
    p.add_argument("--stall-minutes", type=float, default=STALLED["stall_minutes"])
    p.add_argument("--still", type=float, default=STALLED["still_m"])
    p.add_argument("--min-reports", type=int, default=STALLED["min_reports"])
    p.add_argument("--watermark", type=int, default=STALLED["watermark_s"])
    p.add_argument("--max-files", type=int, default=50,
                   help="files per micro-batch (files source); use 1 when replaying a recording")
    p.add_argument("--quiet", action="store_true", help="don't print every stall (long replays)")
    p.add_argument("--report", default=None, help="JSON list of all stalls (default benchmarks/results/...)")
    args = p.parse_args()

    tracker = StallTracker(args.still)
    spark = build_spark(use_kafka=(args.source == "kafka"))
    query, stats = build_query(spark, args, tracker)
    print(f"Stalled-vehicle detector: in service, within {args.still:.0f} m for {args.stall_minutes} min, "
          f"not STOPPED_AT, reading {args.source} (Ctrl+C to stop)")

    stop = {"requested": False}
    signal.signal(signal.SIGINT, lambda s, f: stop.update(requested=True))
    while query.isActive and not stop["requested"]:
        time.sleep(1)
    print("Stopping...")
    query.stop()

    stalls = tracker.stalls
    report = args.report or f"benchmarks/results/stalled_{args.source}_{time.strftime('%Y%m%d_%H%M')}.json"
    os.makedirs(os.path.dirname(report), exist_ok=True)
    with open(report, "w") as f:
        json.dump({"settings": {"stall_minutes": args.stall_minutes, "still_m": args.still,
                                "min_reports": args.min_reports},
                   "vehicle_windows": stats["vehicle_windows"], "stalls": stalls}, f, indent=2)
    print(f"Stopped. Stalls found: {len(stalls)}")
    by_src = {}
    for st in stalls:
        by_src.setdefault(st["source"], []).append(st["minutes"])
    for src, mins in sorted(by_src.items()):
        mins.sort()
        print(f"  {src}: {len(mins)} stalls, median {mins[len(mins) // 2]} min, longest {mins[-1]} min")
    print(f"Report saved to {report}")


if __name__ == "__main__":
    main()

"""
Run the course's streaming algorithms on the live vehicle stream (B17 + B23).

For every micro-batch, each event goes through:
  - a Bloom filter on event_id     -> duplicate detection (B17)
  - a reservoir sampler            -> fair random sample of events for review (B23)
  - Flajolet-Martin per agency     -> estimated number of distinct vehicles (B23)

To show how good the approximations are, the job ALSO keeps the exact answers
(a set of every event_id and every vehicle). That exact check is for evaluation
only: it uses far more memory, which is exactly what the streaming versions avoid.

Run from the repo root:
    python streaming/stream_algorithms.py                                   # fake feed folder
    python streaming/stream_algorithms.py --source kafka --bootstrap localhost:29092
Stop with Ctrl+C. A JSON report is written to benchmarks/results/.
"""

import argparse
import json
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from algorithms.stream_algos import BloomFilter, FlajoletMartin, ReservoirSampler  # noqa: E402
from ingestion.schema import (TOPIC_VEHICLE_POSITIONS,  # noqa: E402
                              VEHICLE_EVENT_FIELDS, spark_schema)
from streaming.stream_to_parquet import build_spark, read_stream  # noqa: E402


class StreamStats:
    """The three streaming algorithms plus exact answers to compare against."""

    def __init__(self, expected_events, fp_rate, sample_size, seed):
        self.bloom = BloomFilter(expected_events, fp_rate)
        self.reservoir = ReservoirSampler(sample_size, seed=seed)
        self.fm = {}                       # agency -> FlajoletMartin
        self.exact_ids = set()             # evaluation only
        self.exact_vehicles = {}           # agency -> set of vehicle ids, evaluation only
        self.events = 0
        self.true_dupes = 0
        self.flagged = 0
        self.false_pos = 0

    def add(self, row):
        self.events += 1
        eid = row["event_id"]
        said_dup = self.bloom.add(eid)
        is_dup = eid in self.exact_ids
        self.exact_ids.add(eid)
        self.flagged += said_dup
        self.true_dupes += is_dup
        self.false_pos += said_dup and not is_dup

        self.reservoir.add({k: row[k] for k in ("source", "route_id", "vehicle_id", "event_ts", "lat", "lon")})

        src = row["source"]
        self.fm.setdefault(src, FlajoletMartin()).add(row["vehicle_id"])
        self.exact_vehicles.setdefault(src, set()).add(row["vehicle_id"])

    def summary(self):
        new_items = self.events - self.true_dupes
        per_agency = {}
        for src in sorted(self.fm):
            est, true = self.fm[src].estimate(), len(self.exact_vehicles[src])
            per_agency[src] = {"distinct_vehicles_exact": true,
                               "distinct_vehicles_fm": round(est),
                               "fm_error_pct": round(abs(est - true) / true * 100, 1) if true else None}
        return {
            "events": self.events,
            "bloom": {
                "true_duplicates": self.true_dupes,
                "flagged": self.flagged,
                "missed": self.true_dupes - (self.flagged - self.false_pos),
                "false_positives": self.false_pos,
                "false_positive_rate_pct": round(self.false_pos / new_items * 100, 3) if new_items else 0,
                "target_rate_pct": self.bloom.p_target * 100,
                "memory_bytes": self.bloom.memory_bytes(),
                "bits_m": self.bloom.m, "hashes_k": self.bloom.k,
            },
            "flajolet_martin": {"memory_bytes_per_agency": 512, "per_agency": per_agency},
            "reservoir": {"k": self.reservoir.k, "seen": self.reservoir.seen,
                          "sample": self.reservoir.sample},
            "exact_check_memory_estimate_bytes": len(self.exact_ids) * 120,
        }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/stream_in", help="folder to watch (files source)")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_POSITIONS", TOPIC_VEHICLE_POSITIONS))
    p.add_argument("--checkpoint", default="data/checkpoints/stream_algorithms")
    p.add_argument("--trigger", type=int, default=10, help="seconds between micro-batches")
    p.add_argument("--expected-events", type=int, default=3_000_000,
                   help="Bloom filter sizing: events expected over the whole run (24 h is about 2.3 million)")
    p.add_argument("--fp-rate", type=float, default=0.01, help="Bloom filter target false-positive rate")
    p.add_argument("--sample-size", type=int, default=20, help="reservoir sample size k")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--report", default=None, help="JSON report path (default: benchmarks/results/...)")
    args = p.parse_args()

    stats = StreamStats(args.expected_events, args.fp_rate, args.sample_size, args.seed)
    spark = build_spark(use_kafka=(args.source == "kafka"))
    events = read_stream(spark, args, spark_schema(VEHICLE_EVENT_FIELDS))

    def handle_batch(batch_df, batch_id):
        n0 = stats.events
        for row in batch_df.select("event_id", "source", "route_id", "vehicle_id",
                                   "event_ts", "lat", "lon").toLocalIterator():
            if row["event_id"] is not None and row["vehicle_id"] is not None:
                stats.add(row.asDict())
        if stats.events == n0:
            return
        s = stats.summary()
        fm = "  ".join(f"{a}: {v['distinct_vehicles_fm']} est / {v['distinct_vehicles_exact']} true"
                       for a, v in s["flajolet_martin"]["per_agency"].items())
        print(f"batch {batch_id:>4}: +{stats.events - n0:>6} events | total {s['events']:>8} | "
              f"dupes caught {s['bloom']['flagged'] - s['bloom']['false_positives']}/{s['bloom']['true_duplicates']} "
              f"(false alarms {s['bloom']['false_positives']}) | vehicles {fm}", flush=True)

    query = (events.writeStream.foreachBatch(handle_batch)
             .option("checkpointLocation", args.checkpoint)
             .trigger(processingTime=f"{args.trigger} seconds")
             .start())
    print(f"Streaming algorithms on {args.source}: Bloom filter (sized for {args.expected_events:,} events, "
          f"{args.fp_rate:.0%} target), reservoir k={args.sample_size}, Flajolet-Martin per agency. Ctrl+C to stop.")

    stop = {"requested": False}
    signal.signal(signal.SIGINT, lambda s, f: stop.update(requested=True))
    while query.isActive and not stop["requested"]:
        time.sleep(1)
    print("Stopping...")
    query.stop()

    s = stats.summary()
    report = args.report or f"benchmarks/results/stream_algorithms_{args.source}_{time.strftime('%Y%m%d_%H%M')}.json"
    os.makedirs(os.path.dirname(report), exist_ok=True)
    with open(report, "w") as f:
        json.dump(s, f, indent=2)
    b = s["bloom"]
    print(f"\nEvents: {s['events']:,}")
    print(f"Bloom filter: caught {b['flagged'] - b['false_positives']}/{b['true_duplicates']} duplicates, "
          f"missed {b['missed']}, false alarms {b['false_positives']} ({b['false_positive_rate_pct']}%, "
          f"target {b['target_rate_pct']}%) | memory {b['memory_bytes']:,} bytes vs exact set ~"
          f"{s['exact_check_memory_estimate_bytes']:,} bytes")
    for a, v in s["flajolet_martin"]["per_agency"].items():
        print(f"Flajolet-Martin {a}: {v['distinct_vehicles_fm']} estimated vs {v['distinct_vehicles_exact']} true "
              f"({v['fm_error_pct']}% error, 512 bytes)")
    print(f"Reservoir: {len(s['reservoir']['sample'])} events sampled uniformly from {s['reservoir']['seen']:,}")
    print(f"Report saved to {report}")


if __name__ == "__main__":
    main()

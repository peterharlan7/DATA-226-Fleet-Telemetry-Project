"""
Rate-controlled Kafka load generator for the pipeline benchmark (B22).

Publishes synthetic vehicle events in the B4 schema (ingestion/schema.py) to one
topic at a fixed target rate, then prints a one-line JSON summary. The benchmark
harness (benchmarks/bench_pipeline.py) runs it as a separate process so the
Python producer does not compete with the Spark driver for the GIL.

Each event's ingest_ts is the moment it was handed to the Kafka producer, so
"Spark commit time - ingest_ts" is the end-to-end latency of the pipeline.

Run from the repo root (normally the harness does this for you):
    python benchmarks/load_generator.py --topic bench-test --rate 2000 --duration 30

Events are tagged source=BENCH (source=WARMUP during --warmup) and use made-up
vehicle ids, so they never mix with live data and need no B13 hashing. Use a
dedicated topic; never point this at vehicle-positions.
"""

import argparse
import json
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ingestion.schema import (SCHEMA_VERSION, VEHICLE_EVENT_FIELDS,  # noqa: E402
                              make_vehicle_event_id, validate)

BENCH_SOURCE = "BENCH"
WARMUP_SOURCE = "WARMUP"

ACKS = {
    # name: producer settings (same three guarantees as ingestion/av_replay.py)
    "at-most-once": {"acks": "0", "retries": 0},
    "at-least-once": {"acks": "all", "retries": 5},
    "exactly-once": {"acks": "all", "enable.idempotence": True},
}


def now_ms():
    return int(time.time() * 1000)


def events_due(elapsed_s, rate):
    """How many events should have been sent `elapsed_s` seconds into a run at `rate` events/s."""
    if elapsed_s <= 0 or rate <= 0:
        return 0
    return int(elapsed_s * rate)


class Fleet:
    """A few hundred fake vehicles doing a random walk around San Francisco."""

    def __init__(self, n, seed=1):
        rnd = random.Random(seed)
        self.rnd = rnd
        self.vehicles = [{"vehicle_id": f"B{i:05d}", "route_id": f"R{i % 40}",
                          "trip_id": f"T{i}", "direction_id": i % 2,
                          "lat": 37.70 + rnd.random() * 0.12, "lon": -122.50 + rnd.random() * 0.12}
                         for i in range(n)]
        self.i = 0

    def next_event(self, source, ts):
        v = self.vehicles[self.i % len(self.vehicles)]
        self.i += 1
        v["lat"] += self.rnd.uniform(-0.0003, 0.0003)
        v["lon"] += self.rnd.uniform(-0.0003, 0.0003)
        return {
            "event_id": make_vehicle_event_id(source, v["vehicle_id"], ts) + f":{self.i}",
            "schema_version": SCHEMA_VERSION, "source": source, "vehicle_id": v["vehicle_id"],
            "route_id": v["route_id"], "trip_id": v["trip_id"], "direction_id": v["direction_id"],
            "event_ts": ts, "ingest_ts": ts, "lat": round(v["lat"], 6), "lon": round(v["lon"], 6),
            "bearing": None, "speed_mps": None, "current_status": "IN_TRANSIT_TO", "stop_id": None,
        }


def run(args):
    from confluent_kafka import Producer

    conf = {"bootstrap.servers": args.bootstrap, "linger.ms": args.linger_ms,
            "queue.buffering.max.messages": 1_000_000}
    conf.update(ACKS[args.delivery])
    producer = Producer(conf)
    fleet = Fleet(args.vehicles, args.seed)

    sample = fleet.next_event(BENCH_SOURCE, now_ms())
    problems = validate(sample, VEHICLE_EVENT_FIELDS)
    if problems:
        sys.exit(f"benchmark event breaks the B4 schema: {problems}")

    stats = {"sent": 0, "warmup_sent": 0, "delivered": 0, "delivery_errors": 0}

    def on_bench_delivery(err, msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["delivery_errors"] += 1

    def on_warmup_delivery(err, msg):
        if err is not None:
            stats["delivery_errors"] += 1

    def send_phase(source, seconds, counter):
        callback = on_bench_delivery if source == BENCH_SOURCE else on_warmup_delivery
        start = time.time()
        sent = 0
        while True:
            elapsed = time.time() - start
            if elapsed >= seconds:
                break
            due = events_due(elapsed, args.rate)
            if due <= sent:
                producer.poll(0)
                time.sleep(0.0005)
                continue
            for _ in range(min(due - sent, 5000)):      # cap the burst so poll() still runs often
                ev = fleet.next_event(source, now_ms())
                while True:
                    try:
                        producer.produce(args.topic, key=ev["vehicle_id"], value=json.dumps(ev),
                                         on_delivery=callback)
                        break
                    except BufferError:                 # local queue full: let it drain
                        producer.poll(0.05)
                sent += 1
            producer.poll(0)
        stats[counter] += sent
        return time.time() - start

    if args.warmup > 0:
        send_phase(WARMUP_SOURCE, args.warmup, "warmup_sent")
    first_ms = now_ms()
    seconds = send_phase(BENCH_SOURCE, args.duration, "sent")
    last_ms = now_ms()
    remaining = producer.flush(60)

    stats.update({
        "topic": args.topic, "rate_target": args.rate, "duration_s": args.duration,
        "send_seconds": round(seconds, 3),
        "producer_rate": round(stats["sent"] / seconds, 1) if seconds else 0.0,
        "first_send_ms": first_ms, "last_send_ms": last_ms,
        "undelivered_after_flush": remaining, "delivery": args.delivery,
    })
    print(json.dumps(stats), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", required=True)
    p.add_argument("--bootstrap", default="localhost:29092")
    p.add_argument("--rate", type=float, required=True, help="target events per second")
    p.add_argument("--duration", type=float, default=60, help="seconds of measured load")
    p.add_argument("--warmup", type=float, default=0, help="seconds of unmeasured load first")
    p.add_argument("--vehicles", type=int, default=500)
    p.add_argument("--delivery", choices=sorted(ACKS), default="at-least-once")
    p.add_argument("--linger-ms", type=int, default=5)
    p.add_argument("--seed", type=int, default=1)
    run(p.parse_args())


if __name__ == "__main__":
    main()

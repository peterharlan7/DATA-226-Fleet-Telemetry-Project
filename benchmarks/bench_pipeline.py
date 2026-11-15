"""
Throughput and latency benchmark for the Kafka -> Spark -> Parquet pipeline (B22).

For every configuration in the sweep (target rate x trigger interval x repeat):
  1. Create a fresh Kafka topic bench-<run_id> (so runs never see each other's data).
  2. Start a Spark Structured Streaming query that reads it, applies the SAME
     transform as the archive job (streaming/stream_to_parquet.py) and writes
     each micro-batch to Parquet (or to Spark's no-op sink with --sink noop).
  3. Run benchmarks/load_generator.py in its own process at the target rate.
  4. Inside every micro-batch, after the write commits, record for each row
         e2e latency   = commit time - ingest_ts   (producer send -> durable in Parquet)
         queue latency = batch start - ingest_ts   (time waiting in Kafka for the next trigger)
     as a 1 ms histogram, so run-level percentiles are exact (not averages of
     per-batch percentiles).
  5. Stop when every delivered event has been committed (or on timeout), then
     drop the topic and the run's Parquet output.

Results (one timestamped pair of CSVs per sweep):
  benchmarks/results/b22_runs_<stamp>.csv     one row per run: throughput, latency percentiles
  benchmarks/results/b22_batches_<stamp>.csv  one row per micro-batch: for latency-over-time plots

Needs Kafka up (docker compose up -d), Java 17 and pyspark, like spark_check.sh.
Run from the repo root:
    python benchmarks/bench_pipeline.py                                  # default sweep (~10 min)
    python benchmarks/bench_pipeline.py --rates 1000 --triggers 1 --duration 20   # quick check
    python benchmarks/bench_pipeline.py --rates 2000,10000,30000 --triggers 1,5,10 --repeats 3
    python benchmarks/bench_pipeline.py --sink noop                      # Spark cost without Parquet

Producer and Spark run on this machine, so both timestamps come from one clock.
"""

import argparse
import copy
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from benchmarks.load_generator import BENCH_SOURCE  # noqa: E402

RUN_COLUMNS = [
    "run_id", "started_at", "rate_target", "trigger_s", "duration_s", "partitions", "sink",
    "delivery", "events_sent", "events_delivered", "events_received", "events_missing",
    "producer_rate", "pipeline_throughput", "spark_processed_rps_mean", "batches",
    "batch_rows_mean", "batch_ms_mean", "batch_ms_p95", "batch_ms_max", "keeps_up",
    "queue_ms_p50", "queue_ms_p95", "queue_ms_p99",
    "e2e_ms_mean", "e2e_ms_p50", "e2e_ms_p95", "e2e_ms_p99", "e2e_ms_max", "timed_out",
]
BATCH_COLUMNS = [
    "run_id", "rate_target", "trigger_s", "batch_id", "start_offset_s", "rows", "warmup_rows",
    "read_ms", "write_ms", "batch_ms", "queue_ms_p50", "e2e_ms_p50", "e2e_ms_p95", "e2e_ms_max",
    "trigger_execution_ms", "input_rps", "processed_rps",
]


# ---------------------------------------------------------------- pure helpers (tested)

def percentile_from_hist(hist, q):
    """q-th percentile (0-100) of values stored as {value: count}. Nearest-rank method."""
    total = sum(hist.values())
    if total == 0:
        return None
    rank = max(1, -(-q * total // 100))            # ceil(q/100 * total), at least 1
    seen = 0
    for value in sorted(hist):
        seen += hist[value]
        if seen >= rank:
            return value
    return max(hist)


def hist_mean(hist):
    total = sum(hist.values())
    return sum(v * c for v, c in hist.items()) / total if total else None


def shift_hist(hist, delta):
    """Histogram of (value - delta). Queue latency is e2e latency minus (commit - batch start)."""
    return Counter({v - delta: c for v, c in hist.items()})


def simple_percentile(values, q):
    return percentile_from_hist(Counter(values), q) if values else None


def summarize_run(config, producer, batches, e2e_hist, queue_hist, timed_out):
    """Build the one-row-per-run summary from the per-batch records."""
    received = sum(b["rows"] for b in batches)
    data = [b for b in batches if b["rows"] > 0]
    batch_ms = [b["batch_ms"] for b in data]
    first_send = producer.get("first_send_ms")
    last_commit = max((b["commit_ms"] for b in data), default=None)
    span_s = ((last_commit - first_send) / 1000
              if first_send is not None and last_commit is not None else None)
    processed = [b["processed_rps"] for b in data if b.get("processed_rps") is not None]
    trigger_ms = config["trigger_s"] * 1000
    p95 = simple_percentile(batch_ms, 95)
    r = lambda x, n=1: None if x is None else round(x, n)  # noqa: E731
    return {
        **{k: config[k] for k in ("run_id", "started_at", "rate_target", "trigger_s", "duration_s",
                                  "partitions", "sink", "delivery")},
        "events_sent": producer.get("sent", 0),
        "events_delivered": producer.get("delivered", 0),
        "events_received": received,
        "events_missing": producer.get("delivered", 0) - received,
        "producer_rate": producer.get("producer_rate"),
        # rows committed per second, from the first send to the last commit
        "pipeline_throughput": r(received / span_s if span_s else None),
        "spark_processed_rps_mean": r(sum(processed) / len(processed) if processed else None),
        "batches": len(data),
        "batch_rows_mean": r(received / len(data) if data else None),
        "batch_ms_mean": r(sum(batch_ms) / len(batch_ms) if batch_ms else None),
        "batch_ms_p95": p95,
        "batch_ms_max": max(batch_ms) if batch_ms else None,
        # Spark keeps up when 95% of micro-batches finish inside one trigger interval
        # (with trigger 0 there is no interval, so judge by a non-growing backlog instead).
        "keeps_up": (p95 is not None and p95 <= trigger_ms) if trigger_ms > 0 else
                    _backlog_flat(data),
        "queue_ms_p50": percentile_from_hist(queue_hist, 50),
        "queue_ms_p95": percentile_from_hist(queue_hist, 95),
        "queue_ms_p99": percentile_from_hist(queue_hist, 99),
        "e2e_ms_mean": r(hist_mean(e2e_hist)),
        "e2e_ms_p50": percentile_from_hist(e2e_hist, 50),
        "e2e_ms_p95": percentile_from_hist(e2e_hist, 95),
        "e2e_ms_p99": percentile_from_hist(e2e_hist, 99),
        "e2e_ms_max": max(e2e_hist) if e2e_hist else None,
        "timed_out": timed_out,
    }


def _backlog_flat(data):
    """True if median latency at the end of a run is not much worse than at the start."""
    if len(data) < 6:
        return None
    k = max(1, len(data) // 5)
    head = sorted(b["e2e_ms_p50"] for b in data[:k])[k // 2]
    tail = sorted(b["e2e_ms_p50"] for b in data[-k:])[k // 2]
    return tail <= 2 * head + 500


# ---------------------------------------------------------------- Kafka topics

def create_topic(bootstrap, topic, partitions):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic].result(30)
    # wait until the topic is visible with all partitions
    for _ in range(50):
        md = admin.list_topics(topic, timeout=5).topics.get(topic)
        if md and len(md.partitions) == partitions and not md.error:
            return
        time.sleep(0.2)
    raise RuntimeError(f"topic {topic} did not become ready")


def delete_topic(bootstrap, topic):
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})   # keep a reference until the future resolves
    try:
        admin.delete_topics([topic])[topic].result(30)
    except Exception as e:                                 # best effort clean-up
        print(f"  (could not delete topic {topic}: {e})")


def check_kafka(bootstrap):
    from confluent_kafka.admin import AdminClient
    try:
        AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=10)
    except Exception as e:
        sys.exit(f"Kafka is not reachable at {bootstrap} ({e}). Start it with: docker compose up -d")


# ---------------------------------------------------------------- one run

def run_once(spark, args, rate, trigger_s, repeat, label=None):
    from pyspark.sql.functions import col, floor, lit, when
    from pyspark.sql.functions import sum as fsum
    from streaming.stream_to_parquet import transform
    from ingestion.schema import VEHICLE_EVENT_FIELDS, spark_schema
    from pyspark.sql.functions import from_json

    run_id = f"{datetime.now():%Y%m%d_%H%M%S}_r{int(rate)}_t{trigger_s:g}_{label or repeat}"
    topic = f"bench-{run_id}".replace(".", "-")
    out_dir = os.path.join(args.work_dir, run_id)
    config = {"run_id": run_id, "started_at": datetime.now().isoformat(timespec="seconds"),
              "rate_target": rate, "trigger_s": trigger_s, "duration_s": args.duration,
              "partitions": args.partitions, "sink": args.sink, "delivery": args.delivery}
    print(f"\n== run {run_id}: {rate:g} events/s, trigger {trigger_s:g} s, "
          f"{args.duration:g} s + {args.warmup:g} s warm-up, sink {args.sink}", flush=True)

    create_topic(args.bootstrap, topic, args.partitions)
    query = None
    try:
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("failOnDataLoss", "false")
               .load())
        parsed = raw.select(from_json(col("value").cast("string"),
                                      spark_schema(VEHICLE_EVENT_FIELDS)).alias("e")).select("e.*")
        events = transform(parsed)                     # same columns/watermark as the archive job

        lock = threading.Lock()
        batches, e2e_hist, queue_hist = [], Counter(), Counter()
        bucket = args.bucket_ms

        def handle_batch(df, batch_id):
            t_start = int(time.time() * 1000)
            df = df.persist()
            n_total = df.count()                       # forces the read from Kafka
            t_read = int(time.time() * 1000)
            if n_total == 0:
                df.unpersist()
                return
            if args.sink == "parquet":
                (df.write.mode("append").partitionBy("event_date", "source")
                   .parquet(os.path.join(out_dir, "parquet")))
            else:
                df.write.format("noop").mode("overwrite").save()
            t_commit = int(time.time() * 1000)

            # 1 ms (or --bucket-ms) histogram of commit latency, bench rows only; warm-up rows just counted
            lat_bucket = floor((lit(t_commit) - col("ingest_ts")) / bucket) * bucket
            rows = (df.groupBy(when(col("source") == BENCH_SOURCE, lat_bucket).alias("b"))
                      .agg(fsum(lit(1)).alias("n")).collect())
            df.unpersist()
            hist = Counter({int(r["b"]): int(r["n"]) for r in rows if r["b"] is not None})
            warm = sum(int(r["n"]) for r in rows if r["b"] is None)
            n = sum(hist.values())
            record = {"batch_id": batch_id, "start_ms": t_start, "commit_ms": t_commit, "rows": n,
                      "warmup_rows": warm, "read_ms": t_read - t_start, "write_ms": t_commit - t_read,
                      "batch_ms": t_commit - t_start,
                      "queue_ms_p50": percentile_from_hist(shift_hist(hist, t_commit - t_start), 50),
                      "e2e_ms_p50": percentile_from_hist(hist, 50),
                      "e2e_ms_p95": percentile_from_hist(hist, 95),
                      "e2e_ms_max": max(hist) if hist else None}
            with lock:
                batches.append(record)
                e2e_hist.update(hist)
                queue_hist.update(shift_hist(hist, t_commit - t_start))
            if n and not args.quiet:
                print(f"  batch {batch_id:>4}: {n:>7} rows | {record['batch_ms']:>6} ms | "
                      f"e2e p50 {record['e2e_ms_p50']} ms p95 {record['e2e_ms_p95']} ms", flush=True)

        query = (events.writeStream
                 .foreachBatch(handle_batch)
                 .option("checkpointLocation", os.path.join(out_dir, "checkpoint"))
                 .trigger(processingTime=f"{trigger_s:g} seconds")
                 .queryName(f"bench_{run_id}".replace(".", "_"))
                 .start())

        # Wait until the query has run its first (empty) batch, so start-up is not measured.
        deadline = time.time() + 180
        while query.lastProgress is None and time.time() < deadline:
            if query.exception():
                raise query.exception()
            time.sleep(0.5)

        gen = subprocess.run(
            [sys.executable, os.path.join(ROOT, "benchmarks", "load_generator.py"),
             "--topic", topic, "--bootstrap", args.bootstrap, "--rate", str(rate),
             "--duration", str(args.duration), "--warmup", str(args.warmup),
             "--delivery", args.delivery, "--seed", str(repeat + 1)],
            capture_output=True, text=True)
        if gen.returncode != 0:
            raise RuntimeError(f"load generator failed:\n{gen.stderr[-2000:]}")
        producer = json.loads(gen.stdout.strip().splitlines()[-1])
        print(f"  producer: {producer['sent']} sent at {producer['producer_rate']} events/s, "
              f"{producer['delivered']} delivered", flush=True)

        # Drain: wait until every delivered bench event has been committed by Spark.
        timeout = args.drain_timeout or max(60, 6 * trigger_s + args.duration)
        deadline = time.time() + timeout
        timed_out = False
        while True:
            with lock:
                got = sum(b["rows"] for b in batches)
            if got >= producer["delivered"]:
                break
            if query.exception():
                raise query.exception()
            if time.time() > deadline:
                timed_out = True
                print(f"  drain timed out after {timeout:g} s: {got}/{producer['delivered']} committed")
                break
            time.sleep(0.5)

        progress = {p["batchId"]: p for p in query.recentProgress}
    finally:                                  # stop and clean up even if the run fails
        if query is not None:
            try:
                query.stop()
            except Exception:
                pass
        delete_topic(args.bootstrap, topic)
        if not args.keep_output:
            shutil.rmtree(out_dir, ignore_errors=True)

    t0 = producer["first_send_ms"]
    batch_rows = []
    for b in sorted(batches, key=lambda b: b["batch_id"]):
        p = progress.get(b["batch_id"], {})
        b["processed_rps"] = p.get("processedRowsPerSecond")
        batch_rows.append({"run_id": run_id, "rate_target": rate, "trigger_s": trigger_s,
                           "batch_id": b["batch_id"], "start_offset_s": round((b["start_ms"] - t0) / 1000, 3),
                           **{k: b[k] for k in ("rows", "warmup_rows", "read_ms", "write_ms", "batch_ms",
                                                "queue_ms_p50", "e2e_ms_p50", "e2e_ms_p95", "e2e_ms_max")},
                           "trigger_execution_ms": p.get("durationMs", {}).get("triggerExecution"),
                           "input_rps": _round(p.get("inputRowsPerSecond")),
                           "processed_rps": _round(p.get("processedRowsPerSecond"))})
    summary = summarize_run(config, producer, batches, e2e_hist, queue_hist, timed_out)
    print(f"  result: {summary['events_received']}/{summary['events_delivered']} rows, "
          f"throughput {summary['pipeline_throughput']} rows/s, e2e p50 {summary['e2e_ms_p50']} ms "
          f"p95 {summary['e2e_ms_p95']} ms p99 {summary['e2e_ms_p99']} ms, keeps up: {summary['keeps_up']}",
          flush=True)
    return summary, batch_rows


def _round(x):
    return None if x is None else round(x, 1)


# ---------------------------------------------------------------- main

def append_csv(path, columns, rows):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        if new:
            w.writeheader()
        w.writerows(rows)


def parse_list(text, cast=float):
    return [cast(x) for x in text.split(",") if x.strip()]


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rates", default="500,2000,5000,10000,20000", help="target events/s, comma list")
    p.add_argument("--triggers", default="1,5", help="micro-batch trigger intervals in seconds, comma list")
    p.add_argument("--repeats", type=int, default=1, help="runs per configuration")
    p.add_argument("--duration", type=float, default=45, help="seconds of measured load per run")
    p.add_argument("--warmup", type=float, default=5, help="seconds of unmeasured load before each run")
    p.add_argument("--jvm-warmup", type=float, default=20,
                   help="seconds of one discarded run before the sweep, to warm up the Spark JVM (0 = off)")
    p.add_argument("--partitions", type=int, default=3, help="partitions of each bench topic (live topics use 3)")
    p.add_argument("--sink", choices=["parquet", "noop"], default="parquet")
    p.add_argument("--delivery", choices=["at-most-once", "at-least-once", "exactly-once"],
                   default="at-least-once")
    p.add_argument("--bootstrap", default="localhost:29092",
                   help="Kafka address from this machine (docker-compose EXTERNAL listener)")
    p.add_argument("--bucket-ms", type=int, default=1, help="latency histogram resolution")
    p.add_argument("--drain-timeout", type=float, default=0, help="seconds to wait for the backlog (0 = auto)")
    p.add_argument("--results-dir", default=os.path.join(ROOT, "benchmarks", "results"))
    p.add_argument("--work-dir", default=os.path.join(ROOT, "data", "bench"))
    p.add_argument("--keep-output", action="store_true", help="keep each run's Parquet and checkpoint")
    p.add_argument("--quiet", action="store_true", help="do not print every micro-batch")
    args = p.parse_args()

    rates, triggers = parse_list(args.rates), parse_list(args.triggers)
    check_kafka(args.bootstrap)

    from streaming.stream_to_parquet import build_spark
    spark = build_spark(use_kafka=True)
    spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "10000")

    os.makedirs(args.results_dir, exist_ok=True)
    stamp = f"{datetime.now():%Y%m%d_%H%M}"
    runs_csv = os.path.join(args.results_dir, f"b22_runs_{stamp}.csv")
    batches_csv = os.path.join(args.results_dir, f"b22_batches_{stamp}.csv")
    print(f"B22 sweep: rates {rates} x triggers {triggers} x {args.repeats} repeat(s), sink {args.sink}")
    print(f"Results: {runs_csv}\n         {batches_csv}")

    if args.jvm_warmup > 0:
        # A fresh Spark JVM is slow for its first ~10 s of real load (JIT, Kafka consumers, Parquet
        # writers), which would make the first run look like it falls behind. Run once, discard it.
        warm = copy.copy(args)
        warm.duration, warm.warmup, warm.quiet = args.jvm_warmup, 0, True
        print(f"\nSpark warm-up run ({args.jvm_warmup:g} s at {max(rates):g} events/s, not recorded)")
        try:
            run_once(spark, warm, max(rates), triggers[0], 0, label="warmup")
        except Exception as e:
            print(f"  warm-up failed ({e}); continuing")

    failures = 0
    for repeat in range(args.repeats):
        for trigger_s in triggers:
            for rate in rates:
                try:
                    summary, batch_rows = run_once(spark, args, rate, trigger_s, repeat)
                except Exception as e:                    # keep the sweep going; note the failure
                    failures += 1
                    print(f"  RUN FAILED: {e}", flush=True)
                    continue
                append_csv(runs_csv, RUN_COLUMNS, [summary])   # write as we go: a crash keeps earlier runs
                append_csv(batches_csv, BATCH_COLUMNS, batch_rows)

    spark.stop()
    print(f"\nDone. {failures} failed run(s). Results in {runs_csv}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()

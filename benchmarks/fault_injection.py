"""
Fault injection and recovery for the Kafka -> Spark -> Parquet pipeline (B24).

Each trial runs the team's real archive job (streaming/stream_to_parquet.py) on a fresh
Kafka topic while a producer publishes numbered B4-schema events at a steady rate. Part way
through, one component is killed with SIGKILL (no clean shutdown) and brought back:

  kafka   docker kill <container>; wait --kafka-down s; docker start <container>
          The Spark job and the producer keep running and must reconnect on their own.
  spark   SIGKILL the archive job (Python driver + JVM); wait --spark-down s; start it
          again with the SAME checkpoint, as a supervisor / restart policy would.

If the Spark job dies on its own during a trial (e.g. because Kafka vanished), the harness
restarts it with the same checkpoint and counts it in `spark_crashes`.

After the producer stops, the harness drains the pipeline, then counts every event_id
  - acknowledged by Kafka to the producer,
  - stored in the topic (read back with a consumer), and
  - committed to Parquet (files listed in the sink's _spark_metadata log, which is what
    Spark readers see) and present on disk (every .parquet file, which is what pandas /
    pyarrow see when they read the folder directly),
so it can report events lost and duplicated at each stage. Recovery time comes from the
rows themselves: lag = spark_ts (micro-batch time) - ingest_ts (producer send time).

Outputs (benchmarks/results/, one timestamped set per sweep, appended trial by trial):
  b24_trials_<stamp>.csv     one row per trial: recovery times, loss and duplicates
  b24_summary_<stamp>.csv    one row per (fault, delivery): mean / min / max over trials
  b24_timeline_<stamp>.csv   one row per trial-second: produced, committed, lag (for plots)

Run from the repo root with Kafka up (docker compose up -d kafka), Java 17 and pyspark:
    python benchmarks/fault_injection.py                          # kafka then spark, 3 trials each
    python benchmarks/fault_injection.py --faults spark --trials 1 --duration 40 --kill-at 15
    benchmarks/kill_kafka.sh  /  benchmarks/kill_spark.sh          # one fault type only
"""

import argparse
import csv
import json
import os
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections import Counter
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from ingestion.schema import (SCHEMA_VERSION, VEHICLE_EVENT_FIELDS,  # noqa: E402
                              make_vehicle_event_id, validate)

FAULT_SOURCE = "B24"
QUIET = {"log_level": 0}      # librdkafka logs every failed reconnect during an outage; we report our own
DELIVERY_CONF = {
    # Same meanings as ingestion/av_replay.py --delivery.
    "at-most-once": {"acks": "0", "retries": 0, "enable.idempotence": False},
    "at-least-once": {"acks": "all", "retries": 2147483647, "enable.idempotence": False},
    "exactly-once": {"acks": "all", "enable.idempotence": True},   # idempotent producer
}

TRIAL_COLUMNS = [
    "trial_id", "started_at", "fault", "delivery", "rate_target", "duration_s", "kill_at_s",
    "downtime_s", "trigger_s", "partitions",
    # timing (seconds)
    "actual_downtime_s", "component_ready_s", "resume_s", "recovery_s", "max_lag_s",
    "baseline_lag_p50_ms", "baseline_lag_p95_ms", "recovery_threshold_ms", "drain_s",
    "spark_crashes", "spark_restarts",
    # counts
    "events_sent", "events_acked", "events_failed",
    "kafka_records", "kafka_distinct", "kafka_duplicates", "acked_missing_in_kafka",
    "parquet_rows", "parquet_distinct", "parquet_duplicates",
    "lost_end_to_end", "lost_acked", "delivered_despite_error",
    "raw_rows", "raw_duplicates", "orphan_files", "orphan_rows",
    "drain_timed_out",
]
TIMELINE_COLUMNS = ["trial_id", "fault", "delivery", "second", "phase", "produced", "committed",
                    "lag_ms_p50", "lag_ms_max"]
SUMMARY_METRICS = ["actual_downtime_s", "component_ready_s", "resume_s", "recovery_s", "max_lag_s"]
SUMMARY_TOTALS = ["events_acked", "lost_end_to_end", "lost_acked", "kafka_duplicates",
                  "parquet_duplicates", "raw_duplicates", "orphan_rows", "spark_crashes"]


# ---------------------------------------------------------------- pure helpers (tested)

def committed_files(metadata_dir):
    """Data files the Spark file sink has committed, from its _spark_metadata log.

    Each log file (batch id, or <id>.compact every 10 batches) is a "v1" header line followed
    by one JSON entry per file. A compact file repeats everything before it, so the union
    of every log file's "add" entries minus "delete" entries is the committed set.
    """
    if not os.path.isdir(metadata_dir):
        return set()
    added, deleted = set(), set()
    for name in os.listdir(metadata_dir):
        if name.startswith(".") or name.endswith(".tmp"):
            continue
        with open(os.path.join(metadata_dir, name)) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("v"):
                    continue
                entry = json.loads(line)
                path = entry["path"]
                path = path[len("file:"):] if path.startswith("file:") else path
                path = os.path.normpath("/" + path.lstrip("/"))
                (deleted if entry.get("action") == "delete" else added).add(path)
    return added - deleted


def parquet_files_on_disk(output_dir):
    """Every .parquet data file under the sink folder (what pandas/pyarrow read)."""
    found = set()
    for dirpath, dirnames, filenames in os.walk(output_dir):
        dirnames[:] = [d for d in dirnames if not d.startswith(("_", "."))]
        for name in filenames:
            if name.endswith(".parquet") and not name.startswith((".", "_")):
                found.add(os.path.normpath(os.path.join(dirpath, name)))
    return found


def account(sent_ids, acked_ids, failed_ids, kafka_counts, parquet_counts):
    """Loss and duplicate counts for one trial.

    sent_ids / acked_ids / failed_ids: sets of event_id from the producer side.
    kafka_counts / parquet_counts: Counter of event_id -> copies stored.
    """
    kafka_ids, parquet_ids = set(kafka_counts), set(parquet_counts)
    return {
        "events_sent": len(sent_ids),
        "events_acked": len(acked_ids),
        "events_failed": len(failed_ids),
        "kafka_records": sum(kafka_counts.values()),
        "kafka_distinct": len(kafka_ids),
        "kafka_duplicates": sum(kafka_counts.values()) - len(kafka_ids),
        "acked_missing_in_kafka": len(acked_ids - kafka_ids),
        "parquet_rows": sum(parquet_counts.values()),
        "parquet_distinct": len(parquet_ids),
        "parquet_duplicates": sum(parquet_counts.values()) - len(parquet_ids),
        # Anything the producer handed over that never reached Parquet.
        "lost_end_to_end": len(sent_ids - parquet_ids),
        # The serious kind: Kafka said "stored" and it still never reached Parquet.
        "lost_acked": len(acked_ids - parquet_ids),
        # Reported failed to the producer (which would resend) but stored anyway.
        "delivered_despite_error": len(failed_ids & parquet_ids),
    }


def percentile(values, q):
    """Nearest-rank q-th percentile (0-100) of a list; None if empty."""
    if not values:
        return None
    s = sorted(values)
    rank = max(1, -(-q * len(s) // 100))
    return s[min(rank, len(s)) - 1]


def recovery_times(rows, t_kill, t_restart, threshold_ms):
    """When the pipeline produced output again, and when it was back to normal.

    rows: (ingest_ms, spark_ms) per committed event. Rows sharing spark_ms belong to one
    micro-batch. Returns (resume_s, recovery_s, max_lag_s), times measured from t_kill (ms):
      resume_s    first micro-batch at or after the restart
      recovery_s  first micro-batch after the restart whose worst lag is within threshold_ms,
                  i.e. the backlog built up during the outage has been worked off
    """
    batches = {}
    for ingest_ms, spark_ms in rows:
        batches[spark_ms] = max(batches.get(spark_ms, 0), spark_ms - ingest_ms)
    max_lag = max(batches.values()) / 1000 if batches else None
    after = sorted(t for t in batches if t >= t_restart)
    resume = (after[0] - t_kill) / 1000 if after else None
    recovery = next(((t - t_kill) / 1000 for t in after if batches[t] <= threshold_ms), None)
    return resume, recovery, max_lag


def recovery_threshold_ms(baseline_lags_ms, trigger_s):
    """Lag that counts as 'normal again': twice the pre-fault p95, at least one trigger."""
    p95 = percentile(baseline_lags_ms, 95) or 0
    return max(2 * p95, p95 + trigger_s * 1000)


def timeline(rows, ingest_all_ms, t_kill, t_restart, t_recovered):
    """Per-second counts relative to the kill: events produced, committed, and their lag."""
    produced = Counter((t - t_kill) // 1000 for t in ingest_all_ms)
    committed, lags = Counter(), {}
    for ingest_ms, spark_ms in rows:
        sec = (spark_ms - t_kill) // 1000
        committed[sec] += 1
        lags.setdefault(sec, []).append(spark_ms - ingest_ms)
    seconds = sorted(set(produced) | set(committed))
    out = []
    for sec in seconds:
        t = t_kill + sec * 1000
        phase = ("before" if t < t_kill else "down" if t < t_restart
                 else "recovering" if t_recovered is None or t < t_recovered else "after")
        lag = lags.get(sec, [])
        out.append({"second": int(sec), "phase": phase, "produced": produced.get(sec, 0),
                    "committed": committed.get(sec, 0),
                    "lag_ms_p50": percentile(lag, 50), "lag_ms_max": max(lag) if lag else None})
    return out


def summarize(trials):
    """One row per (fault, delivery) with mean/min/max timings and summed counts."""
    groups = {}
    for t in trials:
        groups.setdefault((t["fault"], t["delivery"]), []).append(t)
    out = []
    for (fault, delivery), rows in groups.items():
        s = {"fault": fault, "delivery": delivery, "trials": len(rows)}
        for m in SUMMARY_METRICS:
            vals = [r[m] for r in rows if r.get(m) not in (None, "")]
            s[f"{m}_mean"] = round(statistics.mean(vals), 2) if vals else None
            s[f"{m}_min"] = round(min(vals), 2) if vals else None
            s[f"{m}_max"] = round(max(vals), 2) if vals else None
        for m in SUMMARY_TOTALS:
            s[f"{m}_total"] = sum(int(r.get(m) or 0) for r in rows)
        s["trials_unrecovered"] = sum(1 for r in rows if r.get("recovery_s") in (None, ""))
        out.append(s)
    return out


# ---------------------------------------------------------------- Kafka helpers

def kafka_ready(bootstrap, topic=None, timeout=2.0):
    """True when the broker answers and (if given) every partition of topic has a leader."""
    from confluent_kafka.admin import AdminClient
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap, **QUIET})
        md = admin.list_topics(timeout=timeout)
    except Exception:
        return False
    if not md.brokers:
        return False
    if topic is None:
        return True
    t = md.topics.get(topic)
    return bool(t and t.error is None and t.partitions
                and all(p.leader >= 0 and p.error is None for p in t.partitions.values()))


def wait_kafka(bootstrap, topic=None, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        if kafka_ready(bootstrap, topic):
            return time.time()
        time.sleep(0.25)
    raise RuntimeError(f"Kafka at {bootstrap} not ready after {timeout}s")


def create_topic(bootstrap, topic, partitions):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap, **QUIET})
    admin.create_topics([NewTopic(topic, partitions, 1)])[topic].result(30)
    wait_kafka(bootstrap, topic, timeout=60)


def delete_topic(bootstrap, topic):
    from confluent_kafka.admin import AdminClient
    try:
        admin = AdminClient({"bootstrap.servers": bootstrap, **QUIET})   # keep the handle alive
        admin.delete_topics([topic])[topic].result(30)
    except Exception as e:                      # cleanup only; never fail a sweep over it
        print(f"  (could not delete topic {topic}: {e!r})", flush=True)


def read_topic(bootstrap, topic):
    """Counter of event_id over every record in the topic, from the beginning."""
    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"b24-audit-{uuid.uuid4().hex[:8]}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest", **QUIET})
    md = c.list_topics(topic, timeout=30)
    parts = list(md.topics[topic].partitions)
    high = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1] for p in parts}
    c.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
    pos = {p: 0 for p in parts}
    counts = Counter()
    deadline = time.time() + 120
    while any(pos[p] < high[p] for p in parts) and time.time() < deadline:
        for m in c.consume(5000, timeout=1.0):
            if m.error():
                continue
            pos[m.partition()] = m.offset() + 1
            counts[json.loads(m.value())["event_id"]] += 1
    c.close()
    return counts


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout.strip()


# ---------------------------------------------------------------- producer

class SeqProducer(threading.Thread):
    """Publishes B4 vehicle events at a steady rate and records each delivery report."""

    def __init__(self, bootstrap, topic, rate, duration, delivery, vehicles=100):
        super().__init__(daemon=True)
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": bootstrap, "linger.ms": 5, **QUIET,
                "message.timeout.ms": 300000,          # keep retrying through a 5 min outage
                "queue.buffering.max.messages": 1000000}
        conf.update(DELIVERY_CONF[delivery])
        self.producer = Producer(conf)
        self.topic, self.rate, self.duration, self.vehicles = topic, rate, duration, vehicles
        self.sent, self.acked, self.failed = set(), set(), set()
        self.ingest_ms = []
        self.errors = Counter()
        self.start_time = None
        self._last_ts = {}

    def _report(self, err, msg):
        eid = msg.key().decode().split("|", 1)[1]
        if err is None:
            self.acked.add(eid)
        else:
            self.failed.add(eid)
            self.errors[err.name()] += 1

    def _event(self, seq, now):
        vid = f"fault-v{seq % self.vehicles:03d}"
        ts = max(now, self._last_ts.get(vid, 0) + 1)        # event_id must be unique
        self._last_ts[vid] = ts
        ev = {"event_id": make_vehicle_event_id(FAULT_SOURCE, vid, ts), "schema_version": SCHEMA_VERSION,
              "source": FAULT_SOURCE, "vehicle_id": vid, "route_id": f"R{seq % 7}", "trip_id": f"seq-{seq}",
              "direction_id": seq % 2, "event_ts": ts, "ingest_ts": now,
              "lat": 37.77 + (seq % 1000) * 1e-5, "lon": -122.42, "bearing": None, "speed_mps": None,
              "current_status": "IN_TRANSIT_TO", "stop_id": None}
        assert not validate(ev, VEHICLE_EVENT_FIELDS)
        return ev

    def run(self):
        self.start_time = time.time()
        seq = 0
        while True:
            elapsed = time.time() - self.start_time
            if elapsed >= self.duration:
                break
            due = int(elapsed * self.rate)
            while seq < due:
                now = int(time.time() * 1000)
                ev = self._event(seq, now)
                # Key = vehicle id (partitioning, like the live producers) plus the event id so
                # the delivery report can be matched without decoding the value.
                key = f"{ev['vehicle_id']}|{ev['event_id']}"
                while True:
                    try:
                        self.producer.produce(self.topic, key=key, value=json.dumps(ev), on_delivery=self._report)
                        break
                    except BufferError:
                        self.producer.poll(0.1)
                self.sent.add(ev["event_id"])
                self.ingest_ms.append(now)
                seq += 1
            self.producer.poll(0)
            time.sleep(0.005)

    def flush(self, timeout):
        left = self.producer.flush(timeout)
        return left


# ---------------------------------------------------------------- Spark job (the real archive job)

class SparkJob:
    """streaming/stream_to_parquet.py as a child process, killable as a whole process group."""

    def __init__(self, args, topic, output, checkpoint, log_dir):
        self.cmd = [sys.executable, os.path.join(ROOT, "streaming", "stream_to_parquet.py"),
                    "--source", "kafka", "--bootstrap", args.bootstrap, "--topic", topic,
                    "--output", output, "--checkpoint", checkpoint, "--trigger", str(args.trigger)]
        self.log_dir = log_dir
        self.proc = None
        self.ready_at = None
        self.starts = 0
        self.expected_exit = False

    def start(self):
        self.starts += 1
        self.ready_at = None
        self.expected_exit = False
        log = open(os.path.join(self.log_dir, f"spark_{self.starts}.log"), "w")
        env = dict(os.environ, PYTHONUNBUFFERED="1")   # the job's prints must reach us line by line
        self.proc = subprocess.Popen(self.cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     text=True, bufsize=1, start_new_session=True)
        threading.Thread(target=self._pump, args=(self.proc, log), daemon=True).start()

    def _pump(self, proc, log):
        for line in proc.stdout:
            log.write(f"{time.time():.3f} {line}")
            log.flush()
            if line.startswith("Streaming from") and proc is self.proc:
                self.ready_at = time.time()
        log.close()

    def wait_ready(self, timeout=300):
        end = time.time() + timeout
        while time.time() < end:
            if self.ready_at:
                return self.ready_at
            if self.proc.poll() is not None:
                raise RuntimeError(f"Spark job exited during start-up; see {self.log_dir}")
            time.sleep(0.2)
        raise RuntimeError("Spark job did not start in time")

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def kill(self):
        """SIGKILL the driver and its JVM: no shutdown hooks, no final commit."""
        self.expected_exit = True
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()

    def stop(self, timeout=60):
        """Clean stop (Ctrl+C), the way an operator would end the run."""
        if not self.alive():
            return
        self.expected_exit = True
        os.killpg(self.proc.pid, signal.SIGINT)
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.kill()


# ---------------------------------------------------------------- reading the sink

def read_rows(files):
    """Counter of event_id and (ingest_ms, spark_ms) rows from a set of Parquet files."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    per_ms = {"s": None, "ms": 1, "us": 1000, "ns": 1_000_000}
    counts, rows = Counter(), []
    for path in files:
        try:
            t = pq.read_table(path, columns=["event_id", "ingest_ts", "spark_ts"])
        except Exception:
            continue                              # half-written file from a killed batch
        # Spark writes INT96 (read as ns) or INT64 micros depending on config: normalise to ms.
        unit = t.schema.field("spark_ts").type.unit
        raw = t["spark_ts"].cast(pa.int64()).to_pylist()
        spark_ms = [v * 1000 if per_ms[unit] is None else v // per_ms[unit] for v in raw]
        counts.update(t["event_id"].to_pylist())
        rows.extend(zip(t["ingest_ts"].to_pylist(), spark_ms))
    return counts, rows


def committed_distinct(output):
    counts, _ = read_rows(committed_files(os.path.join(output, "_spark_metadata")))
    return len(counts)


# ---------------------------------------------------------------- one trial

def run_trial(args, fault, delivery, trial_no, run_id, workdir):
    trial_id = f"{run_id}_{fault}_{delivery}_{trial_no}"
    topic = f"b24-{trial_id}".replace("_", "-")
    tdir = os.path.join(workdir, trial_id)
    output, checkpoint = os.path.join(tdir, "out"), os.path.join(tdir, "checkpoint")
    os.makedirs(tdir, exist_ok=True)
    print(f"\n== {trial_id}: kill {fault} at {args.kill_at}s, producer {delivery} at {args.rate:g} ev/s",
          flush=True)

    wait_kafka(args.bootstrap)
    create_topic(args.bootstrap, topic, args.partitions)
    spark = SparkJob(args, topic, output, checkpoint, tdir)
    stop_sup = threading.Event()
    try:
        return _trial(args, fault, delivery, trial_id, output, spark, stop_sup)
    finally:                                         # never leak a Spark job or a topic
        stop_sup.set()
        if spark.alive():
            spark.kill()
        delete_topic(args.bootstrap, topic)
        if not args.keep_output:
            shutil.rmtree(tdir, ignore_errors=True)


def _trial(args, fault, delivery, trial_id, output, spark, stop_sup):
    topic = spark.cmd[spark.cmd.index("--topic") + 1]
    spark.start()
    spark.wait_ready()
    time.sleep(args.settle)                          # first (empty) micro-batches, JVM warm-up

    marks = {"crashes": 0}
    lock = threading.Lock()

    def supervise(stop_event):
        """Restart the Spark job if it dies by itself (with the same checkpoint)."""
        while not stop_event.is_set():
            with lock:
                if spark.proc and not spark.alive() and not spark.expected_exit:
                    marks["crashes"] += 1
                    marks.setdefault("crash_at", time.time())
                    print(f"  Spark job exited by itself (code {spark.proc.returncode}); restarting", flush=True)
                    while not kafka_ready(args.bootstrap) and not stop_event.is_set():
                        time.sleep(0.5)
                    spark.start()
            stop_event.wait(0.5)

    threading.Thread(target=supervise, args=(stop_sup,), daemon=True).start()

    prod = SeqProducer(args.bootstrap, topic, args.rate, args.duration, delivery)
    prod.start()
    while prod.start_time is None:
        time.sleep(0.01)
    time.sleep(max(0, prod.start_time + args.kill_at - time.time()))

    # ---- inject the fault
    t_kill = time.time()
    if fault == "kafka":
        docker("kill", args.container)
        print(f"  killed Kafka container '{args.container}'", flush=True)
        time.sleep(args.kafka_down)
        t_restart = time.time()
        docker("start", args.container)
        t_ready = wait_kafka(args.bootstrap, topic)
        print(f"  Kafka back after {t_ready - t_restart:.1f}s", flush=True)
    else:
        with lock:
            spark.kill()
            print(f"  killed Spark job (SIGKILL, pid group {spark.proc.pid})", flush=True)
            time.sleep(args.spark_down)
            t_restart = time.time()
            spark.start()
        t_ready = spark.wait_ready()
        print(f"  Spark job back after {t_ready - t_restart:.1f}s", flush=True)

    prod.join()
    left = prod.flush(120)
    if left:
        print(f"  {left} messages still unacknowledged after flush", flush=True)

    # ---- drain: wait until Spark has committed everything that is in the topic
    kafka_counts = read_topic(args.bootstrap, topic)
    t_drain = time.time()
    timed_out = True
    while time.time() - t_drain < args.drain_timeout:
        if committed_distinct(output) >= len(kafka_counts):
            timed_out = False
            break
        time.sleep(max(1, args.trigger))
    drain_s = time.time() - t_drain
    stop_sup.set()
    with lock:
        spark.stop()

    # ---- count
    committed = committed_files(os.path.join(output, "_spark_metadata"))
    on_disk = parquet_files_on_disk(output)
    parquet_counts, rows = read_rows(committed)
    raw_counts, _ = read_rows(on_disk)
    orphans = on_disk - committed
    orphan_counts, _ = read_rows(orphans)
    acct = account(prod.sent, prod.acked, prod.failed, kafka_counts, parquet_counts)

    kill_ms, restart_ms = int(t_kill * 1000), int(t_restart * 1000)
    baseline = [s - i for i, s in rows if i < kill_ms - args.trigger * 1000 and s < kill_ms]
    threshold = recovery_threshold_ms(baseline, args.trigger)
    resume, recovery, max_lag = recovery_times(rows, kill_ms, restart_ms, threshold)

    trial = {
        "trial_id": trial_id, "started_at": datetime.fromtimestamp(prod.start_time).isoformat(timespec="seconds"),
        "fault": fault, "delivery": delivery, "rate_target": args.rate, "duration_s": args.duration,
        "kill_at_s": args.kill_at, "downtime_s": args.kafka_down if fault == "kafka" else args.spark_down,
        "trigger_s": args.trigger, "partitions": args.partitions,
        "actual_downtime_s": round(t_restart - t_kill, 2), "component_ready_s": round(t_ready - t_restart, 2),
        "resume_s": round(resume, 2) if resume is not None else None,
        "recovery_s": round(recovery, 2) if recovery is not None else None,
        "max_lag_s": round(max_lag, 2) if max_lag is not None else None,
        "baseline_lag_p50_ms": percentile(baseline, 50), "baseline_lag_p95_ms": percentile(baseline, 95),
        "recovery_threshold_ms": threshold, "drain_s": round(drain_s, 1),
        "spark_crashes": marks["crashes"], "spark_restarts": spark.starts - 1,
        **acct,
        "raw_rows": sum(raw_counts.values()), "raw_duplicates": sum(raw_counts.values()) - len(raw_counts),
        "orphan_files": len(orphans), "orphan_rows": sum(orphan_counts.values()),
        "drain_timed_out": timed_out,
    }
    recovered_ms = kill_ms + int(recovery * 1000) if recovery is not None else None
    tl = [{"trial_id": trial_id, "fault": fault, "delivery": delivery, **r}
          for r in timeline(rows, prod.ingest_ms, kill_ms, restart_ms, recovered_ms)]

    print(f"  resume {trial['resume_s']}s, recovered {trial['recovery_s'] or 'n/a (load ended first) '}s after the kill | "
          f"acked {acct['events_acked']}, lost {acct['lost_end_to_end']} (acked {acct['lost_acked']}), "
          f"dup kafka {acct['kafka_duplicates']} / parquet {acct['parquet_duplicates']} / "
          f"raw folder {trial['raw_duplicates']}, crashes {marks['crashes']}", flush=True)
    if prod.errors:
        print(f"  producer delivery errors: {dict(prod.errors)}", flush=True)

    return trial, tl


# ---------------------------------------------------------------- CSV output

class CsvAppender:
    def __init__(self, path, columns):
        self.path, self.columns = path, columns
        with open(path, "w", newline="") as f:
            csv.DictWriter(f, columns).writeheader()

    def add(self, rows):
        with open(self.path, "a", newline="") as f:
            w = csv.DictWriter(f, self.columns, extrasaction="ignore")
            for r in rows:
                w.writerow({k: ("" if r.get(k) is None else r.get(k)) for k in self.columns})


def write_summary(path, trials):
    rows = summarize(trials)
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if v is None else v) for k, v in r.items()})


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--faults", default="kafka,spark", help="comma list, run in this order: kafka, spark")
    p.add_argument("--kafka-deliveries", default="at-most-once,at-least-once,exactly-once",
                   help="producer guarantees to test under a Kafka kill")
    p.add_argument("--spark-deliveries", default="at-least-once",
                   help="producer guarantees to test under a Spark kill (the producer is not affected)")
    p.add_argument("--trials", type=int, default=3, help="repeats per (fault, delivery)")
    p.add_argument("--spark-trials", type=int, default=None,
                   help="repeats for the Spark kill (default: --trials). A kill only leaves orphan files when it "
                        "lands inside a micro-batch's write, so more repeats catch that case more often")
    p.add_argument("--rate", type=float, default=500, help="events per second")
    p.add_argument("--duration", type=float, default=100,
                   help="seconds the producer runs; leave well over a minute after the kill so the backlog can clear")
    p.add_argument("--kill-at", type=float, default=30, help="seconds after the producer starts")
    p.add_argument("--kafka-down", type=float, default=10, help="seconds Kafka stays dead")
    p.add_argument("--spark-down", type=float, default=5, help="seconds before the Spark job is restarted")
    p.add_argument("--trigger", type=int, default=2, help="Spark micro-batch trigger, seconds")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--settle", type=float, default=10, help="seconds Spark runs empty before the load")
    p.add_argument("--drain-timeout", type=float, default=180)
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--container", default="kafka", help="Kafka docker container name")
    p.add_argument("--workdir", default=os.path.join(ROOT, "data", "fault_injection"))
    p.add_argument("--results", default=os.path.join(ROOT, "benchmarks", "results"))
    p.add_argument("--keep-output", action="store_true", help="keep each trial's Parquet, checkpoint and logs")
    args = p.parse_args()
    if args.kill_at >= args.duration:
        p.error("--kill-at must be smaller than --duration")

    faults = [f.strip() for f in args.faults.split(",") if f.strip()]
    for f in faults:
        if f not in ("kafka", "spark"):
            p.error(f"unknown fault {f!r}")
    plan = []
    for f in faults:
        dels = (args.kafka_deliveries if f == "kafka" else args.spark_deliveries).split(",")
        for d in dels:
            if d not in DELIVERY_CONF:
                p.error(f"unknown delivery {d!r}")
            n = args.spark_trials if f == "spark" and args.spark_trials is not None else args.trials
            plan += [(f, d, i) for i in range(n)]

    if "kafka" in faults:
        try:
            docker("inspect", "-f", "{{.State.Running}}", args.container)
        except subprocess.CalledProcessError:
            sys.exit(f"No docker container named '{args.container}'. Start Kafka: docker compose up -d kafka")
    wait_kafka(args.bootstrap, timeout=60)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.results, exist_ok=True)
    os.makedirs(args.workdir, exist_ok=True)
    trials_csv = CsvAppender(os.path.join(args.results, f"b24_trials_{stamp}.csv"), TRIAL_COLUMNS)
    timeline_csv = CsvAppender(os.path.join(args.results, f"b24_timeline_{stamp}.csv"), TIMELINE_COLUMNS)
    summary_path = os.path.join(args.results, f"b24_summary_{stamp}.csv")
    print(f"{len(plan)} trials, about {len(plan) * (args.duration + args.settle + 40) / 60:.0f} min. "
          f"Results: {trials_csv.path}", flush=True)

    done = []
    for fault, delivery, i in plan:
        try:
            trial, tl = run_trial(args, fault, delivery, i, run_id, args.workdir)
        except Exception as e:
            print(f"  trial failed: {e!r}", flush=True)
            if fault == "kafka":                      # never leave Kafka dead
                subprocess.run(["docker", "start", args.container], capture_output=True)
            continue
        done.append(trial)
        trials_csv.add([trial])
        timeline_csv.add(tl)
        write_summary(summary_path, done)

    print(f"\nWrote {trials_csv.path}\n      {summary_path}\n      {timeline_csv.path}")
    for s in summarize(done):
        print(f"  {s['fault']:5} {s['delivery']:13} resume {s['resume_s_mean']}s  recovery {s['recovery_s_mean']}s  "
              f"lost(acked) {s['lost_acked_total']}  dup parquet {s['parquet_duplicates_total']}  "
              f"dup raw {s['raw_duplicates_total']}  crashes {s['spark_crashes_total']}")


if __name__ == "__main__":
    main()

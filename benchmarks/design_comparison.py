"""
Design comparison (B25): window size x delivery guarantee for the bus-bunching detector.

Every combination runs the team's real detector (streaming/detect_bunching.py) on the SAME
workload, read from Kafka, and is scored against an exact offline answer:

  window size   1, 5 and 10 minutes (--window of the detector: how much event time each alert
                covers; the 5-minute movement look-back stays the same)
  delivery      at-least-once  alerts appended to Parquet (the detector's default sink)
                exactly-once   idempotent sink: each micro-batch overwrites <output>/batch_id=<n>

Each run is crashed once, part way through: right after a micro-batch has written its alerts and
before Spark commits that batch (SIGKILL to driver + JVM, the at-least-once window). The job is
restarted on the same checkpoint and runs the batch again, as a supervisor would restart it (B24).

The workload is made once per sweep from a fixed seed with fake_feed.py's vehicles and planters,
on a simulated clock (no waiting): --vehicles buses on 4 routes reporting every --interval s for
--hours, a planted bunching event every --bunch-every reports, and --late of the reports delivered
1-10 minutes late. The last record is a "flush" report far in the future, so every window closes.
The whole workload is produced to a fresh Kafka topic per run (idempotent producer) and Spark reads
it in micro-batches of --max-offsets records, which makes the replay behave like a live feed.

Two answers to score against:
  exact     the detector's own rule (bunching_pairs) on ALL reports, grouped into the same Spark
            windows, with no watermark: what the detector would say if no report were late (B18 idea).
  planted   the bunching events planted in the feed, matched like streaming/evaluate_bunching.py.

Outputs (benchmarks/results/):
  b25_runs_<stamp>.csv        one row per (window, delivery): recall, precision, alert delay,
                              duplicate alerts, reports dropped as late, state size, batch time
  b25_workload_<stamp>.json   the workload settings and counts

Run from the repo root with Kafka up (docker compose up -d kafka), Java 17 and pyspark:
    python benchmarks/design_comparison.py                        # 3 windows x 2 deliveries, ~25 min
    python benchmarks/design_comparison.py --windows 60 --deliveries exactly-once --hours 0.5   # quick check
    python benchmarks/plot_design_comparison.py                   # table + plot from the newest CSV
"""

import argparse
import csv
import json
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from benchmarks.fault_injection import create_topic, delete_topic, wait_kafka  # noqa: E402
from ingestion.schema import SCHEMA_VERSION, VEHICLE_EVENT_FIELDS, make_vehicle_event_id, validate  # noqa: E402
from streaming import fake_feed  # noqa: E402
from streaming.config import BUNCHING  # noqa: E402
from streaming.detect_bunching import bunching_pairs  # noqa: E402

DELIVERY_SINK = {"at-least-once": "append", "exactly-once": "idempotent"}
SIM_START_MS = int(datetime(2026, 11, 10, 16, 0, tzinfo=timezone.utc).timestamp() * 1000)  # 08:00 Pacific
FLUSH_AHEAD_MS = 2 * 3600 * 1000

RUN_COLUMNS = [
    "run_id", "window_s", "delivery", "sink_mode", "watermark_s", "lookback_s",
    # what Spark saw
    "records", "batches", "batch_ms_mean", "batch_ms_p95", "state_rows_max", "state_mb_max",
    "window_rows", "late_window_rows_dropped", "late_drop_pct",
    # the crash
    "crashed", "crash_batch", "restart_to_first_batch_s",
    # alerts
    "alert_rows", "alerts_distinct", "duplicate_alert_rows", "close_pair_rows", "duplicate_close_pair_rows",
    # scored against the exact answer
    "exact_alerts", "true_alerts", "recall_exact", "precision_exact",
    # scored against the planted events
    "planted_judged", "planted_caught", "recall_planted", "planted_caught_exact",
    "alert_delay_s_median", "alert_delay_s_p90", "alert_delay_s_max",
    "wall_s",
]


# ---------------------------------------------------------------- workload (pure, tested)

def make_workload(vehicles=40, interval_s=10, hours=2.0, late=0.1, bunch_every=10, bunch_length=6,
                  bunch_gap_m=60.0, seed=25, planted_log=None):
    """Simulated feed with fake_feed.py's vehicles and bunching planter, without waiting.

    Returns (events in arrival order, planted bunching events). Late reports keep their old
    event_ts and arrive 1-10 minutes later, as in fake_feed.py --late. The last event is a
    "flush" report 2 h ahead of the rest, so the watermark passes every real window.
    """
    random.seed(seed)
    if planted_log and os.path.exists(planted_log):
        os.remove(planted_log)
    fleet = fake_feed.make_vehicles(vehicles)
    planter = fake_feed.BunchingPlanter(fleet, bunch_every, bunch_length, bunch_gap_m, os.devnull)
    planted = []
    planter.write = planted.append                  # keep the log in memory too
    events, held_back = [], []
    ticks = int(hours * 3600 / interval_s)
    for tick in range(ticks):
        now_ms = SIM_START_MS + tick * interval_s * 1000
        for v in fleet:
            fake_feed.step(v, interval_s)
        planter.before_batch(tick, now_ms)
        for v in fleet:
            event_ts = now_ms - v["lag_ms"] - random.randint(0, 1000)
            event = fake_feed.to_event(v, event_ts, now_ms)
            if random.random() < late:
                held_back.append((now_ms + random.randint(60_000, 600_000), event))
                continue
            events.append(event)
        ready = sorted((t, i, e) for i, (t, e) in enumerate(held_back) if t <= now_ms)
        held_back = [(t, e) for t, e in held_back if t > now_ms]
        for t, _, e in ready:
            e["ingest_ts"] = now_ms
            events.append(e)
        planter.after_batch(now_ms)
    planted = [p for p in planted if p["type"] == "planted_bunching"]
    if planted_log:
        with open(planted_log, "w") as f:
            for p in planted:
                f.write(json.dumps(p) + "\n")
    events.append(flush_event(max(e["event_ts"] for e in events) + FLUSH_AHEAD_MS))
    for e in events:
        problems = validate(e, VEHICLE_EVENT_FIELDS)
        if problems:
            raise ValueError(f"workload event breaks the schema: {problems}")
    return events, planted


def flush_event(event_ts):
    """One report on a route of its own, far ahead in event time: pushes the watermark past
    every real window so the detector emits them all (no bunching possible with one bus)."""
    return {"event_id": make_vehicle_event_id("flush", "flush", event_ts), "schema_version": SCHEMA_VERSION,
            "source": "flush", "vehicle_id": "flush", "route_id": "flush", "trip_id": None,
            "direction_id": 0, "event_ts": event_ts, "ingest_ts": event_ts, "lat": 0.0, "lon": 0.0,
            "bearing": None, "speed_mps": None, "current_status": None, "stop_id": None}


# ---------------------------------------------------------------- exact answer (pure, tested)

def spark_windows(ts_ms, span_ms, slide_ms):
    """(start, end) of every sliding window that holds ts_ms, as Spark's window() makes them
    (starts at multiples of slide from the epoch, start <= ts < end)."""
    first = (ts_ms - span_ms) // slide_ms + 1
    last = ts_ms // slide_ms
    return [(k * slide_ms, k * slide_ms + span_ms) for k in range(first, last + 1)]


def alert_key(source, route_id, direction_id, window_start_ms, vehicle_a, vehicle_b):
    return (source, str(route_id), int(direction_id), int(window_start_ms), *sorted((vehicle_a, vehicle_b)))


def exact_alerts(events, window_s, settings=BUNCHING):
    """The detector's rule on every report, no watermark: {alert_key: alert dict} for is_bunching pairs.

    Same grouping as detect_bunching.build_query: per (source, route, direction), sliding windows
    of (look-back + window) seconds moving window seconds at a time; pairs are judged in the last
    window seconds of each (the "current" part)."""
    win_ms = window_s * 1000
    span_ms = (settings["lookback_s"] + window_s) * 1000 if settings["min_move_m"] > 0 else win_ms
    groups = defaultdict(list)
    for e in events:
        if e.get("route_id") is None or e.get("direction_id") is None:
            continue
        for w in spark_windows(e["event_ts"], span_ms, win_ms):
            groups[(e["source"], e["route_id"], e["direction_id"], w)].append(e)
    out = {}
    for (source, route, direction, (start, end)), reports in groups.items():
        for p in bunching_pairs(reports, end - win_ms, settings["max_distance_m"], settings["max_time_gap_s"],
                                settings["min_move_m"], settings["min_history_s"], settings["tandem_m"]):
            if not p[5]:                       # is_bunching
                continue
            key = alert_key(source, route, direction, end - win_ms, p[0], p[1])
            out[key] = {"source": source, "route_id": route, "direction_id": direction,
                        "start_ms": end - win_ms, "end_ms": end, "vehicle_a": p[0], "vehicle_b": p[1],
                        "category": p[4]}
    return out


# ---------------------------------------------------------------- scoring (pure, tested)

def match_planted(planted, alerts, watermark_s):
    """Planted events caught by alerts, matched like evaluate_bunching.py (same route, direction
    and pair of buses, alert window overlapping the planted time).

    alerts: dicts with route_id, direction_id, vehicle_a, vehicle_b, start_ms, end_ms.
    Returns (caught, delays_s): delay = when the first matching alert can be emitted (its window
    end + watermark, in event time) minus when the bunching started.
    """
    by_route = defaultdict(list)
    for a in alerts:
        by_route[(str(a["route_id"]), int(a["direction_id"]))].append(a)
    caught, delays = 0, []
    for ev in planted:
        pair = {ev["vehicle_a"], ev["vehicle_b"]}
        hits = [a for a in by_route[(str(ev["route_id"]), int(ev["direction_id"]))]
                if {a["vehicle_a"], a["vehicle_b"]} == pair
                and a["end_ms"] >= ev["start_ts"] and a["start_ms"] <= ev["end_ts"]]
        if hits:
            caught += 1
            first_end = min(a["end_ms"] for a in hits)
            delays.append((first_end + watermark_s * 1000 - ev["start_ts"]) / 1000)
    return caught, delays


def score(stream_rows, exact, planted, watermark_s):
    """Duplicates, recall and precision of one run's alert rows (bunching only) against the answers."""
    keys = [alert_key(r["source"], r["route_id"], r["direction_id"], r["start_ms"], r["vehicle_a"], r["vehicle_b"])
            for r in stream_rows]
    distinct = {}
    for k, r in zip(keys, stream_rows):
        distinct.setdefault(k, r)
    true = len(set(distinct) & set(exact))
    caught, delays = match_planted(planted, list(distinct.values()), watermark_s)
    caught_exact, _ = match_planted(planted, list(exact.values()), watermark_s)
    return {
        "alert_rows": len(stream_rows), "alerts_distinct": len(distinct),
        "duplicate_alert_rows": len(stream_rows) - len(distinct),
        "exact_alerts": len(exact), "true_alerts": true,
        "recall_exact": round(true / len(exact), 4) if exact else None,
        "precision_exact": round(true / len(distinct), 4) if distinct else None,
        "planted_judged": len(planted), "planted_caught": caught,
        "recall_planted": round(caught / len(planted), 4) if planted else None,
        "planted_caught_exact": caught_exact,
        "alert_delay_s_median": round(statistics.median(delays), 1) if delays else None,
        "alert_delay_s_p90": round(percentile(delays, 90), 1) if delays else None,
        "alert_delay_s_max": round(max(delays), 1) if delays else None,
    }


def percentile(values, q):
    s = sorted(values)
    if not s:
        return None
    k = (len(s) - 1) * q / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def progress_stats(records):
    """Per-batch progress lines from detect_bunching.py --progress-log, after a crash and restart.

    A replayed batch appears twice; the later line (the run that committed it) counts."""
    by_batch = {}
    for r in records:
        by_batch[r["batch_id"]] = r
    data = [r for r in by_batch.values() if r["input_rows"]]
    ms = [r["trigger_ms"] for r in data if r["trigger_ms"] is not None]
    return {
        "batches": len(data),
        "batch_ms_mean": round(statistics.mean(ms)) if ms else None,
        "batch_ms_p95": round(percentile(ms, 95)) if ms else None,
        "state_rows_max": max((r["state_rows"] or 0 for r in by_batch.values()), default=0),
        "state_mb_max": round(max((r["state_bytes"] or 0 for r in by_batch.values()), default=0) / 1e6, 2),
        "late_window_rows_dropped": sum(r["dropped_by_watermark"] or 0 for r in by_batch.values()),
    }


def window_rows(events, window_s, lookback_s=BUNCHING["lookback_s"]):
    """(report, window) rows Spark's sliding window makes from the workload: the unit of
    numRowsDroppedByWatermark. A late report is dropped only from the windows already closed."""
    span_ms, slide_ms = (lookback_s + window_s) * 1000, window_s * 1000
    return sum(len(spark_windows(e["event_ts"], span_ms, slide_ms)) for e in events if e["source"] != "flush")


def offsets_total(end_offset):
    """Records consumed so far, from a progress line's Kafka endOffset {"topic": {"0": n, ...}}."""
    if not end_offset:
        return 0
    if isinstance(end_offset, str):
        end_offset = json.loads(end_offset)
    return sum(sum(parts.values()) for parts in end_offset.values())


# ---------------------------------------------------------------- reading the detector's output

def read_alert_rows(output):
    """Every row the detector wrote (all files on disk, what a pandas reader sees)."""
    import pandas as pd
    if not os.path.isdir(output):
        return [], (0, 0)
    df = pd.read_parquet(output)
    if df.empty:
        return [], (0, 0)
    for c in ("window_start", "window_end"):
        ts = pd.to_datetime(df[c], utc=True)
        df[c.replace("window_", "") + "_ms"] = (ts - pd.Timestamp(0, tz="UTC")) // pd.Timedelta(milliseconds=1)
    close_rows = len(df)
    close_dupes = close_rows - len(df.drop_duplicates(
        subset=["source", "route_id", "direction_id", "start_ms", "vehicle_a", "vehicle_b"]))
    rows = df[df["is_bunching"]][["source", "route_id", "direction_id", "start_ms", "end_ms",
                                  "vehicle_a", "vehicle_b"]].to_dict("records")
    return rows, (close_rows, close_dupes)


# ---------------------------------------------------------------- Kafka and Spark

def produce(bootstrap, topic, events):
    """Publish the workload in arrival order with the idempotent producer (no producer duplicates,
    so every duplicate alert comes from the sink)."""
    from confluent_kafka import Producer
    p = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 20, "log_level": 0})
    for e in events:
        while True:
            try:
                p.produce(topic, key=e["vehicle_id"], value=json.dumps(e))
                break
            except BufferError:
                p.poll(0.5)
        p.poll(0)
    left = p.flush(120)
    if left:
        raise RuntimeError(f"{left} records not delivered to {topic}")


class Detector:
    """streaming/detect_bunching.py as a child process in its own process group."""

    def __init__(self, args, topic, window_s, sink_mode, rundir):
        self.rundir = rundir
        self.progress = os.path.join(rundir, "progress.jsonl")
        self.output = os.path.join(rundir, "alerts")
        self.base = [sys.executable, os.path.join(ROOT, "streaming", "detect_bunching.py"),
                     "--source", "kafka", "--bootstrap", args.bootstrap, "--topic", topic,
                     "--output", self.output, "--checkpoint", os.path.join(rundir, "checkpoint"),
                     "--trigger", "1", "--window", str(window_s), "--watermark", str(args.watermark),
                     "--max-offsets", str(args.max_offsets), "--sink-mode", sink_mode,
                     "--progress-log", self.progress, "--quiet"]
        self.proc, self.starts = None, 0

    def start(self, crash_at=None):
        self.starts += 1
        cmd = self.base + (["--crash-at-batch", str(crash_at)] if crash_at is not None else [])
        log = open(os.path.join(self.rundir, f"spark_{self.starts}.log"), "w")
        # Python UDF workers must use this interpreter too, not whatever "python3" is on PATH.
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYSPARK_PYTHON=sys.executable,
                   PYSPARK_DRIVER_PYTHON=sys.executable)
        self.proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def records(self):
        if not os.path.exists(self.progress):
            return []
        out = []
        with open(self.progress) as f:
            for line in f:
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass                     # a line cut short by the crash
        return out

    def stop(self, timeout=90):
        if not self.alive():
            return
        os.killpg(self.proc.pid, signal.SIGINT)
        try:
            self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.proc.pid, signal.SIGKILL)
            self.proc.wait()


def wait_done(det, total, idle_s, timeout):
    """Wait until Spark has read every record and then gone idle (the flush batch has emitted the
    last windows and no more batches are coming)."""
    end = time.time() + timeout
    n_seen, last_change = -1, time.time()
    while time.time() < end:
        if not det.alive():
            raise RuntimeError(f"detector exited early; see {det.rundir}")
        recs = det.records()
        if len(recs) != n_seen:
            n_seen, last_change = len(recs), time.time()
        consumed = max((offsets_total(r["end_offset"]) for r in recs), default=0)
        if consumed >= total and time.time() - last_change >= idle_s:
            return
        time.sleep(1)
    raise RuntimeError(f"detector did not finish in {timeout}s; see {det.rundir}")


def run_one(args, events, planted, window_s, delivery, run_id, workdir, exact):
    sink_mode = DELIVERY_SINK[delivery]
    name = f"w{window_s}_{sink_mode}"
    rundir = os.path.join(workdir, f"{run_id}_{name}")
    shutil.rmtree(rundir, ignore_errors=True)
    os.makedirs(rundir)
    topic = f"b25-{run_id}-{name}"
    t0 = time.time()
    det = None
    create_topic(args.bootstrap, topic, args.partitions)
    try:
        produce(args.bootstrap, topic, events)
        det = Detector(args, topic, window_s, sink_mode, rundir)
        expected_batches = len(events) // args.max_offsets
        crash_at = int(expected_batches * args.crash_at) if args.crash_at > 0 else None
        det.start(crash_at)

        crashed, crash_batch, restart_s = False, None, None
        if crash_at is not None:
            # Wait for the injected crash (the process kills itself) or for the run to finish.
            end = time.time() + args.timeout
            while det.alive() and time.time() < end:
                recs = det.records()
                if max((offsets_total(r["end_offset"]) for r in recs), default=0) >= len(events) \
                        and recs and recs[-1]["batch_id"] > crash_at + 20:
                    break                        # no alert batch after crash_at: no crash this run
                time.sleep(0.5)
            if not det.alive():
                crashed = det.proc.returncode == -signal.SIGKILL
                t_crash = time.time()
                with open(os.path.join(rundir, "spark_1.log")) as f:
                    for line in f:
                        if line.startswith("CRASH injected after writing micro-batch"):
                            crash_batch = int(line.split()[-1])
                if not crashed:
                    raise RuntimeError(f"detector exited with {det.proc.returncode}; see {rundir}")
                n_before = len(det.records())
                det.start()                                # same checkpoint, no crash flag
                while time.time() < t_crash + args.timeout and len(det.records()) <= n_before:
                    if not det.alive():
                        raise RuntimeError(f"restarted detector exited; see {rundir}")
                    time.sleep(0.2)
                restart_s = round(time.time() - t_crash, 1)

        wait_done(det, len(events), args.idle, args.timeout)
        det.stop()
        rows, (close_rows, close_dupes) = read_alert_rows(det.output)
        stats = progress_stats(det.records())
        n_window_rows = window_rows(events, window_s)
        row = {"run_id": f"{run_id}_{name}", "window_s": window_s, "delivery": delivery, "sink_mode": sink_mode,
               "watermark_s": args.watermark, "lookback_s": BUNCHING["lookback_s"], "records": len(events),
               **stats, "window_rows": n_window_rows,
               "late_drop_pct": round(100 * stats["late_window_rows_dropped"] / n_window_rows, 2),
               "crashed": crashed, "crash_batch": crash_batch, "restart_to_first_batch_s": restart_s,
               "close_pair_rows": close_rows, "duplicate_close_pair_rows": close_dupes,
               **score(rows, exact, planted, args.watermark),
               "wall_s": round(time.time() - t0)}
        return row
    finally:
        if det is not None and det.alive():            # a failed run must not leave Spark running
            os.killpg(det.proc.pid, signal.SIGKILL)
            det.proc.wait()
        delete_topic(args.bootstrap, topic)
        if not args.keep_output:
            shutil.rmtree(rundir, ignore_errors=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--windows", default="60,300,600", help="detector window sizes in seconds")
    p.add_argument("--deliveries", default="at-least-once,exactly-once")
    p.add_argument("--watermark", type=int, default=BUNCHING["watermark_s"])
    p.add_argument("--vehicles", type=int, default=40)
    p.add_argument("--interval", type=int, default=10, help="seconds between reports of one bus")
    p.add_argument("--hours", type=float, default=2.0, help="simulated hours of feed")
    p.add_argument("--late", type=float, default=0.1, help="fraction of reports delivered 1-10 min late")
    p.add_argument("--bunch-every", type=int, default=10, help="plant a bunching event every N report rounds")
    p.add_argument("--bunch-length", type=int, default=6, help="report rounds each planted event lasts")
    p.add_argument("--seed", type=int, default=25)
    p.add_argument("--max-offsets", type=int, default=160,
                   help="Kafka records per micro-batch (160 = 40 s of a 40-bus feed)")
    p.add_argument("--crash-at", type=float, default=0.5,
                   help="crash after the first alert batch past this fraction of the run (0 = no crash)")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--idle", type=float, default=8, help="seconds without a new batch that mean 'done'")
    p.add_argument("--timeout", type=float, default=1200, help="seconds per run before giving up")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--workdir", default=os.path.join(ROOT, "data", "design_comparison"))
    p.add_argument("--results", default=os.path.join(ROOT, "benchmarks", "results"))
    p.add_argument("--keep-output", action="store_true", help="keep each run's alerts, checkpoint and logs")
    args = p.parse_args()

    windows = [int(w) for w in args.windows.split(",") if w.strip()]
    deliveries = [d.strip() for d in args.deliveries.split(",") if d.strip()]
    for d in deliveries:
        if d not in DELIVERY_SINK:
            p.error(f"unknown delivery {d!r}; choose from {list(DELIVERY_SINK)}")
    wait_kafka(args.bootstrap, timeout=60)

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(args.results, exist_ok=True)
    os.makedirs(args.workdir, exist_ok=True)

    t = time.time()
    events, planted = make_workload(args.vehicles, args.interval, args.hours, args.late, args.bunch_every,
                                    args.bunch_length, seed=args.seed,
                                    planted_log=os.path.join(args.workdir, f"{run_id}_planted.jsonl"))
    late = sum(1 for e in events if e["ingest_ts"] - e["event_ts"] > 60_000 and e["source"] != "flush")
    workload = {"stamp": stamp, "seed": args.seed, "vehicles": args.vehicles, "interval_s": args.interval,
                "hours": args.hours, "late_fraction": args.late, "records": len(events), "late_records": late,
                "planted_events": len(planted), "bunch_every": args.bunch_every,
                "bunch_length": args.bunch_length, "watermark_s": args.watermark,
                "max_offsets_per_batch": args.max_offsets, "partitions": args.partitions,
                "crash_at_fraction": args.crash_at, "detector_settings": BUNCHING}
    print(f"Workload: {len(events):,} reports ({late:,} late), {len(planted)} planted bunching events, "
          f"made in {time.time() - t:.1f} s", flush=True)
    exact_by_window = {}
    for w in windows:
        t = time.time()
        exact_by_window[w] = exact_alerts(events, w)
        print(f"Exact answer for {w} s windows: {len(exact_by_window[w])} bunching alerts "
              f"({time.time() - t:.1f} s)", flush=True)
    workload["exact_alerts"] = {str(w): len(a) for w, a in exact_by_window.items()}
    with open(os.path.join(args.results, f"b25_workload_{stamp}.json"), "w") as f:
        json.dump(workload, f, indent=2)

    runs_path = os.path.join(args.results, f"b25_runs_{stamp}.csv")
    with open(runs_path, "w", newline="") as f:
        csv.DictWriter(f, RUN_COLUMNS).writeheader()
    plan = [(w, d) for w in windows for d in deliveries]
    print(f"{len(plan)} runs. Results: {runs_path}", flush=True)
    for w, d in plan:
        print(f"-- window {w} s, {d}", flush=True)
        try:
            row = run_one(args, events, planted, w, d, run_id, args.workdir, exact_by_window[w])
        except Exception as e:
            print(f"   run failed: {e!r}", flush=True)
            continue
        with open(runs_path, "a", newline="") as f:
            csv.DictWriter(f, RUN_COLUMNS).writerow(row)
        print(f"   recall {row['recall_exact']} (exact) / {row['recall_planted']} (planted) | "
              f"duplicate alerts {row['duplicate_alert_rows']} | median alert delay {row['alert_delay_s_median']} s | "
              f"late window rows dropped {row['late_drop_pct']}% | crash batch {row['crash_batch']}, "
              f"restart {row['restart_to_first_batch_s']} s | {row['wall_s']} s", flush=True)
    print(f"Done. {runs_path}")


if __name__ == "__main__":
    main()

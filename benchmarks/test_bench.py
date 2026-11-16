"""Tests for the B22 benchmark helpers (no Kafka or Spark needed): python -m pytest benchmarks"""

import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmarks.bench_pipeline import (hist_mean, percentile_from_hist,  # noqa: E402
                                       shift_hist, summarize_run)
from benchmarks.load_generator import Fleet, events_due  # noqa: E402
from ingestion.schema import VEHICLE_EVENT_FIELDS, validate  # noqa: E402


def test_percentile_matches_nearest_rank():
    values = list(range(1, 101))                 # 1..100
    hist = Counter(values)
    assert percentile_from_hist(hist, 50) == 50
    assert percentile_from_hist(hist, 95) == 95
    assert percentile_from_hist(hist, 99) == 99
    assert percentile_from_hist(hist, 100) == 100
    assert percentile_from_hist(Counter(), 50) is None


def test_percentile_uses_counts():
    hist = Counter({10: 90, 1000: 10})            # 90% fast, 10% slow
    assert percentile_from_hist(hist, 50) == 10
    assert percentile_from_hist(hist, 90) == 10
    assert percentile_from_hist(hist, 91) == 1000
    assert hist_mean(hist) == 109


def test_shift_hist_gives_queue_latency():
    assert shift_hist(Counter({500: 2, 800: 1}), 300) == Counter({200: 2, 500: 1})


def test_events_due_paces_the_rate():
    assert events_due(0, 1000) == 0
    assert events_due(0.5, 1000) == 500
    assert events_due(2.0, 250) == 500
    assert events_due(1.0, 0) == 0


def test_generated_events_follow_b4_schema_and_are_unique():
    fleet = Fleet(5)
    events = [fleet.next_event("BENCH", 1_700_000_000_000) for _ in range(20)]
    assert all(validate(e, VEHICLE_EVENT_FIELDS) == [] for e in events)
    assert len({e["event_id"] for e in events}) == 20    # same ms, still distinct ids


def test_summarize_run():
    config = {"run_id": "x", "started_at": "now", "rate_target": 100, "trigger_s": 1,
              "duration_s": 10, "partitions": 3, "sink": "noop", "delivery": "at-least-once"}
    producer = {"sent": 1000, "delivered": 1000, "producer_rate": 100.0, "first_send_ms": 0}
    batches = [{"batch_id": i, "rows": 100, "batch_ms": 200 + i, "commit_ms": (i + 1) * 1000,
                "e2e_ms_p50": 600, "processed_rps": 500.0} for i in range(10)]
    e2e = Counter({600: 900, 1500: 100})
    s = summarize_run(config, producer, batches, e2e, shift_hist(e2e, 200), False)
    assert s["events_received"] == 1000 and s["events_missing"] == 0
    assert s["pipeline_throughput"] == 100.0          # 1000 rows over 10 s
    assert s["e2e_ms_p50"] == 600 and s["e2e_ms_p99"] == 1500
    assert s["queue_ms_p50"] == 400
    assert s["keeps_up"] is True                      # every batch < 1 s trigger
    assert s["batches"] == 10 and s["batch_rows_mean"] == 100.0

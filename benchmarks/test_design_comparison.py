"""Tests for the B25 helpers (no Kafka or Spark needed): python -m pytest benchmarks"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmarks.design_comparison import (alert_key, exact_alerts, make_workload,  # noqa: E402
                                          match_planted, offsets_total, progress_stats, score,
                                          spark_windows, window_rows)
from streaming.detect_bunching import batch_path, progress_record  # noqa: E402
from streaming.fake_feed import offset  # noqa: E402

SETTINGS = {"max_distance_m": 200, "max_time_gap_s": 60, "lookback_s": 300, "min_move_m": 50,
            "min_history_s": 30, "tandem_m": 40}


def test_spark_windows_tumbling_and_sliding():
    assert spark_windows(61_000, 60_000, 60_000) == [(60_000, 120_000)]
    # 6-minute windows sliding by 1 minute: every report is in 6 of them
    ws = spark_windows(400_000, 360_000, 60_000)
    assert len(ws) == 6 and all(s <= 400_000 < e for s, e in ws)
    # span not a multiple of the slide (15 min every 10 min): 1 or 2 windows, ends at 900 s + k*600 s
    assert spark_windows(100_000, 900_000, 600_000) == [(-600_000, 300_000), (0, 900_000)]
    assert spark_windows(400_000, 900_000, 600_000) == [(0, 900_000)]
    # their "current" parts (last 10 min of each) tile the timeline: [300 s, 900 s), [900 s, 1500 s), ...
    assert [e - 600_000 for _, e in spark_windows(800_000, 900_000, 600_000)] == [300_000, 900_000]
    # a window's start is inclusive, its end exclusive (Spark: start <= ts < end)
    assert (60_000, 120_000) in spark_windows(60_000, 60_000, 60_000)
    assert (0, 60_000) not in spark_windows(60_000, 60_000, 60_000)


def _bus(vid, t0_ms, lat, lon, heading, speed, n=40, every_s=10):
    out = []
    for i in range(n):
        la, lo = offset(lat, lon, heading, speed * i * every_s)
        out.append({"source": "fake", "vehicle_id": vid, "route_id": "22", "direction_id": 0,
                    "event_ts": t0_ms + i * every_s * 1000, "lat": la, "lon": lo, "trip_id": None,
                    "current_status": "IN_TRANSIT_TO", "stop_id": None})
    return out


def test_exact_alerts_finds_two_moving_buses_together():
    t0 = 1_800_000_000_000 - 1_800_000_000_000 % 60_000
    a = _bus("A", t0, 37.33, -121.88, 270, 8)
    lat, lon = offset(37.33, -121.88, 270, -60)          # B 60 m behind A, same speed
    b = _bus("B", t0, lat, lon, 270, 8)
    far_lat, far_lon = offset(37.33, -121.88, 270, -3000)
    c = _bus("C", t0, far_lat, far_lon, 270, 8)
    alerts = exact_alerts(a + b + c, 60, SETTINGS)
    assert alerts and all({x["vehicle_a"], x["vehicle_b"]} == {"A", "B"} for x in alerts.values())
    # A and B are together for the whole 400 s: one alert per minute they share
    assert len(alerts) == 7
    # with 10-minute windows the same pair gives fewer, longer alerts
    assert 0 < len(exact_alerts(a + b + c, 600, SETTINGS)) < len(alerts)


def test_alert_key_ignores_pair_order():
    assert alert_key("SC", 22, 0, 5, "b", "a") == alert_key("SC", "22", 0.0, 5, "a", "b")


def test_match_planted_and_delay():
    planted = [{"route_id": "22", "direction_id": 0, "vehicle_a": "A", "vehicle_b": "B",
                "start_ts": 100_000, "end_ts": 160_000},
               {"route_id": "22", "direction_id": 0, "vehicle_a": "C", "vehicle_b": "D",
                "start_ts": 100_000, "end_ts": 160_000}]
    alerts = [{"route_id": "22", "direction_id": 0, "vehicle_a": "B", "vehicle_b": "A",
               "start_ms": 120_000, "end_ms": 180_000},
              {"route_id": "22", "direction_id": 0, "vehicle_a": "A", "vehicle_b": "B",
               "start_ms": 60_000, "end_ms": 120_000},
              {"route_id": "23", "direction_id": 0, "vehicle_a": "C", "vehicle_b": "D",   # wrong route
               "start_ms": 60_000, "end_ms": 120_000}]
    caught, delays = match_planted(planted, alerts, watermark_s=30)
    assert caught == 1
    assert delays == [(120_000 + 30_000 - 100_000) / 1000]          # earliest matching window


def test_score_counts_duplicates_recall_precision():
    row = {"source": "fake", "route_id": "22", "direction_id": 0, "start_ms": 0, "end_ms": 60_000,
           "vehicle_a": "A", "vehicle_b": "B"}
    other = dict(row, start_ms=60_000, end_ms=120_000)
    wrong = dict(row, vehicle_b="Z")
    exact = {alert_key(r["source"], r["route_id"], r["direction_id"], r["start_ms"],
                       r["vehicle_a"], r["vehicle_b"]): r for r in (row, other)}
    s = score([row, dict(row), wrong], exact, [], watermark_s=30)     # row written twice (a replay)
    assert s["alert_rows"] == 3 and s["alerts_distinct"] == 2 and s["duplicate_alert_rows"] == 1
    assert s["true_alerts"] == 1 and s["recall_exact"] == 0.5 and s["precision_exact"] == 0.5
    assert s["recall_planted"] is None


def test_progress_stats_keeps_last_line_per_batch():
    recs = [{"batch_id": 1, "input_rows": 10, "trigger_ms": 500, "state_rows": 5, "state_bytes": 1e6,
             "dropped_by_watermark": 2},
            {"batch_id": 2, "input_rows": 10, "trigger_ms": 700, "state_rows": 8, "state_bytes": 2e6,
             "dropped_by_watermark": 3},
            # restart: batch 2 runs again; only this line counts
            {"batch_id": 2, "input_rows": 10, "trigger_ms": 900, "state_rows": 8, "state_bytes": 2e6,
             "dropped_by_watermark": 3},
            {"batch_id": 3, "input_rows": 0, "trigger_ms": 50, "state_rows": 0, "state_bytes": 1e5,
             "dropped_by_watermark": 0}]
    s = progress_stats(recs)
    assert s["batches"] == 2 and s["batch_ms_mean"] == 700
    assert s["late_window_rows_dropped"] == 5 and s["state_rows_max"] == 8 and s["state_mb_max"] == 2.0


def test_offsets_total():
    assert offsets_total({"t": {"0": 5, "1": 7}}) == 12
    assert offsets_total('{"t": {"0": 5}}') == 5
    assert offsets_total(None) == 0


def test_workload_is_reproducible_and_valid():
    ev1, pl1 = make_workload(vehicles=8, hours=0.25, seed=3)
    ev2, pl2 = make_workload(vehicles=8, hours=0.25, seed=3)
    assert [e["event_id"] for e in ev1] == [e["event_id"] for e in ev2] and pl1 == pl2
    assert ev1[-1]["source"] == "flush" and ev1[-1]["event_ts"] > max(e["event_ts"] for e in ev1[:-1])
    assert pl1 and all(p["type"] == "planted_bunching" for p in pl1)
    assert any(e["ingest_ts"] - e["event_ts"] > 60_000 for e in ev1[:-1])       # some late reports
    assert window_rows(ev1, 60) > window_rows(ev1, 600)


def test_detector_helpers():
    assert batch_path("out", 7) == os.path.join("out", "batch_id=7")
    rec = progress_record({"batchId": 4, "timestamp": "t", "numInputRows": 3,
                           "durationMs": {"triggerExecution": 120}, "eventTime": {"watermark": "w"},
                           "stateOperators": [{"numRowsTotal": 9, "memoryUsedBytes": 100,
                                               "numRowsDroppedByWatermark": 2}],
                           "sources": [{"endOffset": {"t": {"0": 3}}}]})
    assert rec["batch_id"] == 4 and rec["dropped_by_watermark"] == 2 and rec["state_rows"] == 9
    assert progress_record({"batchId": 0, "timestamp": "t"})["state_rows"] is None

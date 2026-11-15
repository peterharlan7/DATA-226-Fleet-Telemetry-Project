"""Tests for the B24 helpers (no Kafka or Spark needed): python -m pytest benchmarks"""
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from benchmarks.fault_injection import (account, committed_files, parquet_files_on_disk,  # noqa: E402
                                        percentile, recovery_threshold_ms, recovery_times,
                                        summarize, timeline)


def _log(path, entries):
    with open(path, "w") as f:
        f.write("v1\n")
        for e in entries:
            f.write(json.dumps(e) + "\n")


def test_committed_files_union_of_batches_and_compact(tmp_path):
    md = tmp_path / "_spark_metadata"
    md.mkdir()
    a, b, c = (str(tmp_path / f"part-{i}.parquet") for i in "abc")
    _log(md / "9.compact", [{"path": "file://" + a, "action": "add"}])
    _log(md / "10", [{"path": "file://" + b, "action": "add"}])
    _log(md / "11", [{"path": "file://" + a, "action": "delete"}, {"path": "file:" + c, "action": "add"}])
    (md / ".12.tmp").write_text("garbage")
    assert committed_files(str(md)) == {b, c}
    assert committed_files(str(tmp_path / "missing")) == set()


def test_files_on_disk_skips_metadata_and_hidden(tmp_path):
    part = tmp_path / "event_date=2026-11-14" / "source=B24"
    part.mkdir(parents=True)
    (part / "part-0.parquet").write_text("x")
    (part / ".part-0.parquet.crc").write_text("x")
    (tmp_path / "_spark_metadata").mkdir()
    (tmp_path / "_spark_metadata" / "x.parquet").write_text("x")
    assert parquet_files_on_disk(str(tmp_path)) == {str(part / "part-0.parquet")}


def test_account_loss_and_duplicates():
    sent = {"a", "b", "c", "d"}
    acked = {"a", "b", "c"}
    failed = {"d"}
    kafka = Counter({"a": 1, "b": 2, "d": 1})          # c lost by the broker, b stored twice
    parquet = Counter({"a": 1, "b": 2, "d": 1})
    r = account(sent, acked, failed, kafka, parquet)
    assert r["kafka_duplicates"] == 1 and r["acked_missing_in_kafka"] == 1
    assert r["parquet_rows"] == 4 and r["parquet_distinct"] == 3 and r["parquet_duplicates"] == 1
    assert r["lost_end_to_end"] == 1 and r["lost_acked"] == 1
    assert r["delivered_despite_error"] == 1


def test_recovery_times():
    kill, restart = 10_000, 20_000
    rows = [(9_000, 10_000),                        # before the kill, lag 1 s
            (12_000, 25_000), (19_000, 25_000),     # backlog batch: lag up to 13 s
            (26_000, 27_000)]                       # normal again: lag 1 s
    resume, recovery, max_lag = recovery_times(rows, kill, restart, threshold_ms=3000)
    assert (resume, recovery, max_lag) == (15.0, 17.0, 13.0)
    assert recovery_times([], kill, restart, 3000) == (None, None, None)


def test_threshold_and_percentile():
    assert percentile([5, 1, 3, 2, 4], 50) == 3
    assert percentile([], 95) is None
    assert recovery_threshold_ms([1000] * 20, trigger_s=2) == 3000      # p95 + one trigger
    assert recovery_threshold_ms([4000] * 20, trigger_s=2) == 8000      # twice p95


def test_timeline_phases():
    rows = [(9_000, 9_500), (25_000, 25_500)]
    out = timeline(rows, [9_000, 25_000], t_kill=10_000, t_restart=20_000, t_recovered=25_000)
    by_sec = {r["second"]: r for r in out}
    assert by_sec[-1]["phase"] == "before" and by_sec[-1]["committed"] == 1
    assert by_sec[15]["phase"] == "after" and by_sec[15]["lag_ms_max"] == 500


def test_summarize():
    trials = [{"fault": "spark", "delivery": "at-least-once", "resume_s": 10, "recovery_s": 12, "lost_acked": 0},
              {"fault": "spark", "delivery": "at-least-once", "resume_s": 20, "recovery_s": None, "lost_acked": 2}]
    (s,) = summarize(trials)
    assert s["trials"] == 2 and s["resume_s_mean"] == 15 and s["recovery_s_max"] == 12
    assert s["lost_acked_total"] == 2 and s["trials_unrecovered"] == 1

"""Tests for the B18 ground-truth scan and the streaming-vs-ground-truth comparison.

Pure Python / pandas, no Spark needed. Run from the repo root:
    python -m pytest streaming/test_ground_truth.py
"""

import pandas as pd

from streaming.compare_ground_truth import compare_bunching, compare_stalled, match_intervals
from streaming.ground_truth import knock_on_events, stall_events

T0 = 1_763_000_000_000
MIN = 60_000


# ---- delay propagation ---------------------------------------------------------------

def tu(trip, stop, minute, delay, route="R", src="BA"):
    return {"source": src, "trip_id": trip, "route_id": route, "stop_id": stop,
            "delay_sec": delay, "event_ts": T0 + minute * MIN}


def test_knock_on_needs_both_trains_late():
    ups = [tu("t1", "S", 0, 400), tu("t2", "S", 10, 350),     # late after late -> event
           tu("t3", "S", 20, 100),                            # on time after late -> no
           tu("t4", "S", 30, 600)]                            # late after on time -> no
    ev = knock_on_events(ups)
    assert [(e["trip_a"], e["trip_b"]) for e in ev] == [("t1", "t2")]


def test_knock_on_uses_last_prediction_and_time_limit():
    ups = [tu("t1", "S", 0, 0), tu("t1", "S", 5, 400),        # last prediction (late) wins
           tu("t2", "S", 40, 500)]                            # 35 min later: too far apart
    assert knock_on_events(ups) == []
    assert len(knock_on_events(ups, next_within_min=60)) == 1


def test_knock_on_separates_routes_and_skips_bad_rows():
    ups = [tu("t1", "S", 0, 400, route="A"), tu("t2", "S", 5, 400, route="B"),
           {"source": "BA", "trip_id": "x", "stop_id": "S", "delay_sec": None, "event_ts": T0}]
    assert knock_on_events(ups) == []


# ---- stall events --------------------------------------------------------------------

def stall_row(v, window_min, since_min, last_min, lat=37.3, lon=-121.9):
    return {"source": "fake", "vehicle_id": v, "window_end_ms": T0 + window_min * MIN,
            "since_ts": T0 + since_min * MIN, "last_ts": T0 + last_min * MIN, "minutes": last_min - since_min,
            "lat": lat, "lon": lon, "route_id": "22", "trip_id": "t", "status": "IN_TRANSIT_TO", "stop_id": None}


def test_stall_windows_merge_into_one_event_with_full_duration():
    # Same stall seen by three consecutive windows, given out of order.
    rows = [stall_row("A", 13, 0, 12), stall_row("A", 11, 0, 10), stall_row("A", 12, 0, 11)]
    stalls = stall_events(rows, still_m=50)
    assert len(stalls) == 1
    assert stalls[0]["last_ts"] == T0 + 12 * MIN and stalls[0]["minutes"] == 12.0
    assert stalls[0]["detected_window_end_ms"] == T0 + 11 * MIN


def test_stall_somewhere_else_is_a_new_event():
    rows = [stall_row("A", 11, 0, 10), stall_row("A", 40, 30, 39, lat=37.31)]
    assert len(stall_events(rows, still_m=50)) == 2


# ---- comparison ----------------------------------------------------------------------

def bunch(minute, a, b, cat="moving", route="22"):
    start = pd.Timestamp(T0 + minute * MIN, unit="ms", tz="UTC")
    return {"source": "fake", "route_id": route, "direction_id": 0, "window_start": start,
            "window_end": start + pd.Timedelta(minutes=1), "vehicle_a": a, "vehicle_b": b,
            "category": cat, "is_bunching": cat in ("moving", "tandem", "same_stop")}


def test_bunching_precision_recall_and_reasons():
    truth = pd.DataFrame([bunch(0, "1", "2"), bunch(1, "1", "2"), bunch(2, "3", "4"),
                          bunch(3, "5", "6", cat="layover"), bunch(99, "7", "8")])
    stream = pd.DataFrame([bunch(0, "2", "1"),                  # same pair, other order -> TP
                           bunch(1, "1", "2", cat="no_history"),  # labelled differently -> FN
                           bunch(3, "5", "6")])                 # truth says layover -> FP
    cutoff = T0 + 50 * MIN                                      # minute 99 is still pending
    r = compare_bunching(truth, stream, cutoff, None, None, examples=5)
    assert (r["true_positives"], r["false_positives"], r["false_negatives"]) == (1, 1, 2)
    assert r["pending"] == 1
    assert r["precision"] == 0.5 and r["recall"] == round(1 / 3, 4)
    assert r["false_negative_reasons"] == {"stream labelled it no_history": 1,
                                           "stream never saw the pair close": 1}
    assert r["false_positive_reasons"] == {"ground truth labels it layover": 1}


def stall(v, since_min, last_min, detected_min=None):
    return {"source": "fake", "vehicle_id": v, "route_id": "22", "since_ts": T0 + since_min * MIN,
            "last_ts": T0 + last_min * MIN, "minutes": last_min - since_min,
            "detected_window_end_ms": T0 + (detected_min if detected_min is not None else last_min) * MIN}


def test_stalled_overlap_matching():
    truth = pd.DataFrame([stall("A", 0, 20), stall("B", 0, 15), stall("C", 100, 110, detected_min=111)])
    stream = pd.DataFrame([stall("A", 1, 11), stall("D", 0, 12)])
    r = compare_stalled(truth, stream, T0 + 60 * MIN, None, None, examples=5)
    assert (r["true_positives"], r["false_positives"], r["false_negatives"], r["pending"]) == (1, 1, 1, 1)
    assert [m["vehicle_id"] for m in r["missed_examples"]] == ["B"]


def test_match_intervals_needs_same_vehicle():
    left = pd.DataFrame([stall("A", 0, 10)])
    assert match_intervals(left, pd.DataFrame([stall("B", 0, 10)])) == [False]
    assert match_intervals(left, pd.DataFrame([stall("A", 10, 30)])) == [True]

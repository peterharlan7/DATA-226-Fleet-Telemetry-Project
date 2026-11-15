"""Tests for the refined bunching rules (B12) and the stalled-vehicle rule (B16).

Run from the repo root:  python -m pytest streaming/test_detectors.py
"""

from streaming.detect_bunching import bunching_pairs, close_pairs
from streaming.detect_stalled import StallTracker, find_stall

T0 = 1_763_000_000_000          # some time in ms
M = 0.0009                      # about 100 m of latitude


def rep(v, t_s, lat, lon=-121.9, trip=None, status="IN_TRANSIT_TO", stop=None, route="22"):
    return {"vehicle_id": v, "event_ts": T0 + t_s * 1000, "lat": lat, "lon": lon,
            "trip_id": trip or f"trip-{v}", "current_status": status, "stop_id": stop, "route_id": route}


def history(v, lat_now, moving, offset=0.0, step_s=60, n=6, **kw):
    """Reports every step_s seconds for n steps ending at t=330 s; moving = 100 m per step."""
    out = []
    for i in range(n):
        t = 330 - (n - 1 - i) * step_s
        back = (n - 1 - i) * M if moving else 0
        out.append(rep(v, t, lat_now - back + offset, **kw))
    return out


CUR = T0 + 300_000              # current minute starts at t=300 s


def cats(reports, **kw):
    return {(p[0], p[1]): p[4] for p in bunching_pairs(reports, CUR, 200, 60, min_move_m=50, **kw)}


def test_both_moving_is_bunching():
    r = history("A", 37.30, True) + history("B", 37.30, True, offset=-0.0011)  # ~120 m behind
    pairs = bunching_pairs(r, CUR, 200, 60, min_move_m=50)
    assert [(p[4], p[5]) for p in pairs] == [("moving", True)]


def test_parked_pair_is_layover():
    r = history("A", 37.30, False) + history("B", 37.3002, False)
    assert cats(r) == {("A", "B"): "layover"}


def test_bus_passing_parked_bus():
    r = history("A", 37.30, True) + history("B", 37.3003, False)
    assert cats(r) == {("A", "B"): "one_parked"}


def test_tandem_buses():
    r = history("A", 37.30, True) + history("B", 37.30, True, offset=0.0002)    # ~22 m, always
    assert cats(r) == {("A", "B"): "tandem"}


def test_same_stop():
    a = history("A", 37.30, True)
    b = history("B", 37.30, True, offset=0.0011)
    a[-1].update(current_status="STOPPED_AT", stop_id="S1")
    b[-1].update(current_status="STOPPED_AT", stop_id="S1")
    assert cats(a + b) == {("A", "B"): "same_stop"}


def test_no_history_yet():
    r = [rep("A", 310, 37.30), rep("B", 310, 37.3005)]
    assert cats(r) == {("A", "B"): "no_history"}


def test_raw_mode_matches_original_detector():
    r = history("A", 37.30, False) + history("B", 37.3002, False)
    current = [x for x in r if x["event_ts"] >= CUR]
    raw = bunching_pairs(r, CUR, 200, 60, min_move_m=0)
    assert [p[:4] for p in raw] == close_pairs(current, 200, 60)
    assert raw[0][5] is True


def test_only_current_minute_is_compared():
    # Close 4 minutes ago, far apart now -> no alert in this window.
    a = history("A", 37.30, True)
    b = history("B", 37.30, True, offset=0.01)
    b[1]["lat"] = a[1]["lat"]
    assert cats(a + b) == {}


# ---- stalled vehicles (B16) -------------------------------------------------

def stuck(v, minutes, status="IN_TRANSIT_TO", step_s=120, trip="t1"):
    n = int(minutes * 60 / step_s) + 1
    moving = [rep(v, i * 60, 37.20 + i * M, trip=trip) for i in range(3)]
    t_stop = 180
    return moving + [rep(v, t_stop + i * step_s, 37.20 + 3 * M + (i % 2) * 0.0001, trip=trip, status=status)
                     for i in range(n)]


def test_stall_found():
    s = find_stall(stuck("A", 12), 600, 50, 3)
    assert s is not None and s[2] >= 10


def test_short_stop_is_not_a_stall():
    assert find_stall(stuck("A", 6), 600, 50, 3) is None


def test_waiting_at_stop_is_not_a_stall():
    assert find_stall(stuck("A", 12, status="STOPPED_AT"), 600, 50, 3) is None


def test_out_of_service_is_not_a_stall():
    r = stuck("A", 12)
    for x in r:
        x["trip_id"] = None
    assert find_stall(r, 600, 50, 3) is None


def test_tracker_reports_a_long_stall_once():
    tr = StallTracker(50)
    base = {"source": "SF", "vehicle_id": "A", "route_id": "22", "trip_id": "t", "lat": 37.2,
            "lon": -121.9, "status": "IN_TRANSIT_TO", "stop_id": None}
    first = tr.update({**base, "since_ts": 0, "last_ts": 600_000, "minutes": 10})
    again = tr.update({**base, "since_ts": 60_000, "last_ts": 660_000, "minutes": 10})
    assert first is not None and again is None
    assert tr.stalls[0]["minutes"] == 11.0


def test_stuck_after_waiting_at_stop_counts_from_when_status_changed():
    r = stuck("A", 12)
    r[3]["current_status"] = "STOPPED_AT"          # first stationary report was at a stop
    s = find_stall(r, 600, 50, 3)
    assert s is not None and s[0] == r[4]["event_ts"]

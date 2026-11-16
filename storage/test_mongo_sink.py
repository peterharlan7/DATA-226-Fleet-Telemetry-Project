"""
Tests for storage/mongo_sink.py (B11).

The first group needs nothing. The second group uses a real MongoDB
(`docker compose up -d mongo`) in a throwaway database and is skipped when
MongoDB is not reachable.

    pytest storage/
"""

import os
import sys
import uuid
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from storage import mongo_sink as ms  # noqa: E402

T0 = 1_762_000_000_000                       # epoch ms
UTC = timezone.utc


def bunching_row(**kw):
    row = {"source": "SC", "route_id": "22", "direction_id": 0,
           "window_start": datetime(2025, 11, 1, 12, 0, tzinfo=UTC),
           "window_end": datetime(2025, 11, 1, 12, 1, tzinfo=UTC),
           "vehicle_a": "bbb", "vehicle_b": "aaa", "distance_m": 61.2, "time_gap_s": 4.0,
           "detected_at": datetime(2025, 11, 1, 12, 1, 40, tzinfo=UTC)}
    row.update(kw)
    return row


def position(vehicle="v1", ts=T0, lat=37.33, lon=-121.88, source="SC", **kw):
    row = {"event_id": f"{source}:{vehicle}:{ts}", "schema_version": 1, "source": source,
           "vehicle_id": vehicle, "route_id": "22", "trip_id": "t1", "direction_id": 0,
           "event_ts": ts, "ingest_ts": ts + 5000, "lat": lat, "lon": lon, "bearing": None,
           "speed_mps": None, "current_status": "IN_TRANSIT_TO", "stop_id": "s1"}
    row.update(kw)
    return row


# ---------------------------------------------------------------- pure helpers

def test_alert_doc_has_card_fields_and_sorted_vehicles():
    doc = ms.alert_doc(bunching_row(), "bunching")
    for f in ("type", "source", "route_id", "direction_id", "window_start", "window_end",
              "vehicles", "distance_m", "detected_at"):
        assert f in doc
    assert doc["type"] == "bunching"
    assert doc["vehicles"] == ["aaa", "bbb"]
    assert doc["details"] == {"time_gap_s": 4.0}
    assert "vehicle_a" not in doc


def test_alert_id_is_stable_and_ignores_detected_at_and_pair_order():
    a = ms.alert_doc(bunching_row(), "bunching")
    b = ms.alert_doc(bunching_row(vehicle_a="aaa", vehicle_b="bbb",
                                  detected_at=datetime(2025, 11, 2, tzinfo=UTC)), "bunching")
    assert a["_id"] == b["_id"]
    c = ms.alert_doc(bunching_row(route_id="23"), "bunching")
    assert c["_id"] != a["_id"]


def test_single_vehicle_alert_uses_vehicle_id_and_since_ts():
    stall = {"source": "SC", "vehicle_id": "v9", "route_id": "22", "since_ts": T0,
             "last_ts": T0 + 600_000, "minutes": 10.0}
    doc = ms.alert_doc(stall, "stalled")
    assert doc["vehicles"] == ["v9"]
    assert doc["details"]["minutes"] == 10.0
    later = ms.alert_doc(dict(stall, last_ts=T0 + 900_000), "stalled")
    assert later["_id"] == doc["_id"]


def test_to_utc():
    assert ms.to_utc(0) == datetime(1970, 1, 1, tzinfo=UTC)
    aware = datetime(2025, 1, 1, 8, tzinfo=UTC)
    assert ms.to_utc(aware) == aware
    naive = datetime(2025, 1, 1, 8)                       # Spark collect(): local time
    assert ms.to_utc(naive) == naive.astimezone(UTC)
    with pytest.raises(TypeError):
        ms.to_utc("2025-01-01")


def test_latest_per_vehicle_keeps_newest():
    rows = [position("v1", T0), position("v1", T0 + 30_000), position("v1", T0 + 10_000),
            position("v2", T0), position("v1", T0, source="SF")]
    latest = {(r["source"], r["vehicle_id"]): r["event_ts"] for r in ms.latest_per_vehicle(rows)}
    assert latest == {("SC", "v1"): T0 + 30_000, ("SC", "v2"): T0, ("SF", "v1"): T0}


# ---------------------------------------------------------------- against a real MongoDB

@pytest.fixture
def db():
    pytest.importorskip("pymongo")
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError
    client = MongoClient(os.getenv("MONGO_URI", ms.DEFAULT_URI), serverSelectionTimeoutMS=1500,
                         tz_aware=True)
    try:
        client.admin.command("ping")
    except PyMongoError:
        pytest.skip("MongoDB not running (docker compose up -d mongo)")
    name = f"test_b11_{uuid.uuid4().hex[:8]}"
    yield client[name]
    client.drop_database(name)
    client.close()


def test_indexes_created(db):
    ms.ensure_indexes(db)
    alert_idx = db[ms.ALERTS].index_information()
    latest_idx = db[ms.LATEST].index_information()
    assert alert_idx["route_window"]["key"] == [("route_id", 1), ("window_start", 1)]
    assert latest_idx["one_per_vehicle"]["key"] == [("source", 1), ("vehicle_id", 1)]
    assert latest_idx["one_per_vehicle"]["unique"] is True


def test_write_alerts_inserts_once(db):
    rows = [bunching_row(), bunching_row(route_id="23")]
    assert ms.write_alerts(rows, "bunching", db=db) == 2
    assert ms.write_alerts(rows, "bunching", db=db) == 0          # replayed micro-batch
    assert db[ms.ALERTS].count_documents({}) == 2
    doc = db[ms.ALERTS].find_one({"route_id": "22"})
    assert doc["window_start"] == datetime(2025, 11, 1, 12, 0, tzinfo=UTC)
    assert doc["vehicles"] == ["aaa", "bbb"]


def test_latest_position_only_moves_forward(db):
    assert ms.upsert_latest_positions([position("v1", T0, lat=37.0)], db=db) == (1, 0)
    ms.upsert_latest_positions([position("v1", T0 + 30_000, lat=37.1)], db=db)
    ms.upsert_latest_positions([position("v1", T0 + 10_000, lat=99.0)], db=db)   # late report
    ms.upsert_latest_positions([position("v1", T0 + 30_000, lat=37.1)], db=db)   # replay

    docs = list(db[ms.LATEST].find({"source": "SC", "vehicle_id": "v1"}))
    assert len(docs) == 1
    d = docs[0]
    assert d["event_ts"] == T0 + 30_000 and d["lat"] == 37.1
    assert d["location"] == {"type": "Point", "coordinates": [-121.88, 37.1]}
    assert d["event_time"] == ms.to_utc(T0 + 30_000)
    assert "updated_at" in d


def test_latest_positions_one_doc_per_vehicle_and_geo_query(db):
    rows = [position(f"v{i}", T0 + i, lat=37.33 + i * 0.01) for i in range(5)]
    rows += [position("v0", T0 - 1, source="SF")]
    ms.upsert_latest_positions(rows, db=db)
    assert db[ms.LATEST].count_documents({}) == 6
    near = list(db[ms.LATEST].find({"location": {"$nearSphere": {
        "$geometry": {"type": "Point", "coordinates": [-121.88, 37.33]}, "$maxDistance": 1500}}}))
    # v0 (SC and SF) at the point, then v1 about 1.1 km north, nearest first; v2+ are too far
    assert len(near) == 3
    assert (near[2]["source"], near[2]["vehicle_id"]) == ("SC", "v1")

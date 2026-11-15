"""
MongoDB operational store (B11): each vehicle's latest position and the detector alerts.

MongoDB holds current state only. Raw events are NOT copied here: the Parquet
archive (data/raw/vehicle_events, B7) stays the history for scans and analysis.

Collections (database MONGO_DB, default "fleet"):
  alerts            one document per detector alert (bunching now, stalled etc. later)
  latest_positions  one document per vehicle, keyed by (source, vehicle_id)

Both writes are safe to repeat. Spark's foreachBatch can run a micro-batch again
after a crash, so:
  - each alert gets a deterministic _id, and inserting it twice is a no-op;
  - a position only replaces the stored one when its event_ts is newer, so late
    or replayed reports never move a vehicle back in time.

Used by the Spark jobs (pass --mongo):
    python streaming/stream_to_parquet.py --source kafka --mongo   # also keeps latest_positions
    python streaming/detect_bunching.py --source kafka --mongo     # also writes alerts

From the repo root, with `docker compose up -d mongo`:
    python storage/mongo_sink.py --init     # create the indexes
    python storage/mongo_sink.py            # counts, newest alerts, a few latest positions
"""

import argparse
import hashlib
import os
from datetime import datetime, timezone

DEFAULT_URI = "mongodb://localhost:27017"
DEFAULT_DB = "fleet"
ALERTS = "alerts"
LATEST = "latest_positions"

# Vehicle-event fields kept on a latest_positions document (ingestion/schema.py names).
POSITION_FIELDS = ("event_id", "schema_version", "source", "vehicle_id", "route_id", "trip_id",
                   "direction_id", "event_ts", "ingest_ts", "lat", "lon", "bearing", "speed_mps",
                   "current_status", "stop_id")

# Alert fields with a fixed meaning (B11 card). Anything else a detector sends goes under "details".
ALERT_FIELDS = ("source", "route_id", "direction_id", "window_start", "window_end", "distance_m",
                "detected_at")

_clients = {}


# ---------------------------------------------------------------- connection

def get_db(uri=None, db_name=None):
    """Return the pymongo Database. One client per URI per process (Spark runs foreachBatch on the driver)."""
    from pymongo import MongoClient
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    uri = uri or os.getenv("MONGO_URI", DEFAULT_URI)
    db_name = db_name or os.getenv("MONGO_DB", DEFAULT_DB)
    if uri not in _clients:
        _clients[uri] = MongoClient(uri, serverSelectionTimeoutMS=5000, tz_aware=True)
    return _clients[uri][db_name]


def ensure_indexes(db):
    """Create the indexes (idempotent). Called by the writers, so a fresh database just works."""
    from pymongo import ASCENDING, DESCENDING, GEOSPHERE
    db[ALERTS].create_index([("route_id", ASCENDING), ("window_start", ASCENDING)],
                            name="route_window")
    db[ALERTS].create_index([("detected_at", DESCENDING)], name="newest_first")
    db[LATEST].create_index([("source", ASCENDING), ("vehicle_id", ASCENDING)],
                            unique=True, name="one_per_vehicle")
    db[LATEST].create_index([("location", GEOSPHERE)], name="location_2dsphere")


_indexed = set()


def _ready(db):
    key = (id(db.client), db.name)
    if key not in _indexed:
        ensure_indexes(db)
        _indexed.add(key)
    return db


# ---------------------------------------------------------------- pure helpers (tested without Mongo)

def to_utc(value):
    """datetime or epoch ms -> timezone-aware UTC datetime (what MongoDB stores).

    Naive datetimes are read as LOCAL time, because that is what Spark's
    collect() returns for timestamp columns.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError("expected a datetime or epoch milliseconds, got bool")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, tz=timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    raise TypeError(f"expected a datetime or epoch milliseconds, got {type(value).__name__}")


def _vehicles(row):
    if row.get("vehicles"):
        return sorted(row["vehicles"])
    found = [row[k] for k in ("vehicle_a", "vehicle_b", "vehicle_id") if row.get(k)]
    return sorted(found)


def alert_doc(row, alert_type):
    """Turn one detector output row (dict) into an alerts document.

    Bunching rows have vehicle_a / vehicle_b; single-vehicle detectors (stalled)
    have vehicle_id. Both end up in `vehicles`, sorted. Extra columns are kept
    under `details`, so a new detector does not need a schema change here.
    """
    if not alert_type:
        raise ValueError("alert_type is required, e.g. 'bunching'")
    row = dict(row)
    vehicles = _vehicles(row)
    doc = {"type": alert_type, "vehicles": vehicles}
    for k in ALERT_FIELDS:
        v = row.get(k)
        doc[k] = to_utc(v) if k in ("window_start", "window_end", "detected_at") else v
    if doc["detected_at"] is None:
        doc["detected_at"] = datetime.now(timezone.utc)
    skip = set(ALERT_FIELDS) | {"vehicles", "vehicle_a", "vehicle_b", "vehicle_id", "type"}
    details = {k: v for k, v in row.items() if k not in skip}
    if details:
        doc["details"] = details

    # Same alert -> same _id, so a micro-batch replayed after a crash is not stored twice.
    # detected_at is processing time and differs on a replay, so it is not part of the key.
    start = doc["window_start"] or to_utc(details.get("since_ts"))
    key = "|".join(str(x) for x in (alert_type, doc["source"], doc["route_id"], doc["direction_id"],
                                    start.isoformat() if start else None, ",".join(vehicles)))
    doc["_id"] = hashlib.sha1(key.encode()).hexdigest()[:24]
    return doc


def latest_per_vehicle(rows):
    """Keep only the newest report per (source, vehicle_id) in one batch."""
    best = {}
    for r in rows:
        r = dict(r)
        if r.get("source") is None or r.get("vehicle_id") is None or r.get("event_ts") is None:
            continue
        key = (r["source"], r["vehicle_id"])
        if key not in best or r["event_ts"] > best[key]["event_ts"]:
            best[key] = r
    return list(best.values())


def position_update(row):
    """Filter + update pipeline for one latest_positions upsert.

    Every field is set with $cond: take the new value only if the new event_ts
    is newer than the stored one, else keep what is there. All expressions in one
    $set stage see the document as it was, so the comparison is consistent.
    Values are wrapped in $literal so a string starting with '$' is not read as a field path.
    """
    new_ts = row["event_ts"]
    newer = {"$gt": [new_ts, {"$ifNull": ["$event_ts", -1]}]}
    fields = {k: row.get(k) for k in POSITION_FIELDS}
    fields["event_time"] = to_utc(new_ts)
    fields["location"] = ({"type": "Point", "coordinates": [row["lon"], row["lat"]]}
                          if row.get("lat") is not None and row.get("lon") is not None else None)
    set_stage = {k: {"$cond": [newer, {"$literal": v}, f"${k}"]} for k, v in fields.items()}
    set_stage["updated_at"] = {"$cond": [newer, "$$NOW", "$updated_at"]}
    return {"source": row["source"], "vehicle_id": row["vehicle_id"]}, [{"$set": set_stage}]


# ---------------------------------------------------------------- writers (the two B11 functions)

def write_alerts(rows, alert_type="bunching", db=None):
    """Insert one document per alert. Returns how many were new (repeats are skipped)."""
    from pymongo.errors import BulkWriteError
    docs = [alert_doc(r, alert_type) for r in rows]
    if not docs:
        return 0
    db = _ready(db if db is not None else get_db())
    try:
        return len(db[ALERTS].insert_many(docs, ordered=False).inserted_ids)
    except BulkWriteError as e:
        others = [err for err in e.details.get("writeErrors", []) if err.get("code") != 11000]
        if others:
            raise
        return e.details.get("nInserted", 0)


def upsert_latest_positions(rows, db=None):
    """Upsert each vehicle's newest position. Returns (vehicles inserted, vehicles moved forward)."""
    from pymongo import UpdateOne
    from pymongo.errors import BulkWriteError
    rows = latest_per_vehicle(rows)
    if not rows:
        return 0, 0
    db = _ready(db if db is not None else get_db())
    ops = [UpdateOne(f, u, upsert=True) for f, u in map(position_update, rows)]
    try:
        res = db[LATEST].bulk_write(ops, ordered=False)
        return res.upserted_count, res.modified_count
    except BulkWriteError as e:
        # Two writers inserting the same new vehicle at once: one wins the unique
        # index, the other gets 11000. Retrying turns the loser into a normal update.
        failed = e.details.get("writeErrors", [])
        if any(err.get("code") != 11000 for err in failed):
            raise
        retry = [ops[err["index"]] for err in failed]
        res = db[LATEST].bulk_write(retry, ordered=False)
        return (e.details.get("nUpserted", 0) + res.upserted_count,
                e.details.get("nModified", 0) + res.modified_count)


# ---------------------------------------------------------------- command line

def show(db, n=5):
    print(f"MongoDB {db.client.address} database '{db.name}'")
    print(f"  alerts:           {db[ALERTS].count_documents({})}")
    print(f"  latest_positions: {db[LATEST].count_documents({})}")
    print(f"\nNewest {n} alerts:")
    for a in db[ALERTS].find().sort("detected_at", -1).limit(n):
        start = a["window_start"].strftime("%Y-%m-%d %H:%M:%S") if a.get("window_start") else "-"
        dist = f"{a['distance_m']:.0f} m" if a.get("distance_m") is not None else ""
        print(f"  {a['type']:9} {a.get('source')} route {a.get('route_id')} dir {a.get('direction_id')} "
              f"| window {start} UTC | {' & '.join(v[:8] for v in a['vehicles'])} {dist}")
    print(f"\n{n} most recently reported vehicles:")
    for p in db[LATEST].find().sort("event_ts", -1).limit(n):
        print(f"  {p['source']} {p['vehicle_id'][:8]} route {p.get('route_id')} "
              f"({p['lat']:.5f}, {p['lon']:.5f}) at {p['event_time']:%H:%M:%S} UTC")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--init", action="store_true", help="create the indexes and exit")
    p.add_argument("-n", type=int, default=5, help="how many alerts / vehicles to show")
    args = p.parse_args()
    db = get_db()
    if args.init:
        ensure_indexes(db)
        for c in (ALERTS, LATEST):
            print(f"{c}: {', '.join(sorted(db[c].index_information()))}")
        return
    show(db, args.n)


if __name__ == "__main__":
    main()

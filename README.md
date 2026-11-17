# Real-Time Fleet Telemetry Pipeline: From Bay Area Transit to Autonomous Vehicles

DATA 226 term project · Ashritha Achutharaman, Peter Harlan, Jason Letargo

A streaming pipeline that ingests live GTFS-realtime feeds (511, BART, VTA) through Apache Kafka, processes them with Spark Structured Streaming, detects rare operational events (bus bunching, delay propagation, stalled vehicles), and benchmarks how design choices trade storage and latency against detection recall, measured against exact offline ground truth.

## Status

| Sprint | Dates | Goal | Status |
| --- | --- | --- | --- |
| 1 | Oct 19 – Oct 25 | Setup + live GTFS-rt data in Kafka | In progress |
| 2 | Oct 26 – Nov 1 | Spark stream, storage layers, intermediate report | Not started |
| 3 | Nov 2 – Nov 8 | Detectors, Bloom filter, LSH | Not started |
| 4 | Nov 9 – Nov 15 | Benchmarks, privacy, streaming algorithms, slides | Not started |
| Wrap-up | Nov 16 – Nov 17 | Final report and presentation | Not started |

## Architecture

```
GTFS-realtime feeds (511 / BART / VTA)
        │  Python producers (ingestion/)
        ▼
   Apache Kafka  ──►  Spark Structured Streaming (streaming/)
                          │  windowed features, detectors, Bloom filter dedup
                          ├──►  Operational store: MongoDB (latest position per vehicle, alerts)
                          ├──►  Warehouse: ClickHouse or DuckDB (alerts, summaries)
                          └──►  Parquet archive (data/, not committed)
```

A full flow diagram will be added in `docs/`.

## Repository layout

| Folder | Contents |
| --- | --- |
| `ingestion/` | Kafka producers for GTFS-realtime feeds and replay |
| `streaming/` | Spark Structured Streaming jobs and detectors |
| `storage/` | MongoDB and warehouse schemas, sink code, sample queries |
| `algorithms/` | Bloom filter, LSH, reservoir sampling, Flajolet-Martin, privacy |
| `benchmarks/` | Benchmark harness, fault injection (`FAULT_INJECTION.md`), design comparison (`DESIGN_COMPARISON.md`), results CSVs |
| `notebooks/` | Exploration and analysis notebooks (each also exported to `.py`) |
| `docs/` | Diagrams, meeting minutes, evidence for the rubric appendix |
| `report/` | LaTeX source of the IEEE report (synced from Overleaf) |
| `data/` | Local data only; ignored by Git |

## Common event schema (read this before writing a producer or Spark job) *********

        Every record in the pipeline follows one schema, defined once in
        [`ingestion/schema.py`](ingestion/schema.py) (card B4). Producers, Spark jobs,
        detectors and privacy code all import it. Do not redefine fields elsewhere.

**Two record types**
        
        | Record | Kafka topic | Used for |
        | --- | --- | --- |
        | Vehicle event (`VEHICLE_EVENT_FIELDS`) | `vehicle-positions` | Archive, bus bunching, stalled vehicles |
        | Trip-update event (`TRIP_UPDATE_EVENT_FIELDS`) | `trip-updates` | Delay propagation |
        
**Rules**
        
        - **Timestamps are epoch milliseconds, UTC.** GTFS-realtime sends seconds, so multiply by 1000.
        - Two timestamps: `event_ts` (when the vehicle reported, used for windows) and `ingest_ts` (when our producer received it).
        - Units: meters, meters per second (`speed_mps`), degrees.
        - `source` uses 511 operator codes (`SC` = VTA, `SF` = Muni, `BA` = BART); test data uses `fake`.
        - Build `event_id` with `make_vehicle_event_id()` / `make_trip_update_event_id()`.
        - Call `validate()` before publishing a record.
        
**How to use it**
        
        ```python
        from ingestion.schema import VEHICLE_EVENT_FIELDS, validate, spark_schema
        
        problems = validate(record, VEHICLE_EVENT_FIELDS)    # [] means valid
        schema = spark_schema(VEHICLE_EVENT_FIELDS)          # Spark StructType for from_json
        ```
        
        Run `python ingestion/schema.py` to print an example record and the Spark schema.

## Setup

1. Install Docker Desktop and Python 3.10+.
2. Clone the repo and create a virtual environment:
   ```
   git clone https://github.com/<your-account>/fleet-telemetry-pipeline.git
   cd fleet-telemetry-pipeline
   python -m venv .venv
   source .venv/bin/activate      # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Copy `.env.example` to `.env` and add your 511.org API key.
4. Start Kafka and Spark: `docker compose up -d` (docker-compose.yml added in backlog item B2).

## Project links
- https://github.com/peterharlan7/DATA-226-Fleet-Telemetry-Project/tree/main
- Trello board (sprints and backlog): <add link>
- Overleaf report: <add link>
- Elevator pitch video: <add link>

## Team

| Member | Role |
| --- | --- |
| Ashritha Achutharaman | Ingestion and Kafka lead; Product Owner |
| Peter Harlan | Spark and algorithms lead; Scrum Master (odd sprints) |
| Jason Letargo | Storage, benchmarking and privacy lead; Scrum Master (even sprints) |

## Running the producers

All events follow the common schema in `ingestion/schema.py` (timestamps are epoch milliseconds).
Run these from the repo root with Kafka up (`docker compose up -d`) and a key in `.env`:

| Script | What it is | Run it |
| --- | --- | --- |
| `ingestion/producer_all.py` | **Main producer.** Live vehicle positions from 511, one request per agency, so `source` is the agency code (SF, SC, AC, ...) | `python ingestion/producer_all.py --agencies SF,SC,AC` (use `--list` to see all 511 codes) |
| `ingestion/producer_bart.py` | BART trip delays (BART has no vehicle-position feed) | `python ingestion/producer_bart.py` |
| `ingestion/producer_vta.py` | One-feed VTA producer used for the Trello card demo | `python ingestion/producer_vta.py --once` |
| `ingestion/producer_511.py` | Shared functions only (not a program) | imported by the scripts above |
| `ingestion/av_replay.py`, `record_stream.py` | Replay and recording tools for the benchmarks; replays go to `replay-*` topics and are tagged with a Kafka header | see the docstring in each file |
| `ingestion/simulator_producer.py` | Simulated data (source `SIM`), testing only | `python ingestion/simulator_producer.py` |

Vehicle ids are hashed at ingestion (B13, `ingestion/hashing.py`) and `event_id` is built from the hashed id, so raw ids never reach Kafka.
The producers need `VEHICLE_ID_SALT` in `.env` (ask Jason; it must be the same for everyone) and stop at start-up if it is missing.

**Live dashboard: [http://localhost:8000](http://localhost:8000)** (real Kafka data, updates every 2 seconds). Start it with `./demo.sh`, or run `python dashboard/viz_web.py` after the producers are running. A screenshot of a real run is in `docs/dashboard.png`.

Docker Compose also starts MongoDB (`mongo`, port 27017, data kept in the `mongo-data` volume). Its address is `MONGO_URI` in `.env`.

## MongoDB: latest positions and alerts (B11)

MongoDB is the **operational store**: what is true right now. Raw events are **not** copied into it; the Parquet archive (`data/raw/vehicle_events`) stays the history for scans and analysis. All the code is in [`storage/mongo_sink.py`](storage/mongo_sink.py).

| Collection | One document per | Written by | Indexes |
| --- | --- | --- | --- |
| `latest_positions` | vehicle, keyed by (`source`, `vehicle_id`) | `stream_to_parquet.py --mongo` | unique (`source`, `vehicle_id`); 2dsphere on `location` |
| `alerts` | detector alert: `type`, `source`, `route_id`, `direction_id`, `window_start`, `window_end`, `vehicles`, `distance_m`, `detected_at` | `detect_bunching.py --mongo` | (`route_id`, `window_start`); `detected_at` |

- A position replaces the stored one **only if its `event_ts` is newer**, so late or replayed reports never move a vehicle backwards.
- Each alert has a deterministic `_id`, so a micro-batch that Spark re-runs after a crash does not store an alert twice.
- Times are stored as UTC dates. Extra detector columns (e.g. `time_gap_s`) go under `details`.

```
docker compose up -d mongo
python streaming/stream_to_parquet.py --source kafka --mongo   # Parquet archive + latest_positions
python streaming/detect_bunching.py --source kafka --mongo     # Parquet alerts + MongoDB alerts
python storage/mongo_sink.py                                   # counts, newest alerts, latest vehicles
```

Tests: `pytest storage/` (the MongoDB part is skipped when MongoDB is not running).
Checks: `./producer_check.sh SC` (producer to Kafka), `python ingestion/check_real.py` (only real data, follows the schema), `./spark_check.sh` (Kafka to Spark to Parquet).

## Ground truth: how accurate are the streaming detectors? (B18)

The streaming detectors close each window after a short watermark, so reports that arrive late are dropped. [`streaming/ground_truth.py`](streaming/ground_truth.py) is a **batch** job that reads the whole Parquet archive and applies the **same rules with the same code** (the detectors' own window and rule functions, settings from `streaming/config.py`) but with no watermark. Its output is the exact answer. [`streaming/compare_ground_truth.py`](streaming/compare_ground_truth.py) scores the streaming alerts against it.

| Detector | Ground-truth table | Matched on |
| --- | --- | --- |
| Bus bunching (B12) | `data/ground_truth/bunching` (every close pair, with `category` / `is_bunching`) | source, route, direction, minute, pair of buses |
| Stalled vehicle (B16) | `data/ground_truth/stalled` (one row per stall, full duration) | source, vehicle, overlapping time |
| Delay propagation (B15) | `data/ground_truth/delay_propagation` (knock-on delays: a late train followed by a late train at the same station) | source, route, station, pair of trains |

```
python streaming/ground_truth.py                 # archive -> data/ground_truth/*, benchmarks/results/ground_truth_summary.json
python streaming/compare_ground_truth.py         # precision / recall per detector -> benchmarks/results/ground_truth_comparison.json
```

- Use the same settings for both (e.g. `--stall-minutes 1` on the detector and on `ground_truth.py`).
- Ground-truth alerts in the last watermark before the archive ends are **pending** (the stream could not have closed them yet) and are left out of recall. `--since` / `--until` limit scoring to the time the streaming job ran.
- For bunching misses and false alarms, the comparison prints why: the stream labelled the pair differently (e.g. `no_history` after late look-back reports were dropped), or never saw the pair close.
- Trip updates are not in the Parquet archive yet, so delay-propagation ground truth reads `data/raw/trip_updates` if it exists, else the JSON-lines recording. There is no streaming delay detector yet, so that part is ground truth only until one writes to `data/alerts/delay_propagation`.

Tests: `pytest streaming/test_ground_truth.py`.

Benchmark (B22): `python benchmarks/bench_pipeline.py` measures Kafka → Spark → Parquet throughput and latency and saves CSVs to `benchmarks/results/` (see [`benchmarks/README.md`](benchmarks/README.md)).


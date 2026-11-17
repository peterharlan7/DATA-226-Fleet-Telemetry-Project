# Pipeline flow diagram (B29)

Mermaid source for the data flow, for GitHub and Notion. The static figure for the report and slides is
[`flow_diagram.svg`](flow_diagram.svg) / [`flow_diagram.png`](flow_diagram.png).

## 1. Live pipeline

Solid arrows = live path. Dashed = replay path (same jobs, different topic).

```mermaid
flowchart LR
  subgraph SRC["1 · Sources"]
    A511["511.org GTFS-rt<br/>vehicle positions<br/>SF Muni · SC VTA · AC"]
    BART["BART GTFS-rt<br/>trip updates (delays)"]
    RIN["Replay inputs<br/>Argoverse 2 · recording"]
  end

  subgraph ING["2 · Ingestion"]
    PROD["Python producers<br/>producer_all.py · producer_bart.py<br/>decode → HMAC vehicle_id (B13)<br/>→ B4 schema → validate → publish"]
    REP["Replay producer<br/>av_replay.py<br/>speed-up · dup / late faults"]
  end

  subgraph KAF["3 · Apache Kafka 3.8"]
    VP(["vehicle-positions"])
    TU(["trip-updates"])
    RT(["replay-* / bench-*"])
  end

  subgraph SPK["4 · Spark 3.5 Structured Streaming"]
    ARC["Archive job (B7)<br/>stream_to_parquet.py"]
    BUN["Bus-bunching detector (B12)<br/>detect_bunching.py<br/>&lt; 200 m · 60 s window · 30 s watermark"]
    STL["Stalled-vehicle detector (B16)<br/>detect_stalled.py<br/>&lt; 50 m for ≥ 10 min"]
    ALG["Streaming algorithms (B17, B23)<br/>Bloom · reservoir · Flajolet-Martin"]
  end

  subgraph STO["5 · Storage"]
    PQ[("Parquet archive<br/>data/raw/vehicle_events")]
    MDB[("MongoDB 7 (B11)<br/>latest_positions · alerts")]
    PA[("Parquet alerts<br/>bunching · stalled")]
    RJ["JSON / CSV reports<br/>benchmarks/results/"]
  end

  subgraph SRV["6 · Serving"]
    DASH["Live dashboard<br/>viz_web.py :8000"]
    OPS["Ops queries<br/>mongo_sink.py"]
    RPT["Report + slides"]
  end

  RIN -.-> REP
  BART --> PROD
  A511 --> PROD
  REP -.-> RT
  PROD --> TU
  PROD --> VP

  VP --> ALG
  VP --> STL
  VP --> BUN
  VP --> ARC
  RT -.->|same jobs, topic flag| ALG
  VP -->|reads Kafka directly| DASH

  ALG --> RJ
  STL --> PA
  BUN --> PA
  BUN -->|alerts| MDB
  ARC -->|newest position| MDB
  ARC --> PQ

  RJ --> RPT
  MDB --> OPS

  classDef src fill:#e0f2fe,stroke:#0284c7,color:#0f172a
  classDef ing fill:#ede9fe,stroke:#7c3aed,color:#0f172a
  classDef kaf fill:#fef3c7,stroke:#d97706,color:#92400e
  classDef spk fill:#dcfce7,stroke:#16a34a,color:#0f172a
  classDef sto fill:#f1f5f9,stroke:#475569,color:#0f172a
  classDef srv fill:#fee2e2,stroke:#dc2626,color:#0f172a
  classDef rep fill:#f8fafc,stroke:#94a3b8,stroke-dasharray:5 4,color:#334155
  style SRC fill:#f8fafc,stroke:#cbd5e1,color:#0f172a
  style ING fill:#f8fafc,stroke:#cbd5e1,color:#0f172a
  style KAF fill:#fffbeb,stroke:#fcd34d,color:#0f172a
  style SPK fill:#f0fdf4,stroke:#86efac,color:#0f172a
  style STO fill:#f8fafc,stroke:#cbd5e1,color:#0f172a
  style SRV fill:#fef2f2,stroke:#fca5a5,color:#0f172a
  class A511,BART src
  class PROD ing
  class VP,TU kaf
  class ARC,BUN,STL,ALG spk
  class PQ,MDB,PA,RJ sto
  class DASH,OPS,RPT srv
  class RIN,REP,RT rep
```

## 2. Offline: testing and evaluation

```mermaid
flowchart LR
  K(["Kafka live topics<br/>vehicle-positions · trip-updates"])
  REC["Record<br/>record_stream.py<br/>recording_24h.jsonl"]
  RPL["Replay into the detectors<br/>replay_recording.py<br/>original timestamps"]
  SPK["Spark detectors<br/>bunching · stalled"]
  ALR[("Streaming alerts<br/>Parquet")]
  PQ[("Parquet archive")]
  GT["Ground truth (B18)<br/>ground_truth.py<br/>same rules, no watermark"]
  SC["Score the stream<br/>compare_ground_truth.py<br/>precision · recall · why missed"]
  TF["Test feeds<br/>simulator_producer.py · fake_feed.py<br/>planted events, checked by evaluate_*.py"]
  AN["Analyses<br/>B15 delay propagation · B21 MinHash LSH<br/>B26 privacy · B27 fairness"]
  BEN["Benchmarks<br/>B22 throughput + latency<br/>B24 fault injection · B25 design comparison"]

  K -.-> REC
  REC -.-> RPL
  RPL -.-> SPK
  TF -.-> SPK
  SPK -.-> ALR
  ALR -.-> SC
  PQ -.-> GT
  GT -.-> SC
  REC -.-> AN
  PQ -.-> AN
  BEN -.->|own Kafka topic per run + real Spark job| K

  classDef kaf fill:#fef3c7,stroke:#d97706,color:#92400e
  classDef rec fill:#ffffff,stroke:#d97706,color:#0f172a
  classDef rpl fill:#ffffff,stroke:#7c3aed,color:#0f172a
  classDef spk fill:#dcfce7,stroke:#16a34a,color:#0f172a
  classDef sto fill:#f1f5f9,stroke:#475569,color:#0f172a
  classDef gt fill:#ffffff,stroke:#16a34a,color:#0f172a
  classDef an fill:#ffffff,stroke:#dc2626,color:#0f172a
  classDef tst fill:#f8fafc,stroke:#94a3b8,color:#334155
  class K kaf
  class REC rec
  class RPL rpl
  class SPK spk
  class ALR,PQ sto
  class GT,SC gt
  class AN an
  class TF,BEN tst
```

## Notes

- **Vehicle IDs are hashed at ingestion** (HMAC-SHA256, B13), so raw IDs never reach Kafka, Parquet or MongoDB.
- **The dashboard reads Kafka directly**, not MongoDB. It hides simulated (SIM) and replayed events.
- **trip-updates has no streaming job yet.** BART delays are recorded and analysed offline (B15).
- **No warehouse.** ClickHouse / DuckDB is in the README plan but not in the code.
- **MongoDB writes are idempotent:** alerts use a deterministic `_id`, and a position only replaces an older one.

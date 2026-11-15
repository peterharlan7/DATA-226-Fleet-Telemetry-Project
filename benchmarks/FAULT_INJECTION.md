# B24: fault injection and recovery

What happens to the Kafka → Spark → Parquet pipeline when a component dies mid-stream? How long until data
flows again, and is any event lost or written twice?

```
docker compose up -d kafka                         # only Kafka is needed
python benchmarks/fault_injection.py               # kill Kafka, then Spark: 3 trials per setting, ~35 min
benchmarks/kill_kafka.sh                           # Kafka kills only (any fault_injection.py option works)
benchmarks/kill_spark.sh --trials 5                # Spark kills only
python benchmarks/fault_injection.py --faults spark --trials 1 --duration 40 --kill-at 15   # quick check
```

Needs Java 17 and `pyspark`, like `spark_check.sh`. Tests for the helpers (no Kafka or Spark): `python -m pytest benchmarks`.

## How one trial works

1. A fresh topic `b24-<trial id>` (3 partitions, like the live topics) is created, so live topics are never touched.
2. The team's **real archive job**, `streaming/stream_to_parquet.py --source kafka`, starts as a child process with
   its own Parquet output and checkpoint, and runs empty for `--settle` seconds (JVM warm-up).
3. A producer publishes B4-schema vehicle events (`source=B24`) at `--rate` events/s for `--duration` seconds, and
   records Kafka's delivery report for every `event_id`. Each event's `ingest_ts` is the moment it was sent.
4. At `--kill-at` seconds one component is killed with **SIGKILL** (no clean shutdown, like a crash or OOM kill):
   - **Kafka**: `docker kill kafka`, wait `--kafka-down` s (default 10), `docker start kafka`. The producer and
     the Spark job are left alone and must reconnect by themselves.
   - **Spark**: SIGKILL to the job's process group (Python driver + JVM), wait `--spark-down` s (default 5), start
     the same command again with the **same checkpoint**, as a supervisor or restart policy would.
   If the Spark job dies on its own during a trial, the harness restarts it the same way and counts a `spark_crash`.
5. After the producer stops, the harness reads the whole topic back, waits until Spark has committed every
   event in it (or `--drain-timeout`), stops Spark with Ctrl+C, and counts.

The producer is tested under each guarantee in `ingestion/av_replay.py --delivery`:
`at-most-once` (acks=0, no retries), `at-least-once` (acks=all, retries, not idempotent) and `exactly-once`
(idempotent producer). A Spark kill does not touch the producer, so it is run with `at-least-once` only.

### How loss and duplicates are counted

Every event has a unique `event_id`, so each stage is a set (or multiset) of ids:

| Stage | Source |
| --- | --- |
| sent / acked / failed | the producer's delivery reports |
| in Kafka | every record in the topic, read back with a consumer |
| committed Parquet | files listed in the sink's `_spark_metadata` log: what **Spark** readers see |
| raw Parquet folder | every `.parquet` file on disk: what **pandas / pyarrow** see when they read the folder |

A killed micro-batch can leave files that were written but never committed. Spark readers ignore them; a plain
`pd.read_parquet("data/raw/vehicle_events")` does not. `orphan_files` / `raw_duplicates` measure that gap.

### How recovery time is measured

Both timestamps come from this machine's clock. For every committed row, `lag = spark_ts - ingest_ts`
(`spark_ts` is the micro-batch time the archive job stamps on each row).

- `component_ready_s`: restart command → Kafka accepts metadata requests for the topic again / the Spark job
  prints `Streaming from` (JVM up, query started from the checkpoint).
- `resume_s`: kill → first micro-batch with data after the restart (the pipeline is producing output again).
- `recovery_s`: kill → first micro-batch after the restart whose worst lag is back under
  `recovery_threshold_ms` = max(2 × pre-fault p95 lag, pre-fault p95 + one trigger), i.e. the backlog built up
  during the outage has been worked off. Empty if the load ended first.
- `max_lag_s`: the worst lag of any event in the trial (an event sent just after the kill).

## Output

Each sweep writes three CSVs to `benchmarks/results/`, appended trial by trial (a crash keeps finished trials):

- **`b24_trials_<stamp>.csv`**: one row per trial. The configuration, the timings above, `spark_crashes` /
  `spark_restarts`, and the counts: `events_sent`, `events_acked`, `events_failed`, `kafka_records`,
  `kafka_distinct`, `kafka_duplicates`, `acked_missing_in_kafka`, `parquet_rows`, `parquet_distinct`,
  `parquet_duplicates`, `lost_end_to_end` (sent but never committed), `lost_acked` (Kafka said stored, never
  committed: the serious kind), `delivered_despite_error`, `raw_rows`, `raw_duplicates`, `orphan_files`,
  `orphan_rows`, `drain_timed_out`.
- **`b24_summary_<stamp>.csv`**: one row per (fault, delivery): mean / min / max of each timing and totals of the
  loss and duplicate counts.
- **`b24_timeline_<stamp>.csv`**: one row per trial-second, relative to the kill (`second` < 0 is before it),
  with `phase` (before / down / recovering / after), events produced and committed, and median / max lag. Use it
  to plot the outage and the catch-up.

## Results

Sweep of 2026-11-14 (`b24_*_20261114_2216.csv`): 500 events/s, 100 s of load, kill at 30 s, 2 s trigger,
3 partitions, one MacBook (Spark `local[*]`, single-broker KRaft Kafka in Docker). 14 trials, about 50,000 events each.

| Fault | Producer | Trials | Down (s) | Component ready (s) | Resume (s) | Recovered (s) | Max lag (s) | Lost (acked) | Duplicates in Parquet |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Kafka | at-most-once | 3 | 10.4 | 15.3 | 26.6 (26.0–27.1) | 28.2 | 27.7 | **32** of 149,996 | 0 |
| Kafka | at-least-once | 3 | 10.3 | 15.2 | 26.1 (25.2–26.6) | 27.2 | 26.6 | 0 | **1** |
| Kafka | exactly-once | 3 | 10.3 | 15.4 | 25.9 (25.3–26.5) | 27.9 | 27.3 | 0 | 0 |
| Spark | at-least-once | 5 | 5.0 | 4.1 | 9.8 (8.9–11.2) | 11.8 | 10.8 | 0 | 0 |

Times are seconds after the kill (mean, with min–max for resume). Before each fault the median lag was about 1.0 s.

What the numbers say:

- **Kafka kill: about 26 s until data flows again, of which 10 s is the injected outage.** After an unclean
  shutdown the broker needs about 15 s to recover its logs and accept requests again. Spark and the producer
  reconnect by themselves within about 1 s of that, with no restart and no crash (`spark_crashes` = 0 in every
  trial). The backlog clears within about 2 s, so lag is back to normal about 28 s after the kill.
- **Spark kill: about 10 s until data flows again**, of which 5 s is the injected wait before the restart. The JVM
  and the query restart from the checkpoint in about 4 s, the first batch commits about 1 s later, and lag is back
  to normal at about 12 s.
- **Loss depends only on the producer guarantee.** With `at-most-once` (acks=0) Kafka reported every send as
  delivered, yet 9–12 events per Kafka kill never reached the topic: those were in flight when the broker died.
  `at-least-once` and `exactly-once` lost nothing. A Spark kill lost nothing: the job restarts from its checkpointed
  Kafka offsets and the file sink commits each batch once.
- **Duplicates come from producer retries, not Spark.** In one `at-least-once` trial the broker stored a batch but
  its acknowledgement was lost when it died, so the producer resent the batch and one event ended up in Kafka twice.
  Spark archived it twice (the archive job does not deduplicate; the B17 Bloom filter on `event_id` is what catches
  this downstream). The idempotent (`exactly-once`) producer had no duplicates. Spark's restart from the checkpoint
  added none in any trial.
- **No orphan files were seen** (`orphan_files` = 0 in all 5 Spark kills). Each kill landed between micro-batch
  writes, since a batch of about 1,000 rows writes in well under a second. A kill in the middle of a write can still
  leave uncommitted files that pandas would read, so readers of the archive should use Spark (which honours
  `_spark_metadata`) or check `orphan_files` with a longer sweep (`--spark-trials 20`, or a higher `--rate`).

Limits: one broker with replication factor 1, so a Kafka kill is a full outage, not a leader failover. Results
depend on the machine, so record it alongside the CSV when you cite numbers.

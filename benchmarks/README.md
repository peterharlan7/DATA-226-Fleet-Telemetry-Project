# Benchmarks

## B22: pipeline throughput and latency

`bench_pipeline.py` measures how many events per second the Kafka → Spark Structured Streaming → Parquet
path sustains, and how long an event takes from the producer to a committed Parquet file.

```
docker compose up -d kafka                     # only Kafka is needed
python benchmarks/bench_pipeline.py            # default sweep: 5 rates x 2 triggers, ~12 min
python benchmarks/bench_pipeline.py --rates 1000 --triggers 1 --duration 20     # quick check
python benchmarks/bench_pipeline.py --rates 5000,20000,40000 --triggers 1,5,10 --repeats 3
python benchmarks/bench_pipeline.py --sink noop          # Spark cost without the Parquet write
```

Needs Java 17 and `pyspark` (same as `spark_check.sh`). Spark runs in `local[*]` mode on this machine with the
same `build_spark()` and `transform()` as the archive job `streaming/stream_to_parquet.py`, so the numbers
describe the code we actually run.

### How one run works

1. A fresh topic `bench-<run_id>` (3 partitions by default, like the live topics) is created, so runs never
   read each other's data and live topics are never touched.
2. The Spark query starts and runs its first, empty micro-batch before any load, so JVM and connector start-up
   is not measured.
3. `load_generator.py` runs in its own process and publishes B4-schema events at the target rate:
   `--warmup` seconds tagged `source=WARMUP` (not measured), then `--duration` seconds tagged `source=BENCH`.
   Each event's `ingest_ts` is the moment it was handed to the Kafka producer.
4. In each micro-batch the harness writes the rows to Parquet, then takes the commit time and builds a 1 ms
   histogram of `commit time - ingest_ts` for the BENCH rows. Histograms are merged per run, so run-level
   percentiles are exact, not averages of batch percentiles.
5. The run ends when every delivered event is committed (or the drain times out), then the topic and the
   run's Parquet output are deleted (`--keep-output` keeps them).

Both timestamps come from the same machine's clock, so no clock-skew correction is needed.

### Output

Each sweep writes two CSVs to `benchmarks/results/`, appended run by run (a crash keeps finished runs):

**`b22_runs_<stamp>.csv`**: one row per run

| Column | Meaning |
| --- | --- |
| `rate_target`, `trigger_s`, `duration_s`, `partitions`, `sink`, `delivery` | the configuration |
| `events_sent` / `events_delivered` | produced / acknowledged by Kafka |
| `events_received`, `events_missing` | committed by Spark; `delivered - received` (should be 0) |
| `producer_rate` | events/s the load generator actually achieved |
| `pipeline_throughput` | committed rows / (last commit - first send). Includes the drain tail, so slightly below `producer_rate` even when Spark keeps up |
| `spark_processed_rps_mean` | mean of Spark's own `processedRowsPerSecond` over data batches |
| `batches`, `batch_rows_mean`, `batch_ms_mean/p95/max` | micro-batches with data and their duration (read + write) |
| `keeps_up` | True if 95% of micro-batches finish within one trigger interval, i.e. no growing backlog |
| `queue_ms_p50/p95/p99` | time an event waited in Kafka before its micro-batch started |
| `e2e_ms_mean/p50/p95/p99/max` | producer send → committed in Parquet |
| `timed_out` | the drain gave up before every event was committed |

**`b22_batches_<stamp>.csv`**: one row per micro-batch (`start_offset_s` is seconds after the first
measured send; negative = warm-up), with rows, read/write/batch milliseconds, batch latency percentiles and
Spark's `triggerExecution`, `inputRowsPerSecond` and `processedRowsPerSecond`. Use it to plot latency over
time and spot a backlog building up.

### Reading the results

- With a trigger of T seconds, an event waits on average about T/2 for the next batch, then the batch time.
  So `e2e_ms_p50 ≈ T/2 + batch_ms_mean` while Spark keeps up.
- Once `batch_ms` exceeds T, each batch starts late and grows: `keeps_up` turns False and e2e latency climbs
  through the run. The highest rate that still keeps up is the sustainable throughput for that trigger.
- Results depend on the machine (cores, disk). Record it alongside the CSV when you cite numbers.

Tests for the helpers (no Kafka or Spark needed): `python -m pytest benchmarks`

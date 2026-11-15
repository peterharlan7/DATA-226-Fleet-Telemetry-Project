#!/bin/bash
# B24: kill the Spark archive job (SIGKILL to driver + JVM) mid-stream, restart it on the same
# checkpoint, and measure recovery, loss and duplicates. Extra arguments go to fault_injection.py, e.g.
#   benchmarks/kill_spark.sh --trials 5 --spark-down 0
cd "$(dirname "$0")/.."
docker compose up -d kafka >/dev/null || exit 1
exec python benchmarks/fault_injection.py --faults spark "$@"

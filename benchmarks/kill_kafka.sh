#!/bin/bash
# B24: kill the Kafka broker (docker kill = SIGKILL) mid-stream, restart it, and measure recovery,
# loss and duplicates for each producer guarantee. Extra arguments go to fault_injection.py, e.g.
#   benchmarks/kill_kafka.sh --trials 1 --kafka-down 30
cd "$(dirname "$0")/.."
docker compose up -d kafka >/dev/null || exit 1
exec python benchmarks/fault_injection.py --faults kafka "$@"

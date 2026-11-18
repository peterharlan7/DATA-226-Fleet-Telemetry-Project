#!/bin/bash
# End-to-end smoke test: Kafka -> Spark archive job -> Parquet, with simulated or live data.
#   ./run_all.sh          simulated vehicles (no 511 key needed)
#   ./run_all.sh live     one pass over the live 511 feeds (needs API_511_KEY and VEHICLE_ID_SALT in .env)
# Uses the team's one archive job, streaming/stream_to_parquet.py, in Spark local mode
# (needs Java and pyspark on your computer, like spark_check.sh).
cd "$(dirname "$0")"
MODE=${1:-sim}   # sim = simulator, live = 511 feed
AGENCIES=${AGENCIES:-SF,SC}
mkdir -p logs data
LOG=logs/run_all_archive.log
# One Kafka address for every step. Without this, scripts that load .env can use a different port than Spark.
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}

command -v java >/dev/null || { echo "FAIL: Java is not installed. Run: brew install openjdk@17"; exit 1; }
python -c "import pyspark" 2>/dev/null || { echo "FAIL: pyspark is missing. Run: python -m pip install -r requirements.txt"; exit 1; }

docker compose up -d || { echo "FAIL: docker compose did not start. Is Docker running?"; exit 1; }
for i in $(seq 1 40); do   # up to 2 minutes
  [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = "healthy" ] && break
  echo "waiting for Kafka..."; sleep 3
done
if [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" != "healthy" ]; then
  echo "FAIL: Kafka was not healthy after 2 minutes. Is Docker running? Check: docker compose logs kafka"; exit 1
fi

echo "Starting the Spark archive job in the background (Kafka at $KAFKA_BOOTSTRAP_SERVERS)..."
python -u streaming/stream_to_parquet.py --source kafka > "$LOG" 2>&1 &
PID=$!
trap 'kill -INT $PID 2>/dev/null' EXIT

echo "Waiting for Spark to be ready (the first run downloads the Kafka connector)..."
for i in $(seq 1 60); do
  grep -q "^Streaming from" "$LOG" 2>/dev/null && break
  kill -0 $PID 2>/dev/null || break
  sleep 5
done
if ! grep -q "^Streaming from" "$LOG" 2>/dev/null; then
  echo "Spark did not start. Last log lines:"; tail -n 30 "$LOG"; exit 1
fi
echo "Spark is ready. Sending data..."

if [ "$MODE" = "live" ]; then
  python ingestion/producer_all.py --agencies "$AGENCIES" --once || exit 1
else
  python ingestion/simulator_producer.py --seconds 60 || exit 1
fi

sleep 15      # one more micro-batch (default trigger is 10 s)
kill -INT $PID 2>/dev/null; wait $PID 2>/dev/null
trap - EXIT

echo "---- Spark output (last lines) ----"
grep -E "^batch|Stopped" "$LOG" | tail -n 20
echo "---- Archive ----"
python streaming/stream_to_parquet.py --count 2>&1 | grep -vE "WARN|INFO|JAVA_TOOL|setLogLevel|^$"

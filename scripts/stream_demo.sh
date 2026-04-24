#!/usr/bin/env zsh
# stream_demo.sh — one-command streaming demo.
#
# Resets the streaming progress pointer, runs the simulator at a configurable
# rate, waits for the consumer to drain, then prints the inserted/duplicate/
# error totals and the RDS row-count delta.
#
# Tunables (export before running):
#   MAX_EVENTS=500        # how many events to send (default 500)
#   EVENTS_PER_SECOND=50  # producer throughput (default 50)
#
# Requires: source ./scripts/load_env.sh (API, S3_BUCKET, KINESIS_STREAM)
set -euo pipefail

: "${API:?source scripts/load_env.sh first}"
: "${S3_BUCKET:?source scripts/load_env.sh first}"
: "${KINESIS_STREAM:?source scripts/load_env.sh first}"
: "${STREAM_CONSUMER:?source scripts/load_env.sh first}"

MAX_EVENTS="${MAX_EVENTS:-500}"
EVENTS_PER_SECOND="${EVENTS_PER_SECOND:-50}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "── 1. Reset streaming progress in S3 ──"
aws s3 rm "s3://$S3_BUCKET/omop/streaming_progress.json" 2>/dev/null || true
echo "Done."

echo
echo "── 2. Pre-stream snapshot ──"
BEFORE=$(curl -s "$API/analytics/overview")
echo "$BEFORE" | python3 -m json.tool

echo
echo "── 3. Stream $MAX_EVENTS events @ ${EVENTS_PER_SECOND}/sec ──"
export S3_BUCKET KINESIS_STREAM MAX_EVENTS EVENTS_PER_SECOND
(cd "$PROJECT_DIR" && source etl/venv/bin/activate && python3 etl/stream_simulator.py)

echo
echo "── 4. Waiting 30s for consumer to drain ──"
sleep 30

echo
echo "── 5. Consumer totals (last 5 min) ──"
aws logs tail "/aws/lambda/$STREAM_CONSUMER" --since 5m 2>/dev/null \
    | grep "Inserted" \
    | awk -F'Inserted |, duplicates |, errors | \(of | received' \
          '{ins+=$2; dup+=$3; err+=$4; rec+=$5}
           END {printf "  received=%d  inserted=%d  duplicates=%d  errors=%d\n", rec, ins, dup, err}'

echo
echo "── 6. Post-stream snapshot ──"
curl -s "$API/analytics/overview" | python3 -m json.tool

#!/usr/bin/env zsh
# tail_all.sh — live-tail every pipeline Lambda in one stream.
#
# Fans out `aws logs tail --follow` across multiple Lambda log groups in
# parallel, prefixing each line with the function name so the interleaved
# stream is readable. Ctrl-C to stop all tails at once.
#
# Usage:
#   ./scripts/tail_all.sh              # all pipeline lambdas
#   ./scripts/tail_all.sh batch        # only batch lambdas
#   ./scripts/tail_all.sh stream       # only streaming lambda
#   ./scripts/tail_all.sh api          # only API lambda
#
# Set SINCE env var to override the default 5-minute backfill window, e.g.
# `SINCE=30m ./scripts/tail_all.sh batch` to catch up on a long-running run.
set -u

REGION="${AWS_REGION:-us-east-1}"
SINCE="${SINCE:-5m}"

BATCH_FNS=(
    schema-init
    etl-s3-to-rds
    etl-rds-to-redshift
    ml-redshift
    ml-comorbidity
)
STREAM_FNS=(stream-consumer)
API_FNS=(api)

case "${1:-all}" in
    batch)  FNS=("${BATCH_FNS[@]}") ;;
    stream) FNS=("${STREAM_FNS[@]}") ;;
    api)    FNS=("${API_FNS[@]}") ;;
    all)    FNS=("${BATCH_FNS[@]}" "${STREAM_FNS[@]}" "${API_FNS[@]}") ;;
    *)      echo "usage: $0 [all|batch|stream|api]" >&2; exit 1 ;;
esac

echo "Live-tailing ${#FNS[@]} Lambda log groups (Ctrl-C to stop):"
for fn in "${FNS[@]}"; do echo "  • $fn"; done
echo

pids=()

# Kill all background tails (and their descendants) on exit. `aws logs tail`
# is a Python process that doesn't always honour SIGTERM promptly, so go
# straight to SIGKILL and use pkill -P to sweep any stragglers (awk, etc).
cleanup() {
    for pid in "${pids[@]}"; do
        pkill -KILL -P "$pid" 2>/dev/null || true
        kill -KILL "$pid" 2>/dev/null || true
    done
    exit 0
}
trap cleanup INT TERM

for fn in "${FNS[@]}"; do
    # Pad name to 22 chars for aligned output.
    label=$(printf "%-22s" "$fn")
    aws logs tail "/aws/lambda/healthcare-dev-${fn}" \
        --follow --since "$SINCE" --region "$REGION" 2>/dev/null \
        | awk -v p="[${label}] " '{print p $0; fflush()}' &
    pids+=($!)
done

wait

#!/usr/bin/env zsh
# show_data.sh — snapshot of what's currently in the platform.
#
# Requires: source ./scripts/load_env.sh
set -u

: "${API:?set API (run: source scripts/load_env.sh)}"
: "${STREAM_CONSUMER:?set STREAM_CONSUMER (run: source scripts/load_env.sh)}"

echo "── RDS row counts (via API) ──"
curl -s "$API/analytics/overview" | python3 -m json.tool

echo
echo "── Stream consumer totals (last 15 min) ──"
aws logs tail "/aws/lambda/$STREAM_CONSUMER" --since 15m 2>/dev/null \
    | grep "Inserted" \
    | awk '
        {
            for (i = 1; i <= NF; i++) {
                if ($i == "Inserted")   ins += $(i+1) + 0
                if ($i == "duplicates") dup += $(i+1) + 0
                if ($i == "errors")     err += $(i+1) + 0
                if ($i == "(of")        rec += $(i+1) + 0
            }
        }
        END {
            if (rec == 0) print "  (no recent activity)"
            else printf "  received=%d  inserted=%d  duplicates=%d  errors=%d\n", rec, ins, dup, err
        }'

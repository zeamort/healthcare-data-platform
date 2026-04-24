#!/usr/bin/env zsh
# show_api.sh — hit representative REST API endpoints for the demo.
#
# Requires: source ./scripts/load_env.sh  (or export API yourself)
set -u

: "${API:?set API (run: source scripts/load_env.sh)}"

banner() { printf "\n── %s ──\n" "$1"; }

banner "GET /health"
curl -s "$API/health" | python3 -m json.tool

banner "GET /analytics/overview"
curl -s "$API/analytics/overview" | python3 -m json.tool

banner "GET /analytics/demographics"
curl -s "$API/analytics/demographics" | python3 -m json.tool

banner "GET /persons?limit=3"
curl -s "$API/persons?limit=3" | python3 -m json.tool

banner "GET /persons/1/summary"
curl -s "$API/persons/1/summary" | python3 -m json.tool

banner "GET /persons/1/conditions"
curl -s "$API/persons/1/conditions?limit=3" | python3 -m json.tool

echo
echo "Swagger UI: ${API%/}/docs"

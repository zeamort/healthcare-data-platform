#!/usr/bin/env zsh
# reset_data.sh — wipe all clinical data without tearing down infra.
#
# Re-invokes schema_init (DROP + CREATE of all RDS and Redshift tables) and
# deletes the streaming progress files from S3. Leaves infra, lambdas, ML
# models, ECR images, and dashboard untouched. Re-run setup_data.sh after
# to reload the Synthea dataset.
#
# Requires: source ./scripts/load_env.sh
set -euo pipefail

: "${SCHEMA_INIT_FN:?set SCHEMA_INIT_FN (run: source scripts/load_env.sh)}"
: "${S3_BUCKET:?set S3_BUCKET (run: source scripts/load_env.sh)}"

echo "Resetting RDS + Redshift schemas via $SCHEMA_INIT_FN ..."
aws lambda invoke \
    --function-name "$SCHEMA_INIT_FN" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    /tmp/schema_init_response.json \
    >/dev/null
cat /tmp/schema_init_response.json
echo

echo "Clearing streaming progress in s3://$S3_BUCKET/omop/ ..."
aws s3 rm "s3://$S3_BUCKET/omop/streaming_progress.json" 2>/dev/null || true
aws s3 rm "s3://$S3_BUCKET/omop/simulation_state.json" 2>/dev/null || true
aws s3 rm "s3://$S3_BUCKET/pipeline/s3_to_rds_complete.json" 2>/dev/null || true
aws s3 rm "s3://$S3_BUCKET/pipeline/rds_to_redshift_complete.json" 2>/dev/null || true

echo
echo "Done. Next:"
echo "  ./scripts/setup_data.sh       # reload Synthea → RDS → Redshift → ML"

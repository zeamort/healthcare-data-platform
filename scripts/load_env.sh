#!/usr/bin/env zsh
# load_env.sh — source this after `terraform apply` to populate env vars.
#
# Usage:
#   source ./scripts/load_env.sh
#
# After sourcing:
#   echo $API
#   curl -s "$API/health"
#   aws logs tail "/aws/lambda/$STREAM_CONSUMER" --follow

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="$PROJECT_DIR/terraform"

_tf() { (cd "$TF_DIR" && terraform output -raw "$1") }

export API="$(_tf api_endpoint)"
export S3_BUCKET="$(_tf s3_data_bucket)"
export KINESIS_STREAM="$(_tf kinesis_stream_name)"
export DASHBOARD_URL="$(_tf dashboard_url)"

export SCHEMA_INIT_FN="$(_tf schema_init_function)"
export ETL_S3_RDS_FN="$(_tf etl_s3_to_rds_function)"
export ETL_RDS_REDSHIFT_FN="$(_tf etl_rds_to_redshift_function)"
export API_FN="$(_tf api_function_name)"
export STREAM_CONSUMER="healthcare-dev-stream-consumer"

_redshift_host="$(_tf redshift_endpoint | cut -d: -f1)"
export REDSHIFT_CLUSTER="${_redshift_host%%.*}"
export REDSHIFT_DATABASE="$(_tf redshift_database)"

cat <<EOF
Environment loaded:
  API              = $API
  DASHBOARD_URL    = $DASHBOARD_URL
  S3_BUCKET        = $S3_BUCKET
  KINESIS_STREAM   = $KINESIS_STREAM
  REDSHIFT_CLUSTER = $REDSHIFT_CLUSTER
  STREAM_CONSUMER  = $STREAM_CONSUMER
EOF

#!/usr/bin/env zsh
# Source this file to get an `rs` function that runs SQL against the
# Redshift cluster via the Redshift Data API.
#
# Cluster identifier, database, user, and region are read live from Terraform
# outputs so this keeps working across fresh deploys.
#
# Usage:
#   source scripts/redshift_query.sh
#   rs "SELECT COUNT(*) FROM dim_patient;"

_tf_output() {
  (cd "$(dirname "${(%):-%x}")/../terraform" && terraform output -raw "$1" 2>/dev/null)
}

# Redshift endpoint is "host:port" — extract the host, strip the dev suffix
# back to the cluster identifier.
_redshift_host=$(_tf_output redshift_endpoint | cut -d: -f1)
CLUSTER="${_redshift_host%%.*}"
DATABASE="$(_tf_output redshift_database)"
DB_USER="${DB_USER:-postgres}"
export AWS_REGION="${AWS_REGION:-us-east-1}"

if [[ -z "$CLUSTER" ]]; then
  echo "Could not read cluster ID from 'terraform output redshift_endpoint'."
  echo "Make sure you've run 'terraform apply' and that this file is sourced from the repo root."
  return 1 2>/dev/null || exit 1
fi

echo "Redshift Data API helper loaded."
echo "  Cluster:  $CLUSTER"
echo "  Database: $DATABASE"
echo "  User:     $DB_USER"
echo "  Region:   $AWS_REGION"

rs() {
  local id state
  id=$(aws redshift-data execute-statement \
    --region "$AWS_REGION" \
    --cluster-identifier "$CLUSTER" \
    --database "$DATABASE" \
    --db-user "$DB_USER" \
    --sql "$1" \
    --query 'Id' --output text)
  if [[ -z "$id" || "$id" == "None" ]]; then
    echo "Failed to submit statement."
    return 1
  fi
  while true; do
    state=$(aws redshift-data describe-statement --region "$AWS_REGION" --id "$id" --query 'Status' --output text)
    case "$state" in
      FINISHED|FAILED|ABORTED) break ;;
    esac
    sleep 1
  done
  if [[ "$state" != "FINISHED" ]]; then
    aws redshift-data describe-statement --region "$AWS_REGION" --id "$id" --query 'Error' --output text
    return 1
  fi
  aws redshift-data get-statement-result --region "$AWS_REGION" --id "$id" --output text
}

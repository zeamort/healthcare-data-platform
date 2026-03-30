#!/usr/bin/env bash
#
# setup_data.sh — Copy Synthea OMOP data to the project S3 bucket
# and print commands for schema init + ETL execution.
#
# Prerequisites:
#   - AWS CLI configured with valid credentials
#   - Terraform already applied (terraform apply)
#
# Usage:
#   ./scripts/setup_data.sh              # uses 1k dataset (default)
#   ./scripts/setup_data.sh 1k           # 1,000 patients, ~25 MB
#
set -euo pipefail

DATASET="${1:-1k}"
SOURCE_BUCKET="synthea-omop"
SOURCE_PREFIX="synthea${DATASET}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

# Only the 1k dataset has plain CSVs; 100k is LZO-compressed.
if [[ "$DATASET" != "1k" ]]; then
    echo "WARNING: Only the 1k dataset has plain CSV files."
    echo "The 100k and 23m datasets are LZO-compressed and require decompression."
    read -p "Continue anyway? [y/N]: " confirm
    [[ "$confirm" =~ ^[Yy]$ ]] || exit 0
fi

# ── Read Terraform outputs ──────────────────────────────
echo "Reading Terraform outputs..."
cd "$PROJECT_DIR/terraform"

S3_BUCKET=$(terraform output -raw s3_data_bucket 2>/dev/null) || {
    echo "ERROR: Could not read terraform outputs. Have you run 'terraform apply'?"
    exit 1
}
S3_PREFIX="omop/"

RDS_ENDPOINT=$(terraform output -raw rds_endpoint 2>/dev/null)
RDS_HOST=$(echo "$RDS_ENDPOINT" | cut -d: -f1)
RDS_PORT=$(echo "$RDS_ENDPOINT" | cut -d: -f2)
RDS_DATABASE=$(terraform output -raw rds_database 2>/dev/null)

REDSHIFT_ENDPOINT=$(terraform output -raw redshift_endpoint 2>/dev/null)
REDSHIFT_HOST=$(echo "$REDSHIFT_ENDPOINT" | cut -d: -f1)
REDSHIFT_PORT=$(echo "$REDSHIFT_ENDPOINT" | cut -d: -f2)
REDSHIFT_DATABASE=$(terraform output -raw redshift_database 2>/dev/null)

KINESIS_STREAM=$(terraform output -raw kinesis_stream_name 2>/dev/null)

cd "$PROJECT_DIR"

echo ""
echo "Configuration:"
echo "  Source:   s3://${SOURCE_BUCKET}/${SOURCE_PREFIX}/"
echo "  Target:   s3://${S3_BUCKET}/${S3_PREFIX}"
echo "  RDS:      ${RDS_HOST}:${RDS_PORT}/${RDS_DATABASE}"
echo "  Redshift: ${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DATABASE}"
echo "  Kinesis:  ${KINESIS_STREAM}"
echo ""

# ── Step 1: Copy Synthea data to project S3 bucket ──────
echo "═══ Step 1: Copying Synthea ${DATASET} dataset to S3 ═══"

OMOP_FILES=(
    person.csv
    observation_period.csv
    visit_occurrence.csv
    condition_occurrence.csv
    drug_exposure.csv
    procedure_occurrence.csv
    measurement.csv
    observation.csv
    condition_era.csv
    drug_era.csv
)

for file in "${OMOP_FILES[@]}"; do
    echo "  Copying ${file}..."
    aws s3 cp "s3://${SOURCE_BUCKET}/${SOURCE_PREFIX}/${file}" \
              "s3://${S3_BUCKET}/${S3_PREFIX}${file}" \
              --quiet
done

echo ""
echo "  Done. Files in s3://${S3_BUCKET}/${S3_PREFIX}:"
aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}" --human-readable
echo ""

# ── Step 2: Print remaining commands ────────────────────
cat <<COMMANDS
═══ Data copied! Run the following steps: ═══

Step 2 — Initialize database schemas:

  psql -h ${RDS_HOST} -p ${RDS_PORT} -U postgres -d ${RDS_DATABASE} \\
       -f sql/rds_schema.sql

  psql -h ${REDSHIFT_HOST} -p ${REDSHIFT_PORT} -U postgres -d ${REDSHIFT_DATABASE} \\
       -f sql/redshift_schema.sql

Step 3 — Set environment variables:

  export S3_BUCKET=${S3_BUCKET}
  export S3_PREFIX=${S3_PREFIX}
  export RDS_HOST=${RDS_HOST}
  export RDS_PORT=${RDS_PORT}
  export RDS_DATABASE=${RDS_DATABASE}
  export RDS_USER=postgres
  export RDS_PASSWORD=<your-db-password>
  export REDSHIFT_HOST=${REDSHIFT_HOST}
  export REDSHIFT_PORT=${REDSHIFT_PORT}
  export REDSHIFT_DATABASE=${REDSHIFT_DATABASE}
  export REDSHIFT_USER=postgres
  export REDSHIFT_PASSWORD=<your-db-password>
  export KINESIS_STREAM=${KINESIS_STREAM}
  export AWS_REGION=us-east-1
  export CUTOFF_DATE=2020-01-01

Step 4 — Run batch ETL (loads records before cutoff into RDS):

  python3 etl/etl_s3_to_rds.py

Step 5 — Transform to Redshift star schema:

  python3 etl/etl_rds_to_redshift.py

Step 6 — Run ML analytics:

  python3 etl/ml_clustering.py
  python3 etl/ml_risk_scoring.py
  python3 etl/ml_comorbidity.py

Step 7 — Start streaming simulator (pushes post-cutoff data to Kinesis):

  python3 etl/stream_simulator.py

COMMANDS

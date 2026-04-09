#!/usr/bin/env bash
#
# setup_data.sh — Copy Synthea OMOP data to the project S3 bucket
# and trigger the automated ETL pipeline.
#
# After terraform apply + package_lambdas.sh, this is the only script
# you need to run. It copies OMOP CSVs to S3, then uploads a manifest
# file that automatically triggers the full pipeline:
#
#   _manifest.json → etl_s3_to_rds → etl_rds_to_redshift → ML Lambdas
#
# Prerequisites:
#   - AWS CLI configured with valid credentials
#   - Terraform already applied (terraform apply)
#   - Lambda packages deployed (./scripts/package_lambdas.sh)
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

cd "$PROJECT_DIR"

echo ""
echo "Configuration:"
echo "  Source:   s3://${SOURCE_BUCKET}/${SOURCE_PREFIX}/"
echo "  Target:   s3://${S3_BUCKET}/${S3_PREFIX}"
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
              --copy-props none --quiet
done

echo ""
echo "  Done. Files in s3://${S3_BUCKET}/${S3_PREFIX}:"
aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}" --human-readable
echo ""

# ── Step 2: Upload manifest to trigger the pipeline ─────
echo "═══ Step 2: Uploading manifest to trigger ETL pipeline ═══"

MANIFEST=$(cat <<EOF
{
  "dataset": "synthea${DATASET}",
  "files": [$(printf '"%s",' "${OMOP_FILES[@]}" | sed 's/,$//')],
  "uploaded_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
)

echo "$MANIFEST" | aws s3 cp - "s3://${S3_BUCKET}/${S3_PREFIX}_manifest.json" \
    --content-type application/json --quiet

echo "  Uploaded _manifest.json — pipeline triggered!"
echo ""
echo "═══ Pipeline is running automatically ═══"
echo ""
echo "  The following Lambda functions will execute in sequence:"
echo "    1. etl-s3-to-rds      (loads OMOP CSVs into RDS)"
echo "    2. etl-rds-to-redshift (transforms to Kimball star schema)"
echo "    3. ml-clustering       (patient segmentation)"
echo "    4. ml-risk-scoring     (readmission risk)"
echo "    5. ml-comorbidity      (disease co-occurrence)"
echo ""
echo "  Monitor progress in CloudWatch Logs or run:"
echo "    aws logs tail /aws/lambda/healthcare-dev-etl-s3-to-rds --follow"
echo ""
echo "  To start the streaming simulator after batch completes:"
echo "    python3 etl/stream_simulator.py"
echo ""

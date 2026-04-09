#!/usr/bin/env bash
#
# package_lambdas.sh — Build Lambda deployment zips and upload to S3.
#
# Creates deployment packages for each Lambda function:
#   - schema_init:          handler + SQL files
#   - etl_s3_to_rds:        handler + ETL script
#   - etl_rds_to_redshift:  handler + ETL script
#   - stream_consumer:      ETL script (already has handler)
#   - ml_clustering:        generic handler + ML script
#   - ml_risk_scoring:      generic handler + ML script
#   - ml_comorbidity:       generic handler + ML script
#
# Also builds a psycopg2 Lambda layer.
#
# Usage:
#   ./scripts/package_lambdas.sh
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/.build/lambda"
HANDLERS_DIR="$PROJECT_DIR/lambda/handlers"
ETL_DIR="$PROJECT_DIR/etl"
SQL_DIR="$PROJECT_DIR/sql"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR/zips"

echo "═══ Building Lambda deployment packages ═══"

# ── Helper ──────────────────────────────────────────────
build_zip() {
    local name="$1"
    local zip_path="$BUILD_DIR/zips/${name}.zip"
    local pkg_dir="$BUILD_DIR/${name}"

    mkdir -p "$pkg_dir"
    echo "  Building ${name}..."

    # Copy files passed as remaining args
    shift
    for src in "$@"; do
        cp "$src" "$pkg_dir/"
    done

    (cd "$pkg_dir" && zip -q -r "$zip_path" .)
    echo "    → ${name}.zip ($(du -h "$zip_path" | cut -f1))"
}

# ── Schema Init ─────────────────────────────────────────
build_schema_init() {
    local pkg_dir="$BUILD_DIR/schema_init"
    local zip_path="$BUILD_DIR/zips/schema_init.zip"
    mkdir -p "$pkg_dir/sql"
    cp "$HANDLERS_DIR/schema_init.py" "$pkg_dir/"
    cp "$SQL_DIR/rds_schema.sql" "$pkg_dir/sql/"
    cp "$SQL_DIR/redshift_schema.sql" "$pkg_dir/sql/"
    (cd "$pkg_dir" && zip -q -r "$zip_path" .)
    echo "  Building schema_init..."
    echo "    → schema_init.zip ($(du -h "$zip_path" | cut -f1))"
}

# ── ETL: S3 → RDS ──────────────────────────────────────
build_zip "etl_s3_to_rds" \
    "$HANDLERS_DIR/etl_s3_to_rds_handler.py" \
    "$ETL_DIR/etl_s3_to_rds.py"

# ── ETL: RDS → Redshift ────────────────────────────────
build_zip "etl_rds_to_redshift" \
    "$HANDLERS_DIR/etl_rds_to_redshift_handler.py" \
    "$ETL_DIR/etl_rds_to_redshift.py"

# ── Stream Consumer ─────────────────────────────────────
build_zip "stream_consumer" \
    "$ETL_DIR/stream_consumer.py"

# ── ML: Clustering ──────────────────────────────────────
build_zip "ml_clustering" \
    "$HANDLERS_DIR/ml_handler.py" \
    "$ETL_DIR/ml_clustering.py"

# ── ML: Risk Scoring ───────────────────────────────────
build_zip "ml_risk_scoring" \
    "$HANDLERS_DIR/ml_handler.py" \
    "$ETL_DIR/ml_risk_scoring.py"

# ── ML: Comorbidity ─────────────────────────────────────
build_zip "ml_comorbidity" \
    "$HANDLERS_DIR/ml_handler.py" \
    "$ETL_DIR/ml_comorbidity.py"

# ── Schema Init (special — includes sql/ directory) ─────
build_schema_init

# ── psycopg2 Lambda Layer ──────────────────────────────
echo ""
echo "═══ Building psycopg2 Lambda layer ═══"
LAYER_DIR="$BUILD_DIR/psycopg2-layer/python"
mkdir -p "$LAYER_DIR"

pip install psycopg2-binary -t "$LAYER_DIR" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    --quiet 2>/dev/null || {
    echo "  WARNING: Could not build psycopg2 layer locally."
    echo "  Falling back to Docker build..."
    docker run --rm -v "$BUILD_DIR/psycopg2-layer:/out" \
        public.ecr.aws/lambda/python:3.12 \
        pip install psycopg2-binary -t /out/python --quiet
}

(cd "$BUILD_DIR/psycopg2-layer" && zip -q -r "$BUILD_DIR/zips/psycopg2-layer.zip" python/)
echo "  → psycopg2-layer.zip ($(du -h "$BUILD_DIR/zips/psycopg2-layer.zip" | cut -f1))"

# ── Upload to S3 ───────────────────────────────────────
echo ""
echo "═══ Uploading to S3 ═══"

cd "$PROJECT_DIR/terraform"
S3_BUCKET=$(terraform output -raw s3_data_bucket 2>/dev/null | sed 's/data/lambda/')

# Derive the lambda bucket name from the data bucket name
LAMBDA_BUCKET=$(echo "$S3_BUCKET" | sed 's/-data-/-lambda-/')

echo "  Target: s3://${LAMBDA_BUCKET}/lambda/"

for zip_file in "$BUILD_DIR/zips/"*.zip; do
    filename=$(basename "$zip_file")
    echo "  Uploading ${filename}..."
    aws s3 cp "$zip_file" "s3://${LAMBDA_BUCKET}/lambda/${filename}" --quiet
done

echo ""
echo "═══ Done! Lambda packages uploaded. ═══"
echo ""
echo "Packages built:"
ls -lh "$BUILD_DIR/zips/"

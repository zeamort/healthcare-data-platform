#!/usr/bin/env bash
#
# package_lambdas.sh — Build Lambda deployment zips locally.
#
# Terraform references these zips directly via `filename`, so no S3
# upload is needed. Just run this before `terraform apply`.
#
# Usage:
#   ./scripts/package_lambdas.sh
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$PROJECT_DIR/.build/lambda/zips"
HANDLERS_DIR="$PROJECT_DIR/lambda/handlers"
ETL_DIR="$PROJECT_DIR/etl"
SQL_DIR="$PROJECT_DIR/sql"

rm -rf "$PROJECT_DIR/.build/lambda"
mkdir -p "$BUILD_DIR"

echo "═══ Building Lambda deployment packages ═══"

# ── Helper ──────────────────────────────────────────────
build_zip() {
    local name="$1"
    local zip_path="$BUILD_DIR/${name}.zip"
    local pkg_dir="$PROJECT_DIR/.build/lambda/${name}"

    mkdir -p "$pkg_dir"

    shift
    for src in "$@"; do
        cp "$src" "$pkg_dir/"
    done

    (cd "$pkg_dir" && zip -q -r "$zip_path" .)
    echo "  ${name}.zip ($(du -h "$zip_path" | cut -f1))"
}

# ── Schema Init (includes sql/ directory) ───────────────
echo "  Building schema_init..."
SCHEMA_DIR="$PROJECT_DIR/.build/lambda/schema_init"
mkdir -p "$SCHEMA_DIR/sql"
cp "$HANDLERS_DIR/schema_init.py" "$SCHEMA_DIR/"
cp "$SQL_DIR/rds_schema.sql" "$SCHEMA_DIR/sql/"
cp "$SQL_DIR/redshift_schema.sql" "$SCHEMA_DIR/sql/"
(cd "$SCHEMA_DIR" && zip -q -r "$BUILD_DIR/schema_init.zip" .)
echo "  schema_init.zip ($(du -h "$BUILD_DIR/schema_init.zip" | cut -f1))"

# ── ETL + ML handlers ──────────────────────────────────
build_zip "etl_s3_to_rds" \
    "$HANDLERS_DIR/etl_s3_to_rds_handler.py" \
    "$ETL_DIR/etl_s3_to_rds.py"

build_zip "etl_rds_to_redshift" \
    "$HANDLERS_DIR/etl_rds_to_redshift_handler.py" \
    "$ETL_DIR/etl_rds_to_redshift.py"

build_zip "stream_consumer" \
    "$ETL_DIR/stream_consumer.py"

build_zip "ml_redshift" \
    "$HANDLERS_DIR/ml_handler.py" \
    "$ETL_DIR/ml_redshift.py"

build_zip "ml_comorbidity" \
    "$HANDLERS_DIR/ml_handler.py" \
    "$ETL_DIR/ml_comorbidity.py"

# ── psycopg2 Lambda Layer ──────────────────────────────
echo ""
echo "═══ Building psycopg2 Lambda layer ═══"
LAYER_DIR="$PROJECT_DIR/.build/lambda/psycopg2-layer/python"
mkdir -p "$LAYER_DIR"

pip3 install psycopg2-binary -t "$LAYER_DIR" \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.12 \
    --only-binary=:all: \
    --quiet 2>/dev/null || {
    echo "  WARNING: Could not build psycopg2 layer with pip."
    echo "  Falling back to Docker build..."
    docker run --rm -v "$PROJECT_DIR/.build/lambda/psycopg2-layer:/out" \
        public.ecr.aws/lambda/python:3.12 \
        pip install psycopg2-binary -t /out/python --quiet
}

(cd "$PROJECT_DIR/.build/lambda/psycopg2-layer" && zip -q -r "$BUILD_DIR/psycopg2-layer.zip" python/)
echo "  psycopg2-layer.zip ($(du -h "$BUILD_DIR/psycopg2-layer.zip" | cut -f1))"

echo ""
echo "═══ Done! Zips ready in .build/lambda/zips/ ═══"
ls -lh "$BUILD_DIR/"
echo ""
echo "Next: cd terraform && terraform apply"

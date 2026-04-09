"""
Lambda handler for S3 → RDS ETL.

Triggered by an S3 event when _manifest.json is uploaded.
Verifies all expected OMOP CSV files are present, then runs the
batch ETL pipeline. Writes a completion marker to S3 to trigger
the next stage (RDS → Redshift).
"""

import os
import json
import logging

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "omop/")

s3 = boto3.client("s3")

EXPECTED_FILES = [
    "person.csv", "observation_period.csv", "visit_occurrence.csv",
    "condition_occurrence.csv", "drug_exposure.csv", "procedure_occurrence.csv",
    "measurement.csv", "observation.csv", "condition_era.csv", "drug_era.csv",
]


def _verify_files():
    """Check that all expected OMOP CSVs are in S3."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=S3_PREFIX)
    keys = {obj["Key"].replace(S3_PREFIX, "") for obj in response.get("Contents", [])}
    missing = [f for f in EXPECTED_FILES if f not in keys]
    if missing:
        raise FileNotFoundError(f"Missing OMOP files in S3: {missing}")
    log.info("All %d expected OMOP files present.", len(EXPECTED_FILES))


def _write_completion_marker():
    """Write a marker file to S3 to trigger the next pipeline stage."""
    marker_key = "pipeline/s3_to_rds_complete.json"
    body = json.dumps({"status": "complete", "source": "etl_s3_to_rds"})
    s3.put_object(Bucket=S3_BUCKET, Key=marker_key, Body=body,
                  ContentType="application/json")
    log.info("Wrote completion marker: s3://%s/%s", S3_BUCKET, marker_key)


def lambda_handler(event, context):
    """S3 event → verify files → run ETL → write marker."""
    log.info("Triggered by S3 event: %s", json.dumps(event, default=str))

    _verify_files()

    # Import and run the ETL (module reads env vars at import time)
    import etl_s3_to_rds
    etl_s3_to_rds.main()

    _write_completion_marker()

    return {"status": "complete"}

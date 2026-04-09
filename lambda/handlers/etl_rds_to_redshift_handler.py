"""
Lambda handler for RDS → Redshift ETL.

Triggered by an S3 completion marker from the S3→RDS stage.
Transforms OMOP data into Kimball star schema, then invokes
the ML Lambda functions asynchronously.
"""

import os
import json
import logging

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

lambda_client = boto3.client("lambda")

ML_FUNCTIONS = [
    os.environ.get("ML_REDSHIFT_FUNCTION", ""),
    os.environ.get("ML_COMORBIDITY_FUNCTION", ""),
]


def _invoke_ml_lambdas():
    """Invoke ML Lambda functions asynchronously."""
    for func_name in ML_FUNCTIONS:
        if not func_name:
            continue
        log.info("Invoking ML function: %s", func_name)
        lambda_client.invoke(
            FunctionName=func_name,
            InvocationType="Event",  # async
            Payload=json.dumps({"source": "etl_rds_to_redshift"}),
        )


def lambda_handler(event, context):
    """Run RDS → Redshift ETL, then trigger ML Lambdas."""
    log.info("Triggered by event: %s", json.dumps(event, default=str))

    import etl_rds_to_redshift
    etl_rds_to_redshift.main()

    _invoke_ml_lambdas()

    return {"status": "complete"}

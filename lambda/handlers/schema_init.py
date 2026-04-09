"""
Schema Init Lambda — initializes RDS and Redshift schemas.

Reads the SQL files bundled in the deployment package and executes
them against both databases. Designed to run once after infrastructure
is created (triggered by Terraform).
"""

import os
import logging

import psycopg2

log = logging.getLogger()
log.setLevel(logging.INFO)

RDS_HOST = os.environ["RDS_HOST"]
RDS_PORT = int(os.environ.get("RDS_PORT", "5432"))
RDS_DATABASE = os.environ.get("RDS_DATABASE", "healthcare")
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]

REDSHIFT_HOST = os.environ["REDSHIFT_HOST"]
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "analytics")
REDSHIFT_USER = os.environ["REDSHIFT_USER"]
REDSHIFT_PASSWORD = os.environ["REDSHIFT_PASSWORD"]


def _read_sql(filename):
    """Read a SQL file bundled in the deployment package."""
    path = os.path.join(os.path.dirname(__file__), "sql", filename)
    with open(path, "r") as f:
        return f.read()


def _execute_schema(host, port, database, user, password, sql, label):
    """Connect and execute a full SQL schema script."""
    log.info("Connecting to %s at %s:%d/%s", label, host, port, database)
    conn = psycopg2.connect(
        host=host, port=port, database=database,
        user=user, password=password,
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(sql)
        log.info("%s schema initialized successfully.", label)
    finally:
        cur.close()
        conn.close()


def lambda_handler(event, context):
    """Initialize both database schemas."""
    results = {}

    # RDS schema
    try:
        rds_sql = _read_sql("rds_schema.sql")
        _execute_schema(
            RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD,
            rds_sql, "RDS",
        )
        results["rds"] = "success"
    except Exception as e:
        log.error("RDS schema init failed: %s", e)
        results["rds"] = f"error: {e}"

    # Redshift schema
    try:
        redshift_sql = _read_sql("redshift_schema.sql")
        _execute_schema(
            REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE,
            REDSHIFT_USER, REDSHIFT_PASSWORD,
            redshift_sql, "Redshift",
        )
        results["redshift"] = "success"
    except Exception as e:
        log.error("Redshift schema init failed: %s", e)
        results["redshift"] = f"error: {e}"

    log.info("Schema init results: %s", results)

    if any("error" in v for v in results.values()):
        raise RuntimeError(f"Schema init had errors: {results}")

    return results

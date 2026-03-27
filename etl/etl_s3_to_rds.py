"""
ETL Pipeline: S3 → RDS PostgreSQL
Loads Synthea CSV files from S3 into the operational database.

Configuration is read from environment variables:
    S3_BUCKET       — S3 bucket containing raw CSVs
    S3_PREFIX       — key prefix (default: raw/)
    RDS_HOST        — RDS endpoint hostname
    RDS_PORT        — RDS port (default: 5432)
    RDS_DATABASE    — database name (default: healthcare)
    RDS_USER        — database username
    RDS_PASSWORD    — database password
"""

import os
import sys
import csv
import logging
from io import StringIO
from datetime import datetime

import boto3
import psycopg2

# ── Logging ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "raw/")
RDS_HOST = os.environ["RDS_HOST"]
RDS_PORT = int(os.environ.get("RDS_PORT", "5432"))
RDS_DATABASE = os.environ.get("RDS_DATABASE", "healthcare")
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]

s3 = boto3.client("s3")


# ── Helpers ──────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        database=RDS_DATABASE,
        user=RDS_USER,
        password=RDS_PASSWORD,
    )


def clean(value):
    """Return None for empty/null-like strings."""
    if value in ("", "NULL", None):
        return None
    return value


def parse_timestamp(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        return None


def read_csv_from_s3(key):
    """Download a CSV from S3 and return a DictReader."""
    s3_key = f"{S3_PREFIX}{key}"
    log.info("Downloading s3://%s/%s", S3_BUCKET, s3_key)
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    body = response["Body"].read().decode("utf-8")
    return csv.DictReader(StringIO(body))


# ── Table loaders ────────────────────────────────────────

def load_patients():
    log.info("Loading PATIENTS")
    reader = read_csv_from_s3("patients.csv")
    conn = get_connection()
    cur = conn.cursor()

    loaded, skipped = 0, 0
    try:
        for row in reader:
            if not row.get("Id"):
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO patients (
                    id, birthdate, deathdate, ssn, drivers, passport,
                    prefix, first_name, middle_name, last_name, suffix, maiden,
                    marital, race, ethnicity, gender, birthplace, address,
                    city, state, county, fips, zip, lat, lon,
                    healthcare_expenses, healthcare_coverage, income
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    row["Id"],
                    clean(row.get("BIRTHDATE")),
                    clean(row.get("DEATHDATE")),
                    clean(row.get("SSN")),
                    clean(row.get("DRIVERS")),
                    clean(row.get("PASSPORT")),
                    clean(row.get("PREFIX")),
                    clean(row.get("FIRST")),
                    clean(row.get("MIDDLE")),
                    clean(row.get("LAST")),
                    clean(row.get("SUFFIX")),
                    clean(row.get("MAIDEN")),
                    clean(row.get("MARITAL")),
                    clean(row.get("RACE")),
                    clean(row.get("ETHNICITY")),
                    row.get("GENDER"),
                    clean(row.get("BIRTHPLACE")),
                    clean(row.get("ADDRESS")),
                    clean(row.get("CITY")),
                    clean(row.get("STATE")),
                    clean(row.get("COUNTY")),
                    clean(row.get("FIPS")),
                    clean(row.get("ZIP")),
                    clean(row.get("LAT")),
                    clean(row.get("LON")),
                    clean(row.get("HEALTHCARE_EXPENSES")),
                    clean(row.get("HEALTHCARE_COVERAGE")),
                    clean(row.get("INCOME")),
                ),
            )
            loaded += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    log.info("Patients: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_encounters():
    log.info("Loading ENCOUNTERS")
    reader = read_csv_from_s3("encounters.csv")
    conn = get_connection()
    cur = conn.cursor()

    loaded, skipped = 0, 0
    try:
        for row in reader:
            if not row.get("Id") or not row.get("PATIENT"):
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO encounters (
                    id, start_time, stop_time, patient_id, organization_id,
                    provider_id, payer_id, encounter_class, code, description,
                    base_encounter_cost, total_claim_cost, payer_coverage,
                    reason_code, reason_description
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    row["Id"],
                    parse_timestamp(row["START"]),
                    clean(parse_timestamp(row.get("STOP"))),
                    row["PATIENT"],
                    clean(row.get("ORGANIZATION")),
                    clean(row.get("PROVIDER")),
                    clean(row.get("PAYER")),
                    clean(row.get("ENCOUNTERCLASS")),
                    clean(row.get("CODE")),
                    clean(row.get("DESCRIPTION")),
                    clean(row.get("BASE_ENCOUNTER_COST")),
                    clean(row.get("TOTAL_CLAIM_COST")),
                    clean(row.get("PAYER_COVERAGE")),
                    clean(row.get("REASONCODE")),
                    clean(row.get("REASONDESCRIPTION")),
                ),
            )
            loaded += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    log.info("Encounters: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_conditions():
    log.info("Loading CONDITIONS")
    reader = read_csv_from_s3("conditions.csv")
    conn = get_connection()
    cur = conn.cursor()

    loaded, skipped = 0, 0
    try:
        for row in reader:
            if not row.get("PATIENT"):
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO conditions (
                    start_date, stop_date, patient_id, encounter_id,
                    system, code, description
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    clean(row.get("START")),
                    clean(row.get("STOP")),
                    row["PATIENT"],
                    clean(row.get("ENCOUNTER")),
                    clean(row.get("SYSTEM")),
                    clean(row.get("CODE")),
                    clean(row.get("DESCRIPTION")),
                ),
            )
            loaded += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    log.info("Conditions: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_medications():
    log.info("Loading MEDICATIONS")
    reader = read_csv_from_s3("medications.csv")
    conn = get_connection()
    cur = conn.cursor()

    loaded, skipped = 0, 0
    try:
        for row in reader:
            if not row.get("PATIENT"):
                skipped += 1
                continue
            cur.execute(
                """
                INSERT INTO medications (
                    start_time, stop_time, patient_id, payer_id, encounter_id,
                    code, description, base_cost, payer_coverage, dispenses,
                    total_cost, reason_code, reason_description
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    parse_timestamp(row["START"]),
                    clean(parse_timestamp(row.get("STOP"))),
                    row["PATIENT"],
                    clean(row.get("PAYER")),
                    clean(row.get("ENCOUNTER")),
                    clean(row.get("CODE")),
                    clean(row.get("DESCRIPTION")),
                    clean(row.get("BASE_COST")),
                    clean(row.get("PAYER_COVERAGE")),
                    clean(row.get("DISPENSES")),
                    clean(row.get("TOTALCOST")),
                    clean(row.get("REASONCODE")),
                    clean(row.get("REASONDESCRIPTION")),
                ),
            )
            loaded += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    log.info("Medications: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


# ── Verification ─────────────────────────────────────────

def verify():
    conn = get_connection()
    cur = conn.cursor()
    try:
        tables = ["patients", "encounters", "conditions", "medications"]
        log.info("── Data verification ──")
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            log.info("  %-15s %s rows", table, f"{count:,}")

        cur.execute("SELECT * FROM data_quality_check")
        issues = cur.fetchall()
        for check_name, count in issues:
            if count > 0:
                log.warning("  DQ issue: %s = %d", check_name, count)
            else:
                log.info("  DQ pass:  %s", check_name)
    finally:
        cur.close()
        conn.close()


# ── Main ─────────────────────────────────────────────────

def main():
    log.info("ETL: S3 → RDS PostgreSQL")
    log.info("Source: s3://%s/%s", S3_BUCKET, S3_PREFIX)
    log.info("Target: %s/%s", RDS_HOST, RDS_DATABASE)

    results = {}
    loaders = [
        ("patients", load_patients),
        ("encounters", load_encounters),
        ("conditions", load_conditions),
        ("medications", load_medications),
    ]

    for name, loader in loaders:
        loaded, skipped = loader()
        results[name] = {"loaded": loaded, "skipped": skipped}

    verify()

    log.info("── ETL Summary ──")
    total_loaded = sum(r["loaded"] for r in results.values())
    total_skipped = sum(r["skipped"] for r in results.values())
    log.info("Total records loaded: %d", total_loaded)
    log.info("Total records skipped: %d", total_skipped)
    log.info("ETL complete.")


if __name__ == "__main__":
    main()

"""
ETL Pipeline: S3 → RDS PostgreSQL (OMOP CDM)
Loads Synthea OMOP CSV files from S3 into the operational database.

Supports batch/streaming split via CUTOFF_DATE: records with dates before
the cutoff are loaded in batch; records after are left for the streaming
simulator.

Configuration via environment variables:
    S3_BUCKET       — S3 bucket containing OMOP CSVs
    S3_PREFIX       — key prefix (default: omop/)
    RDS_HOST        — RDS endpoint hostname
    RDS_PORT        — RDS port (default: 5432)
    RDS_DATABASE    — database name (default: healthcare)
    RDS_USER        — database username
    RDS_PASSWORD    — database password
    CUTOFF_DATE     — optional date (YYYY-MM-DD) for batch/streaming split
"""

import os
import sys
import csv
import json
import logging
from io import StringIO
from datetime import date, datetime

import boto3
import psycopg2
from psycopg2.extras import execute_values

# ── Logging ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "omop/")
RDS_HOST = os.environ["RDS_HOST"]
RDS_PORT = int(os.environ.get("RDS_PORT", "5432"))
RDS_DATABASE = os.environ.get("RDS_DATABASE", "healthcare")
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]
CUTOFF_DATE = os.environ.get("CUTOFF_DATE")  # YYYY-MM-DD or None

s3 = boto3.client("s3")

BATCH_SIZE = 5000


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


def clean_int(value):
    """Parse integer or return None."""
    v = clean(value)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def clean_numeric(value):
    """Parse numeric or return None."""
    v = clean(value)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def clean_date(value):
    """Parse date string or return None."""
    v = clean(value)
    if v is None:
        return None
    try:
        return datetime.strptime(v[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def before_cutoff(date_value):
    """Return True if date is before cutoff (or no cutoff set)."""
    if CUTOFF_DATE is None:
        return True
    if date_value is None:
        return True
    cutoff = datetime.strptime(CUTOFF_DATE, "%Y-%m-%d").date()
    if isinstance(date_value, str):
        date_value = clean_date(date_value)
    if date_value is None:
        return True
    return date_value <= cutoff


def read_csv_from_s3(filename):
    """Download a CSV from S3 and return a DictReader."""
    s3_key = f"{S3_PREFIX}{filename}"
    log.info("Downloading s3://%s/%s", S3_BUCKET, s3_key)
    response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    body = response["Body"].read().decode("utf-8")
    return csv.DictReader(StringIO(body))


def bulk_insert(conn, table, columns, rows):
    """Insert rows in batches using execute_values for performance."""
    if not rows:
        return 0
    cur = conn.cursor()
    cols = ", ".join(columns)
    template = "(" + ", ".join(["%s"] * len(columns)) + ")"
    query = f"INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT DO NOTHING"
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        execute_values(cur, query, batch, template=template, page_size=BATCH_SIZE)
        total += len(batch)
    conn.commit()
    cur.close()
    return total


# ── OMOP Concept Seed ────────────────────────────────────
# The Synthea OMOP dataset doesn't include vocabulary tables.
# We seed a minimal concept table with well-known OMOP concept IDs
# used in the dataset, so views and API can resolve concept_id to names.

SEED_CONCEPTS = [
    # Gender
    (8507, "Male", "Gender", "Gender", "Gender", "M"),
    (8532, "Female", "Gender", "Gender", "Gender", "F"),
    # Race
    (8527, "White", "Race", "Race", "Race", "W"),
    (8516, "Black or African American", "Race", "Race", "Race", "B"),
    (8515, "Asian", "Race", "Race", "Race", "A"),
    (8657, "American Indian or Alaska Native", "Race", "Race", "Race", "AI"),
    (8557, "Native Hawaiian or Other Pacific Islander", "Race", "Race", "Race", "NH"),
    (0, "Unknown/No matching concept", "Metadata", "None", "Undefined", "0"),
    # Visit types
    (9201, "Inpatient Visit", "Visit", "Visit", "Visit", "IP"),
    (9202, "Outpatient Visit", "Visit", "Visit", "Visit", "OP"),
    (9203, "Emergency Room Visit", "Visit", "Visit", "Visit", "ER"),
    (262, "Emergency Room and Inpatient Visit", "Visit", "Visit", "Visit", "ERIP"),
    (9211, "Non-hospital institution Visit", "Visit", "Visit", "Visit", "NHIV"),
    (581477, "Ambulance Visit", "Visit", "Visit", "Visit", "AMB"),
    # Visit type concept
    (44818517, "Visit derived from EHR", "Type Concept", "Visit Type", "Visit Type", "OMOP4822053"),
    # Condition type
    (32020, "EHR encounter diagnosis", "Type Concept", "Condition Type", "Condition Type", "OMOP4822054"),
    # Drug type
    (581452, "Dispensed in Outpatient office", "Type Concept", "Drug Type", "Drug Type", "OMOP4822055"),
    # Procedure type
    (38000275, "EHR order list entry", "Type Concept", "Procedure Type", "Procedure Type", "OMOP4822056"),
    # Measurement type
    (5001, "Test", "Type Concept", "Meas Type", "Meas Type", "OMOP4822057"),
    # Observation type
    (38000280, "Observation recorded from EHR", "Type Concept", "Obs Type", "Obs Type", "OMOP4822058"),
    # Observation period type
    (44814724, "Period covering healthcare encounters", "Type Concept", "Obs Period Type", "Obs Period Type", "OMOP4822059"),
]


def load_seed_concepts(conn):
    """Insert well-known OMOP concept IDs used by the Synthea dataset."""
    log.info("Seeding concept lookup table")
    rows = [
        (cid, name, domain, vocab, cclass, code)
        for cid, name, domain, vocab, cclass, code in SEED_CONCEPTS
    ]
    columns = ["concept_id", "concept_name", "domain_id", "vocabulary_id",
               "concept_class_id", "concept_code"]
    return bulk_insert(conn, "concept", columns, rows)


def enrich_concepts_from_data(conn):
    """Discover concept_ids used in clinical tables but missing from concept.
    Populate them with source_value as a fallback name."""
    log.info("Enriching concept table from clinical data")
    cur = conn.cursor()

    sources = [
        ("condition_occurrence", "condition_concept_id", "condition_source_value"),
        ("drug_exposure", "drug_concept_id", "drug_source_value"),
        ("procedure_occurrence", "procedure_concept_id", "procedure_source_value"),
        ("measurement", "measurement_concept_id", "measurement_source_value"),
        ("observation", "observation_concept_id", "observation_source_value"),
    ]

    total_added = 0
    for table, concept_col, source_col in sources:
        cur.execute(f"""
            INSERT INTO concept (concept_id, concept_name, domain_id, concept_code)
            SELECT DISTINCT t.{concept_col},
                   COALESCE(MIN(t.{source_col}), 'Unknown'),
                   '{table.replace('_occurrence', '').replace('_exposure', '').title()}',
                   COALESCE(MIN(t.{source_col}), '')
            FROM {table} t
            LEFT JOIN concept c ON t.{concept_col} = c.concept_id
            WHERE c.concept_id IS NULL AND t.{concept_col} != 0
            GROUP BY t.{concept_col}
            ON CONFLICT (concept_id) DO NOTHING
        """)
        added = cur.rowcount
        total_added += added
        if added:
            log.info("  Added %d concepts from %s", added, table)

    conn.commit()
    cur.close()
    log.info("Enriched concept table with %d entries from clinical data", total_added)
    return total_added


# ── Table Loaders ────────────────────────────────────────

def load_person():
    """Load person.csv — always loaded in full (reference data)."""
    log.info("Loading PERSON")
    reader = read_csv_from_s3("person.csv")
    columns = [
        "person_id", "gender_concept_id", "year_of_birth", "month_of_birth",
        "day_of_birth", "birth_datetime", "race_concept_id", "ethnicity_concept_id",
        "location_id", "provider_id", "care_site_id", "person_source_value",
        "gender_source_value", "gender_source_concept_id", "race_source_value",
        "race_source_concept_id", "ethnicity_source_value", "ethnicity_source_concept_id",
    ]

    rows = []
    skipped = 0
    for row in reader:
        pid = clean_int(row.get("person_id"))
        if pid is None:
            skipped += 1
            continue
        rows.append((
            pid,
            clean_int(row.get("gender_concept_id")) or 0,
            clean_int(row.get("year_of_birth")),
            clean_int(row.get("month_of_birth")),
            clean_int(row.get("day_of_birth")),
            clean_date(row.get("birth_datetime")),
            clean_int(row.get("race_concept_id")) or 0,
            clean_int(row.get("ethnicity_concept_id")) or 0,
            clean_int(row.get("location_id")),
            clean_int(row.get("provider_id")),
            clean_int(row.get("care_site_id")),
            clean(row.get("person_source_value")),
            clean(row.get("gender_source_value")),
            clean_int(row.get("gender_source_concept_id")) or 0,
            clean(row.get("race_source_value")),
            clean_int(row.get("race_source_concept_id")) or 0,
            clean(row.get("ethnicity_source_value")),
            clean_int(row.get("ethnicity_source_concept_id")) or 0,
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "person", columns, rows)
    conn.close()
    log.info("Person: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_observation_period():
    """Load observation_period.csv — always loaded in full."""
    log.info("Loading OBSERVATION_PERIOD")
    reader = read_csv_from_s3("observation_period.csv")
    columns = [
        "observation_period_id", "person_id",
        "observation_period_start_date", "observation_period_end_date",
        "period_type_concept_id",
    ]

    rows = []
    skipped = 0
    for row in reader:
        opid = clean_int(row.get("observation_period_id"))
        if opid is None:
            skipped += 1
            continue
        rows.append((
            opid,
            clean_int(row.get("person_id")),
            clean_date(row.get("observation_period_start_date")),
            clean_date(row.get("observation_period_end_date")),
            clean_int(row.get("period_type_concept_id")) or 0,
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "observation_period", columns, rows)
    conn.close()
    log.info("Observation period: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_visit_occurrence():
    """Load visit_occurrence.csv with optional cutoff date filter."""
    log.info("Loading VISIT_OCCURRENCE")
    reader = read_csv_from_s3("visit_occurrence.csv")
    columns = [
        "visit_occurrence_id", "person_id", "visit_concept_id",
        "visit_start_date", "visit_start_datetime",
        "visit_end_date", "visit_end_datetime",
        "visit_type_concept_id", "provider_id", "care_site_id",
        "visit_source_value", "visit_source_concept_id",
        "admitting_source_concept_id", "admitting_source_value",
        "discharge_to_concept_id", "discharge_to_source_value",
        "preceding_visit_occurrence_id",
    ]

    # NOTE: visit_occurrence is NOT split by the batch/stream cutoff.
    # Child events (conditions, procedures, etc.) can have dates that straddle
    # their parent visit's date range, so filtering visits by start_date would
    # cause FK violations in either direction. Visit table is small (~27k rows)
    # so the full preload cost is negligible.
    rows = []
    skipped = 0
    for row in reader:
        vid = clean_int(row.get("visit_occurrence_id"))
        if vid is None:
            skipped += 1
            continue
        start_date = clean_date(row.get("visit_start_date"))
        rows.append((
            vid,
            clean_int(row.get("person_id")),
            clean_int(row.get("visit_concept_id")) or 0,
            start_date,
            clean_date(row.get("visit_start_datetime")),
            clean_date(row.get("visit_end_date")),
            clean_date(row.get("visit_end_datetime")),
            clean_int(row.get("visit_type_concept_id")) or 0,
            clean_int(row.get("provider_id")),
            clean_int(row.get("care_site_id")),
            clean(row.get("visit_source_value")),
            clean_int(row.get("visit_source_concept_id")) or 0,
            clean_int(row.get("admitting_source_concept_id")) or 0,
            clean(row.get("admitting_source_value")),
            clean_int(row.get("discharge_to_concept_id")) or 0,
            clean(row.get("discharge_to_source_value")),
            clean_int(row.get("preceding_visit_occurrence_id")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "visit_occurrence", columns, rows)
    conn.close()
    log.info("Visit occurrence: %d loaded, %d skipped (full preload — no cutoff)",
             loaded, skipped)
    return loaded, skipped


def load_condition_occurrence():
    """Load condition_occurrence.csv with optional cutoff date filter."""
    log.info("Loading CONDITION_OCCURRENCE")
    reader = read_csv_from_s3("condition_occurrence.csv")
    columns = [
        "condition_occurrence_id", "person_id", "condition_concept_id",
        "condition_start_date", "condition_start_datetime",
        "condition_end_date", "condition_end_datetime",
        "condition_type_concept_id", "stop_reason",
        "provider_id", "visit_occurrence_id", "visit_detail_id",
        "condition_source_value", "condition_source_concept_id",
        "condition_status_source_value", "condition_status_concept_id",
    ]

    rows = []
    skipped = 0
    cutoff_skipped = 0
    for row in reader:
        coid = clean_int(row.get("condition_occurrence_id"))
        if coid is None:
            skipped += 1
            continue
        start_date = clean_date(row.get("condition_start_date"))
        if not before_cutoff(start_date):
            cutoff_skipped += 1
            continue
        rows.append((
            coid,
            clean_int(row.get("person_id")),
            clean_int(row.get("condition_concept_id")) or 0,
            start_date,
            clean_date(row.get("condition_start_datetime")),
            clean_date(row.get("condition_end_date")),
            clean_date(row.get("condition_end_datetime")),
            clean_int(row.get("condition_type_concept_id")) or 0,
            clean(row.get("stop_reason")),
            clean_int(row.get("provider_id")),
            clean_int(row.get("visit_occurrence_id")),
            clean_int(row.get("visit_detail_id")),
            clean(row.get("condition_source_value")),
            clean_int(row.get("condition_source_concept_id")) or 0,
            clean(row.get("condition_status_source_value")),
            clean_int(row.get("condition_status_concept_id")) or 0,
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "condition_occurrence", columns, rows)
    conn.close()
    log.info("Condition occurrence: %d loaded, %d skipped, %d after cutoff",
             loaded, skipped, cutoff_skipped)
    return loaded, skipped


def load_drug_exposure():
    """Load drug_exposure.csv with optional cutoff date filter."""
    log.info("Loading DRUG_EXPOSURE")
    reader = read_csv_from_s3("drug_exposure.csv")
    columns = [
        "drug_exposure_id", "person_id", "drug_concept_id",
        "drug_exposure_start_date", "drug_exposure_start_datetime",
        "drug_exposure_end_date", "drug_exposure_end_datetime",
        "verbatim_end_date", "drug_type_concept_id", "stop_reason",
        "refills", "quantity", "days_supply", "sig",
        "route_concept_id", "lot_number",
        "provider_id", "visit_occurrence_id", "visit_detail_id",
        "drug_source_value", "drug_source_concept_id",
        "route_source_value", "dose_unit_source_value",
    ]

    rows = []
    skipped = 0
    cutoff_skipped = 0
    for row in reader:
        deid = clean_int(row.get("drug_exposure_id"))
        if deid is None:
            skipped += 1
            continue
        start_date = clean_date(row.get("drug_exposure_start_date"))
        if not before_cutoff(start_date):
            cutoff_skipped += 1
            continue
        rows.append((
            deid,
            clean_int(row.get("person_id")),
            clean_int(row.get("drug_concept_id")) or 0,
            start_date,
            clean_date(row.get("drug_exposure_start_datetime")),
            clean_date(row.get("drug_exposure_end_date")),
            clean_date(row.get("drug_exposure_end_datetime")),
            clean_date(row.get("verbatim_end_date")),
            clean_int(row.get("drug_type_concept_id")) or 0,
            clean(row.get("stop_reason")),
            clean_int(row.get("refills")),
            clean_numeric(row.get("quantity")),
            clean_int(row.get("days_supply")),
            clean(row.get("sig")),
            clean_int(row.get("route_concept_id")) or 0,
            clean(row.get("lot_number")),
            clean_int(row.get("provider_id")),
            clean_int(row.get("visit_occurrence_id")),
            clean_int(row.get("visit_detail_id")),
            clean(row.get("drug_source_value")),
            clean_int(row.get("drug_source_concept_id")) or 0,
            clean(row.get("route_source_value")),
            clean(row.get("dose_unit_source_value")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "drug_exposure", columns, rows)
    conn.close()
    log.info("Drug exposure: %d loaded, %d skipped, %d after cutoff",
             loaded, skipped, cutoff_skipped)
    return loaded, skipped


def load_procedure_occurrence():
    """Load procedure_occurrence.csv with optional cutoff date filter."""
    log.info("Loading PROCEDURE_OCCURRENCE")
    reader = read_csv_from_s3("procedure_occurrence.csv")
    columns = [
        "procedure_occurrence_id", "person_id", "procedure_concept_id",
        "procedure_date", "procedure_datetime",
        "procedure_type_concept_id", "modifier_concept_id", "quantity",
        "provider_id", "visit_occurrence_id", "visit_detail_id",
        "procedure_source_value", "procedure_source_concept_id",
        "modifier_source_value",
    ]

    rows = []
    skipped = 0
    cutoff_skipped = 0
    for row in reader:
        poid = clean_int(row.get("procedure_occurrence_id"))
        if poid is None:
            skipped += 1
            continue
        proc_date = clean_date(row.get("procedure_date"))
        if not before_cutoff(proc_date):
            cutoff_skipped += 1
            continue
        rows.append((
            poid,
            clean_int(row.get("person_id")),
            clean_int(row.get("procedure_concept_id")) or 0,
            proc_date,
            clean_date(row.get("procedure_datetime")),
            clean_int(row.get("procedure_type_concept_id")) or 0,
            clean_int(row.get("modifier_concept_id")) or 0,
            clean_int(row.get("quantity")),
            clean_int(row.get("provider_id")),
            clean_int(row.get("visit_occurrence_id")),
            clean_int(row.get("visit_detail_id")),
            clean(row.get("procedure_source_value")),
            clean_int(row.get("procedure_source_concept_id")) or 0,
            clean(row.get("modifier_source_value")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "procedure_occurrence", columns, rows)
    conn.close()
    log.info("Procedure occurrence: %d loaded, %d skipped, %d after cutoff",
             loaded, skipped, cutoff_skipped)
    return loaded, skipped


def load_measurement():
    """Load measurement.csv with optional cutoff date filter."""
    log.info("Loading MEASUREMENT")
    reader = read_csv_from_s3("measurement.csv")
    columns = [
        "measurement_id", "person_id", "measurement_concept_id",
        "measurement_date", "measurement_datetime", "measurement_time",
        "measurement_type_concept_id", "operator_concept_id",
        "value_as_number", "value_as_concept_id", "unit_concept_id",
        "range_low", "range_high",
        "provider_id", "visit_occurrence_id", "visit_detail_id",
        "measurement_source_value", "measurement_source_concept_id",
        "unit_source_value", "value_source_value",
    ]

    rows = []
    skipped = 0
    cutoff_skipped = 0
    for row in reader:
        mid = clean_int(row.get("measurement_id"))
        if mid is None:
            skipped += 1
            continue
        m_date = clean_date(row.get("measurement_date"))
        if not before_cutoff(m_date):
            cutoff_skipped += 1
            continue
        rows.append((
            mid,
            clean_int(row.get("person_id")),
            clean_int(row.get("measurement_concept_id")) or 0,
            m_date,
            clean_date(row.get("measurement_datetime")),
            clean(row.get("measurement_time")),
            clean_int(row.get("measurement_type_concept_id")) or 0,
            clean_int(row.get("operator_concept_id")) or 0,
            clean_numeric(row.get("value_as_number")),
            clean_int(row.get("value_as_concept_id")) or 0,
            clean_int(row.get("unit_concept_id")) or 0,
            clean_numeric(row.get("range_low")),
            clean_numeric(row.get("range_high")),
            clean_int(row.get("provider_id")),
            clean_int(row.get("visit_occurrence_id")),
            clean_int(row.get("visit_detail_id")),
            clean(row.get("measurement_source_value")),
            clean_int(row.get("measurement_source_concept_id")) or 0,
            clean(row.get("unit_source_value")),
            clean(row.get("value_source_value")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "measurement", columns, rows)
    conn.close()
    log.info("Measurement: %d loaded, %d skipped, %d after cutoff",
             loaded, skipped, cutoff_skipped)
    return loaded, skipped


def load_observation():
    """Load observation.csv with optional cutoff date filter."""
    log.info("Loading OBSERVATION")
    reader = read_csv_from_s3("observation.csv")
    columns = [
        "observation_id", "person_id", "observation_concept_id",
        "observation_date", "observation_datetime",
        "observation_type_concept_id",
        "value_as_number", "value_as_string", "value_as_concept_id",
        "qualifier_concept_id", "unit_concept_id",
        "provider_id", "visit_occurrence_id", "visit_detail_id",
        "observation_source_value", "observation_source_concept_id",
        "unit_source_value", "qualifier_source_value",
    ]

    rows = []
    skipped = 0
    cutoff_skipped = 0
    for row in reader:
        oid = clean_int(row.get("observation_id"))
        if oid is None:
            skipped += 1
            continue
        obs_date = clean_date(row.get("observation_date"))
        if not before_cutoff(obs_date):
            cutoff_skipped += 1
            continue
        rows.append((
            oid,
            clean_int(row.get("person_id")),
            clean_int(row.get("observation_concept_id")) or 0,
            obs_date,
            clean_date(row.get("observation_datetime")),
            clean_int(row.get("observation_type_concept_id")) or 0,
            clean_numeric(row.get("value_as_number")),
            clean(row.get("value_as_string")),
            clean_int(row.get("value_as_concept_id")) or 0,
            clean_int(row.get("qualifier_concept_id")) or 0,
            clean_int(row.get("unit_concept_id")) or 0,
            clean_int(row.get("provider_id")),
            clean_int(row.get("visit_occurrence_id")),
            clean_int(row.get("visit_detail_id")),
            clean(row.get("observation_source_value")),
            clean_int(row.get("observation_source_concept_id")) or 0,
            clean(row.get("unit_source_value")),
            clean(row.get("qualifier_source_value")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "observation", columns, rows)
    conn.close()
    log.info("Observation: %d loaded, %d skipped, %d after cutoff",
             loaded, skipped, cutoff_skipped)
    return loaded, skipped


def load_condition_era():
    """Load condition_era.csv — derived/aggregated, always loaded in full."""
    log.info("Loading CONDITION_ERA")
    reader = read_csv_from_s3("condition_era.csv")
    columns = [
        "condition_era_id", "person_id", "condition_concept_id",
        "condition_era_start_date", "condition_era_end_date",
        "condition_occurrence_count",
    ]

    rows = []
    skipped = 0
    for row in reader:
        ceid = clean_int(row.get("condition_era_id"))
        if ceid is None:
            skipped += 1
            continue
        rows.append((
            ceid,
            clean_int(row.get("person_id")),
            clean_int(row.get("condition_concept_id")) or 0,
            clean_date(row.get("condition_era_start_date")),
            clean_date(row.get("condition_era_end_date")),
            clean_int(row.get("condition_occurrence_count")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "condition_era", columns, rows)
    conn.close()
    log.info("Condition era: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


def load_drug_era():
    """Load drug_era.csv — derived/aggregated, always loaded in full."""
    log.info("Loading DRUG_ERA")
    reader = read_csv_from_s3("drug_era.csv")
    columns = [
        "drug_era_id", "person_id", "drug_concept_id",
        "drug_era_start_date", "drug_era_end_date",
        "drug_exposure_count", "gap_days",
    ]

    rows = []
    skipped = 0
    for row in reader:
        deid = clean_int(row.get("drug_era_id"))
        if deid is None:
            skipped += 1
            continue
        rows.append((
            deid,
            clean_int(row.get("person_id")),
            clean_int(row.get("drug_concept_id")) or 0,
            clean_date(row.get("drug_era_start_date")),
            clean_date(row.get("drug_era_end_date")),
            clean_int(row.get("drug_exposure_count")),
            clean_int(row.get("gap_days")),
        ))

    conn = get_connection()
    loaded = bulk_insert(conn, "drug_era", columns, rows)
    conn.close()
    log.info("Drug era: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


# ── Verification ─────────────────────────────────────────

def verify():
    conn = get_connection()
    cur = conn.cursor()
    try:
        tables = [
            "concept", "person", "observation_period", "visit_occurrence",
            "condition_occurrence", "drug_exposure", "procedure_occurrence",
            "measurement", "observation", "condition_era", "drug_era",
        ]
        log.info("── Data verification ──")
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            log.info("  %-25s %s rows", table, f"{count:,}")

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


# ── Simulation State ─────────────────────────────────────

def save_simulation_state():
    """Save the cutoff date to S3 so the streaming simulator knows where
    to pick up."""
    if CUTOFF_DATE is None:
        return
    state = {
        "batch_cutoff_date": CUTOFF_DATE,
        "batch_loaded_at": datetime.utcnow().isoformat(),
    }
    state_key = f"{S3_PREFIX}simulation_state.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=state_key,
        Body=json.dumps(state, indent=2),
        ContentType="application/json",
    )
    log.info("Saved simulation state to s3://%s/%s", S3_BUCKET, state_key)


# ── Main ─────────────────────────────────────────────────

def main():
    log.info("ETL: S3 → RDS PostgreSQL (OMOP CDM)")
    log.info("Source: s3://%s/%s", S3_BUCKET, S3_PREFIX)
    log.info("Target: %s/%s", RDS_HOST, RDS_DATABASE)
    if CUTOFF_DATE:
        log.info("Cutoff date: %s (records after this date reserved for streaming)",
                 CUTOFF_DATE)

    # Seed concept lookup
    conn = get_connection()
    load_seed_concepts(conn)
    conn.close()

    # Load tables in FK dependency order
    loaders = [
        ("person", load_person),
        ("observation_period", load_observation_period),
        ("visit_occurrence", load_visit_occurrence),
        ("condition_occurrence", load_condition_occurrence),
        ("drug_exposure", load_drug_exposure),
        ("procedure_occurrence", load_procedure_occurrence),
        ("measurement", load_measurement),
        ("observation", load_observation),
        ("condition_era", load_condition_era),
        ("drug_era", load_drug_era),
    ]

    results = {}
    for name, loader in loaders:
        loaded, skipped = loader()
        results[name] = {"loaded": loaded, "skipped": skipped}

    # Enrich concept table from loaded clinical data
    conn = get_connection()
    enrich_concepts_from_data(conn)
    conn.close()

    verify()
    save_simulation_state()

    log.info("── ETL Summary ──")
    total_loaded = sum(r["loaded"] for r in results.values())
    total_skipped = sum(r["skipped"] for r in results.values())
    log.info("Total records loaded: %s", f"{total_loaded:,}")
    log.info("Total records skipped: %s", f"{total_skipped:,}")
    log.info("ETL complete.")


if __name__ == "__main__":
    main()

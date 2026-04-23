"""
Kinesis Stream Consumer: Process clinical events into RDS (OMOP CDM)

Designed to run as an AWS Lambda function triggered by Kinesis.
Each record is a JSON clinical event (visit, condition, drug exposure, etc.)
that gets inserted into the corresponding OMOP table in RDS.

Configuration via environment variables:
    RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD
"""

import os
import json
import base64
import logging

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Lambda's runtime installs its own root handler before this module imports,
# so basicConfig() is a no-op and our level/handlers don't take effect. Force
# INFO on the root and our module logger so handler logs reach CloudWatch.
logging.getLogger().setLevel(logging.INFO)
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

RDS_HOST = os.environ["RDS_HOST"]
RDS_PORT = int(os.environ.get("RDS_PORT", "5432"))
RDS_DATABASE = os.environ.get("RDS_DATABASE", "healthcare")
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]

# Reuse connection across Lambda invocations
_conn = None


def get_connection():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(
            host=RDS_HOST, port=RDS_PORT,
            database=RDS_DATABASE, user=RDS_USER, password=RDS_PASSWORD,
        )
        _conn.autocommit = False
    return _conn


def clean(value):
    if value in ("", "NULL", None):
        return None
    return value


def clean_int(value):
    v = clean(value)
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def clean_numeric(value):
    v = clean(value)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


# ── Table Inserters ──────────────────────────────────────

def insert_visit_occurrence(cur, d):
    cur.execute("""
        INSERT INTO visit_occurrence (
            visit_occurrence_id, person_id, visit_concept_id,
            visit_start_date, visit_start_datetime,
            visit_end_date, visit_end_datetime,
            visit_type_concept_id, provider_id, care_site_id,
            visit_source_value, visit_source_concept_id,
            admitting_source_concept_id, admitting_source_value,
            discharge_to_concept_id, discharge_to_source_value,
            preceding_visit_occurrence_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (visit_occurrence_id) DO NOTHING
    """, (
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("visit_concept_id")) or 0,
        clean(d.get("visit_start_date")),
        clean(d.get("visit_start_datetime")),
        clean(d.get("visit_end_date")),
        clean(d.get("visit_end_datetime")),
        clean_int(d.get("visit_type_concept_id")) or 0,
        clean_int(d.get("provider_id")),
        clean_int(d.get("care_site_id")),
        clean(d.get("visit_source_value")),
        clean_int(d.get("visit_source_concept_id")) or 0,
        clean_int(d.get("admitting_source_concept_id")) or 0,
        clean(d.get("admitting_source_value")),
        clean_int(d.get("discharge_to_concept_id")) or 0,
        clean(d.get("discharge_to_source_value")),
        clean_int(d.get("preceding_visit_occurrence_id")),
    ))


def insert_condition_occurrence(cur, d):
    cur.execute("""
        INSERT INTO condition_occurrence (
            condition_occurrence_id, person_id, condition_concept_id,
            condition_start_date, condition_start_datetime,
            condition_end_date, condition_end_datetime,
            condition_type_concept_id, stop_reason,
            provider_id, visit_occurrence_id, visit_detail_id,
            condition_source_value, condition_source_concept_id,
            condition_status_source_value, condition_status_concept_id
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (condition_occurrence_id) DO NOTHING
    """, (
        clean_int(d.get("condition_occurrence_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("condition_concept_id")) or 0,
        clean(d.get("condition_start_date")),
        clean(d.get("condition_start_datetime")),
        clean(d.get("condition_end_date")),
        clean(d.get("condition_end_datetime")),
        clean_int(d.get("condition_type_concept_id")) or 0,
        clean(d.get("stop_reason")),
        clean_int(d.get("provider_id")),
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("visit_detail_id")),
        clean(d.get("condition_source_value")),
        clean_int(d.get("condition_source_concept_id")) or 0,
        clean(d.get("condition_status_source_value")),
        clean_int(d.get("condition_status_concept_id")) or 0,
    ))


def insert_drug_exposure(cur, d):
    cur.execute("""
        INSERT INTO drug_exposure (
            drug_exposure_id, person_id, drug_concept_id,
            drug_exposure_start_date, drug_exposure_start_datetime,
            drug_exposure_end_date, drug_exposure_end_datetime,
            verbatim_end_date, drug_type_concept_id, stop_reason,
            refills, quantity, days_supply, sig,
            route_concept_id, lot_number,
            provider_id, visit_occurrence_id, visit_detail_id,
            drug_source_value, drug_source_concept_id,
            route_source_value, dose_unit_source_value
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (drug_exposure_id) DO NOTHING
    """, (
        clean_int(d.get("drug_exposure_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("drug_concept_id")) or 0,
        clean(d.get("drug_exposure_start_date")),
        clean(d.get("drug_exposure_start_datetime")),
        clean(d.get("drug_exposure_end_date")),
        clean(d.get("drug_exposure_end_datetime")),
        clean(d.get("verbatim_end_date")),
        clean_int(d.get("drug_type_concept_id")) or 0,
        clean(d.get("stop_reason")),
        clean_int(d.get("refills")),
        clean_numeric(d.get("quantity")),
        clean_int(d.get("days_supply")),
        clean(d.get("sig")),
        clean_int(d.get("route_concept_id")) or 0,
        clean(d.get("lot_number")),
        clean_int(d.get("provider_id")),
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("visit_detail_id")),
        clean(d.get("drug_source_value")),
        clean_int(d.get("drug_source_concept_id")) or 0,
        clean(d.get("route_source_value")),
        clean(d.get("dose_unit_source_value")),
    ))


def insert_procedure_occurrence(cur, d):
    cur.execute("""
        INSERT INTO procedure_occurrence (
            procedure_occurrence_id, person_id, procedure_concept_id,
            procedure_date, procedure_datetime,
            procedure_type_concept_id, modifier_concept_id, quantity,
            provider_id, visit_occurrence_id, visit_detail_id,
            procedure_source_value, procedure_source_concept_id,
            modifier_source_value
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (procedure_occurrence_id) DO NOTHING
    """, (
        clean_int(d.get("procedure_occurrence_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("procedure_concept_id")) or 0,
        clean(d.get("procedure_date")),
        clean(d.get("procedure_datetime")),
        clean_int(d.get("procedure_type_concept_id")) or 0,
        clean_int(d.get("modifier_concept_id")) or 0,
        clean_int(d.get("quantity")),
        clean_int(d.get("provider_id")),
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("visit_detail_id")),
        clean(d.get("procedure_source_value")),
        clean_int(d.get("procedure_source_concept_id")) or 0,
        clean(d.get("modifier_source_value")),
    ))


def insert_measurement(cur, d):
    cur.execute("""
        INSERT INTO measurement (
            measurement_id, person_id, measurement_concept_id,
            measurement_date, measurement_datetime, measurement_time,
            measurement_type_concept_id, operator_concept_id,
            value_as_number, value_as_concept_id, unit_concept_id,
            range_low, range_high,
            provider_id, visit_occurrence_id, visit_detail_id,
            measurement_source_value, measurement_source_concept_id,
            unit_source_value, value_source_value
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (measurement_id) DO NOTHING
    """, (
        clean_int(d.get("measurement_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("measurement_concept_id")) or 0,
        clean(d.get("measurement_date")),
        clean(d.get("measurement_datetime")),
        clean(d.get("measurement_time")),
        clean_int(d.get("measurement_type_concept_id")) or 0,
        clean_int(d.get("operator_concept_id")) or 0,
        clean_numeric(d.get("value_as_number")),
        clean_int(d.get("value_as_concept_id")) or 0,
        clean_int(d.get("unit_concept_id")) or 0,
        clean_numeric(d.get("range_low")),
        clean_numeric(d.get("range_high")),
        clean_int(d.get("provider_id")),
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("visit_detail_id")),
        clean(d.get("measurement_source_value")),
        clean_int(d.get("measurement_source_concept_id")) or 0,
        clean(d.get("unit_source_value")),
        clean(d.get("value_source_value")),
    ))


def insert_observation(cur, d):
    cur.execute("""
        INSERT INTO observation (
            observation_id, person_id, observation_concept_id,
            observation_date, observation_datetime,
            observation_type_concept_id,
            value_as_number, value_as_string, value_as_concept_id,
            qualifier_concept_id, unit_concept_id,
            provider_id, visit_occurrence_id, visit_detail_id,
            observation_source_value, observation_source_concept_id,
            unit_source_value, qualifier_source_value
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (observation_id) DO NOTHING
    """, (
        clean_int(d.get("observation_id")),
        clean_int(d.get("person_id")),
        clean_int(d.get("observation_concept_id")) or 0,
        clean(d.get("observation_date")),
        clean(d.get("observation_datetime")),
        clean_int(d.get("observation_type_concept_id")) or 0,
        clean_numeric(d.get("value_as_number")),
        clean(d.get("value_as_string")),
        clean_int(d.get("value_as_concept_id")) or 0,
        clean_int(d.get("qualifier_concept_id")) or 0,
        clean_int(d.get("unit_concept_id")) or 0,
        clean_int(d.get("provider_id")),
        clean_int(d.get("visit_occurrence_id")),
        clean_int(d.get("visit_detail_id")),
        clean(d.get("observation_source_value")),
        clean_int(d.get("observation_source_concept_id")) or 0,
        clean(d.get("unit_source_value")),
        clean(d.get("qualifier_source_value")),
    ))


# ── Dispatch ─────────────────────────────────────────────

INSERTERS = {
    "visit_occurrence": insert_visit_occurrence,
    "condition_occurrence": insert_condition_occurrence,
    "drug_exposure": insert_drug_exposure,
    "procedure_occurrence": insert_procedure_occurrence,
    "measurement": insert_measurement,
    "observation": insert_observation,
}


# ── Lambda Handler ───────────────────────────────────────

def lambda_handler(event, context):
    """Process Kinesis records and insert into RDS."""
    conn = get_connection()
    cur = conn.cursor()

    inserted = 0
    duplicates = 0
    errors = 0

    try:
        for record in event["Records"]:
            try:
                payload = base64.b64decode(record["kinesis"]["data"])
                event_data = json.loads(payload)

                event_type = event_data.get("event_type")
                data = event_data.get("data", {})

                inserter = INSERTERS.get(event_type)
                if inserter is None:
                    log.warning("Unknown event type: %s", event_type)
                    continue

                # Savepoint per record: a single bad row (e.g. a dirty FK
                # reference) must not poison the rest of the Kinesis batch.
                # Without this, psycopg2 leaves the transaction in an aborted
                # state and every subsequent INSERT fails until rollback.
                cur.execute("SAVEPOINT rec")
                try:
                    inserter(cur, data)
                    # rowcount is 1 when inserted, 0 when ON CONFLICT DO NOTHING
                    # suppressed a duplicate primary key. Capture it BEFORE the
                    # RELEASE SAVEPOINT below, which runs its own execute and
                    # overwrites cur.rowcount to 0.
                    rc = cur.rowcount
                    cur.execute("RELEASE SAVEPOINT rec")
                    if rc == 1:
                        inserted += 1
                    else:
                        duplicates += 1
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT rec")
                    log.warning("Record dropped (%s): %s", event_type, e)
                    errors += 1

            except Exception as e:
                log.error("Malformed record: %s", e)
                errors += 1

        conn.commit()

    except Exception as e:
        conn.rollback()
        log.error("Batch failed: %s", e)
        raise

    finally:
        cur.close()

    log.info("Inserted %d, duplicates %d, errors %d (of %d received)",
             inserted, duplicates, errors, inserted + duplicates + errors)
    return {"inserted": inserted, "duplicates": duplicates, "errors": errors}

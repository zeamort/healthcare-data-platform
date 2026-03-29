"""
ETL Pipeline: RDS PostgreSQL → Redshift
Transforms normalized operational data into dimensional models for analytics.

Stages:
    1. Load dimension tables (dim_patient, dim_condition, dim_medication)
    2. Load fact tables (fact_encounters, fact_conditions, fact_medications)
    3. Build aggregate table (fact_patient_metrics)

Configuration via environment variables:
    RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD
    REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE, REDSHIFT_USER, REDSHIFT_PASSWORD
"""

import os
import sys
import logging
from datetime import date

import psycopg2

# ── Logging ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────

RDS = {
    "host": os.environ["RDS_HOST"],
    "port": int(os.environ.get("RDS_PORT", "5432")),
    "database": os.environ.get("RDS_DATABASE", "healthcare"),
    "user": os.environ["RDS_USER"],
    "password": os.environ["RDS_PASSWORD"],
}

REDSHIFT = {
    "host": os.environ["REDSHIFT_HOST"],
    "port": int(os.environ.get("REDSHIFT_PORT", "5439")),
    "database": os.environ.get("REDSHIFT_DATABASE", "analytics"),
    "user": os.environ["REDSHIFT_USER"],
    "password": os.environ["REDSHIFT_PASSWORD"],
}


def rds_conn():
    return psycopg2.connect(**RDS)


def rs_conn():
    return psycopg2.connect(**REDSHIFT)


# ── Helpers ──────────────────────────────────────────────

def rs_execute(sql, params=None):
    """Execute a single statement on Redshift and return row count."""
    conn = rs_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        count = cur.rowcount
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def rs_insert_batch(sql, rows, batch_size=500):
    """Insert rows into Redshift in batches."""
    conn = rs_conn()
    conn.autocommit = False
    cur = conn.cursor()
    total = 0
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(sql, batch)
            total += len(batch)
            if total % 2000 == 0:
                log.info("  ... %d rows inserted", total)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
    return total


def rds_fetch_all(sql):
    """Run a query on RDS and return all rows."""
    conn = rds_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


def compute_age(birthdate, deathdate=None):
    """Calculate age from birthdate, using deathdate if deceased."""
    if not birthdate:
        return None
    ref = deathdate if deathdate else date.today()
    return ref.year - birthdate.year - (
        (ref.month, ref.day) < (birthdate.month, birthdate.day)
    )


def age_group(age):
    if age is None:
        return None
    if age <= 17:
        return "0-17"
    if age <= 34:
        return "18-34"
    if age <= 49:
        return "35-49"
    if age <= 64:
        return "50-64"
    if age <= 79:
        return "65-79"
    return "80+"


def income_bracket(income):
    if income is None:
        return None
    if income < 30000:
        return "low"
    if income < 75000:
        return "middle"
    return "high"


# ── Dimension Loaders ────────────────────────────────────

def load_dim_patient():
    log.info("Loading dim_patient")

    rows = rds_fetch_all("""
        SELECT id, first_name, last_name, gender, race, ethnicity, marital,
               birthdate, deathdate, state, city, county, zip, income
        FROM patients
    """)

    # Clear and reload (Type 1 SCD — full refresh)
    rs_execute("DELETE FROM fact_patient_metrics")
    rs_execute("DELETE FROM dim_patient")

    insert_sql = """
        INSERT INTO dim_patient (
            patient_id, first_name, last_name, gender, race, ethnicity,
            marital_status, birthdate, deathdate, is_deceased,
            age, age_group, state, city, county, zip, income, income_bracket
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    transformed = []
    for row in rows:
        pid, fname, lname, gender, race, eth, marital, bdate, ddate, \
            st, city, county, zipcode, inc = row
        a = compute_age(bdate, ddate)
        transformed.append((
            pid, fname, lname, gender, race, eth, marital,
            bdate, ddate, ddate is not None,
            a, age_group(a), st, city, county, zipcode, inc, income_bracket(inc),
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_patient: %d rows loaded", count)
    return count


def load_dim_condition():
    log.info("Loading dim_condition")

    rows = rds_fetch_all("""
        SELECT DISTINCT code, description, system
        FROM conditions
        WHERE code IS NOT NULL
    """)

    rs_execute("DELETE FROM dim_condition")

    insert_sql = """
        INSERT INTO dim_condition (code, description, body_system, chronicity, severity_tier)
        VALUES (%s,%s,%s,%s,%s)
    """

    transformed = []
    for code, desc, system in rows:
        body_sys = classify_body_system(desc)
        chron = classify_chronicity(desc)
        severity = classify_severity(desc)
        transformed.append((code, desc, body_sys, chron, severity))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_condition: %d rows loaded", count)
    return count


def load_dim_medication():
    log.info("Loading dim_medication")

    rows = rds_fetch_all("""
        SELECT DISTINCT code, description
        FROM medications
        WHERE code IS NOT NULL
    """)

    rs_execute("DELETE FROM dim_medication")

    insert_sql = """
        INSERT INTO dim_medication (code, description, therapeutic_class)
        VALUES (%s,%s,%s)
    """

    transformed = []
    for code, desc in rows:
        t_class = classify_therapeutic_class(desc)
        transformed.append((code, desc, t_class))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_medication: %d rows loaded", count)
    return count


# ── Fact Loaders ─────────────────────────────────────────

def load_fact_encounters():
    log.info("Loading fact_encounters")

    rows = rds_fetch_all("""
        SELECT id, patient_id, start_time, stop_time, encounter_class,
               code, description, base_encounter_cost, total_claim_cost,
               payer_coverage, reason_code, reason_description
        FROM encounters
    """)

    rs_execute("DELETE FROM fact_encounters")

    insert_sql = """
        INSERT INTO fact_encounters (
            encounter_id, patient_id, date_key, encounter_class, code,
            description, start_time, stop_time, duration_minutes,
            base_encounter_cost, total_claim_cost, payer_coverage,
            patient_out_of_pocket, reason_code, reason_description
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    transformed = []
    for row in rows:
        eid, pid, start, stop, eclass, code, desc, base, total, payer, \
            rcode, rdesc = row

        date_key = int(start.strftime("%Y%m%d")) if start else None
        duration = None
        if start and stop:
            duration = int((stop - start).total_seconds() / 60)
        oop = None
        if total is not None and payer is not None:
            oop = max(float(total) - float(payer), 0)

        transformed.append((
            eid, pid, date_key, eclass, code, desc,
            start, stop, duration, base, total, payer, oop,
            rcode, rdesc,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_encounters: %d rows loaded", count)
    return count


def load_fact_conditions():
    log.info("Loading fact_conditions")

    rows = rds_fetch_all("""
        SELECT patient_id, encounter_id, code, start_date, stop_date
        FROM conditions
    """)

    rs_execute("DELETE FROM fact_conditions")

    insert_sql = """
        INSERT INTO fact_conditions (
            patient_id, encounter_id, condition_code, date_key,
            start_date, stop_date, is_active, duration_days
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    today = date.today()
    transformed = []
    for pid, eid, code, start, stop in rows:
        date_key = int(start.strftime("%Y%m%d")) if start else None
        is_active = stop is None or stop > today
        duration = None
        if start and stop:
            duration = (stop - start).days
        elif start:
            duration = (today - start).days

        transformed.append((
            pid, eid, code, date_key, start, stop, is_active, duration,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_conditions: %d rows loaded", count)
    return count


def load_fact_medications():
    log.info("Loading fact_medications")

    rows = rds_fetch_all("""
        SELECT patient_id, encounter_id, code, start_time, stop_time,
               base_cost, payer_coverage, total_cost, dispenses
        FROM medications
    """)

    rs_execute("DELETE FROM fact_medications")

    insert_sql = """
        INSERT INTO fact_medications (
            patient_id, encounter_id, medication_code, date_key,
            start_date, stop_date, is_active, duration_days,
            dispenses, base_cost, payer_coverage, total_cost
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    today = date.today()
    transformed = []
    for pid, eid, code, start, stop, base, payer, total, disp in rows:
        start_date = start.date() if start else None
        stop_date = stop.date() if stop else None
        date_key = int(start.strftime("%Y%m%d")) if start else None
        is_active = stop is None or stop_date > today
        duration = None
        if start_date and stop_date:
            duration = (stop_date - start_date).days
        elif start_date:
            duration = (today - start_date).days

        transformed.append((
            pid, eid, code, date_key, start_date, stop_date,
            is_active, duration, disp, base, payer, total,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_medications: %d rows loaded", count)
    return count


# ── Aggregate Builder ────────────────────────────────────

def build_fact_patient_metrics():
    """Build the pre-aggregated patient metrics table from fact tables."""
    log.info("Building fact_patient_metrics")

    rs_execute("DELETE FROM fact_patient_metrics")

    sql = """
    INSERT INTO fact_patient_metrics (
        patient_id, age, gender, state, income,
        total_encounters, wellness_encounters, emergency_encounters,
        inpatient_encounters, outpatient_encounters,
        ambulatory_encounters, urgentcare_encounters,
        total_conditions, active_conditions, chronic_condition_count,
        total_medications, active_medications, unique_medication_codes,
        polypharmacy_flag,
        total_encounter_cost, avg_encounter_cost,
        total_medication_cost, total_out_of_pocket,
        first_encounter_date, last_encounter_date,
        years_in_system, encounters_per_year, avg_days_between_encounters,
        had_30_day_readmission
    )
    SELECT
        dp.patient_id,
        dp.age,
        dp.gender,
        dp.state,
        dp.income,

        -- Encounter counts
        COALESCE(enc.total_encounters, 0),
        COALESCE(enc.wellness, 0),
        COALESCE(enc.emergency, 0),
        COALESCE(enc.inpatient, 0),
        COALESCE(enc.outpatient, 0),
        COALESCE(enc.ambulatory, 0),
        COALESCE(enc.urgentcare, 0),

        -- Condition counts
        COALESCE(cond.total_conditions, 0),
        COALESCE(cond.active_conditions, 0),
        COALESCE(cond.chronic_conditions, 0),

        -- Medication counts
        COALESCE(med.total_medications, 0),
        COALESCE(med.active_medications, 0),
        COALESCE(med.unique_codes, 0),
        COALESCE(med.active_medications, 0) >= 5,

        -- Costs
        COALESCE(enc.total_cost, 0),
        CASE WHEN COALESCE(enc.total_encounters, 0) > 0
             THEN COALESCE(enc.total_cost, 0) / enc.total_encounters
             ELSE 0 END,
        COALESCE(med.total_med_cost, 0),
        COALESCE(enc.total_oop, 0),

        -- Utilization
        enc.first_encounter,
        enc.last_encounter,
        CASE WHEN enc.first_encounter IS NOT NULL AND enc.last_encounter IS NOT NULL
             THEN DATEDIFF(day, enc.first_encounter, enc.last_encounter) / 365.25
             ELSE 0 END,
        CASE WHEN enc.first_encounter IS NOT NULL AND enc.last_encounter IS NOT NULL
                  AND DATEDIFF(day, enc.first_encounter, enc.last_encounter) > 0
             THEN enc.total_encounters * 365.25
                  / DATEDIFF(day, enc.first_encounter, enc.last_encounter)
             ELSE 0 END,
        CASE WHEN COALESCE(enc.total_encounters, 0) > 1
                  AND enc.first_encounter IS NOT NULL AND enc.last_encounter IS NOT NULL
             THEN DATEDIFF(day, enc.first_encounter, enc.last_encounter)
                  / (enc.total_encounters - 1.0)
             ELSE NULL END,

        -- 30-day readmission
        COALESCE(readmit.had_readmission, FALSE)

    FROM dim_patient dp

    LEFT JOIN (
        SELECT
            patient_id,
            COUNT(*)                                                    AS total_encounters,
            SUM(CASE WHEN encounter_class = 'wellness' THEN 1 ELSE 0 END) AS wellness,
            SUM(CASE WHEN encounter_class = 'emergency' THEN 1 ELSE 0 END) AS emergency,
            SUM(CASE WHEN encounter_class = 'inpatient' THEN 1 ELSE 0 END) AS inpatient,
            SUM(CASE WHEN encounter_class = 'outpatient' THEN 1 ELSE 0 END) AS outpatient,
            SUM(CASE WHEN encounter_class = 'ambulatory' THEN 1 ELSE 0 END) AS ambulatory,
            SUM(CASE WHEN encounter_class = 'urgentcare' THEN 1 ELSE 0 END) AS urgentcare,
            SUM(COALESCE(total_claim_cost, 0))                         AS total_cost,
            SUM(COALESCE(patient_out_of_pocket, 0))                    AS total_oop,
            MIN(start_time)::DATE                                      AS first_encounter,
            MAX(start_time)::DATE                                      AS last_encounter
        FROM fact_encounters
        GROUP BY patient_id
    ) enc ON dp.patient_id = enc.patient_id

    LEFT JOIN (
        SELECT
            patient_id,
            COUNT(*)                                        AS total_conditions,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END)     AS active_conditions,
            SUM(CASE WHEN duration_days > 365 OR is_active THEN 1 ELSE 0 END) AS chronic_conditions
        FROM fact_conditions
        GROUP BY patient_id
    ) cond ON dp.patient_id = cond.patient_id

    LEFT JOIN (
        SELECT
            patient_id,
            COUNT(*)                                        AS total_medications,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END)     AS active_medications,
            COUNT(DISTINCT medication_code)                 AS unique_codes,
            SUM(COALESCE(total_cost, 0))                    AS total_med_cost
        FROM fact_medications
        GROUP BY patient_id
    ) med ON dp.patient_id = med.patient_id

    LEFT JOIN (
        SELECT DISTINCT e1.patient_id, TRUE AS had_readmission
        FROM fact_encounters e1
        JOIN fact_encounters e2
            ON e1.patient_id = e2.patient_id
            AND e2.start_time > e1.start_time
            AND DATEDIFF(day, e1.start_time, e2.start_time) <= 30
            AND e1.encounter_id != e2.encounter_id
        WHERE e1.encounter_class IN ('inpatient', 'emergency')
    ) readmit ON dp.patient_id = readmit.patient_id
    """

    count = rs_execute(sql)
    log.info("fact_patient_metrics: %d rows built", count)
    return count


# ── Classification Helpers ───────────────────────────────
# These use keyword matching on Synthea descriptions to derive categories.
# In production you'd use a proper terminology service or lookup table.

BODY_SYSTEM_KEYWORDS = {
    "Cardiovascular": ["heart", "cardiac", "coronary", "hypertens", "atrial", "angina", "myocardial"],
    "Respiratory": ["lung", "asthma", "bronch", "pneumonia", "copd", "respiratory", "sinusitis"],
    "Endocrine": ["diabetes", "thyroid", "metabolic", "prediabetes", "hyperlipidemia", "cholesterol"],
    "Musculoskeletal": ["osteo", "arthritis", "fracture", "sprain", "joint", "back pain", "neck pain"],
    "Neurological": ["alzheimer", "epilepsy", "seizure", "migraine", "neuropathy", "stroke", "concussion"],
    "Mental Health": ["depression", "anxiety", "stress", "bipolar", "disorder", "adhd", "ptsd"],
    "Gastrointestinal": ["gastro", "gerd", "appendicitis", "liver", "hepat", "bowel", "crohn"],
    "Dermatological": ["dermatitis", "eczema", "acne", "skin", "rash", "burn"],
    "Renal": ["kidney", "renal", "urinary", "cystitis"],
    "Oncology": ["cancer", "carcinoma", "neoplasm", "tumor", "malignant", "lymphoma", "leukemia"],
    "Infectious": ["infection", "viral", "bacterial", "hiv", "influenza", "covid", "sepsis"],
    "Reproductive": ["pregnancy", "prenatal", "miscarriage", "contracepti"],
}

CHRONIC_KEYWORDS = [
    "diabetes", "hypertens", "asthma", "copd", "heart failure", "coronary",
    "chronic", "osteoporosis", "arthritis", "alzheimer", "epilepsy",
    "depression", "anxiety", "cancer", "hiv", "hepatitis", "obesity",
    "hyperlipidemia", "atrial fibrillation", "kidney disease",
]

SEVERE_KEYWORDS = [
    "cancer", "carcinoma", "malignant", "stroke", "myocardial infarction",
    "heart failure", "sepsis", "renal failure", "liver failure",
    "alzheimer", "hiv", "leukemia", "lymphoma", "pulmonary embolism",
]


def classify_body_system(description):
    if not description:
        return "Other"
    desc_lower = description.lower()
    for system, keywords in BODY_SYSTEM_KEYWORDS.items():
        if any(kw in desc_lower for kw in keywords):
            return system
    return "Other"


def classify_chronicity(description):
    if not description:
        return "acute"
    desc_lower = description.lower()
    if any(kw in desc_lower for kw in CHRONIC_KEYWORDS):
        return "chronic"
    return "acute"


def classify_severity(description):
    if not description:
        return "mild"
    desc_lower = description.lower()
    if any(kw in desc_lower for kw in SEVERE_KEYWORDS):
        return "severe"
    if any(kw in desc_lower for kw in CHRONIC_KEYWORDS):
        return "moderate"
    return "mild"


def classify_therapeutic_class(description):
    if not description:
        return "Other"
    desc_lower = description.lower()
    classes = {
        "Analgesic": ["acetaminophen", "ibuprofen", "naproxen", "aspirin", "pain"],
        "Antibiotic": ["amoxicillin", "penicillin", "azithromycin", "cephalexin", "antibiotic", "cillin"],
        "Antihypertensive": ["lisinopril", "amlodipine", "losartan", "metoprolol", "atenolol", "hydrochlorothiazide"],
        "Antidiabetic": ["metformin", "insulin", "glipizide", "glyburide"],
        "Statin": ["simvastatin", "atorvastatin", "rosuvastatin", "pravastatin", "lovastatin"],
        "Antidepressant": ["sertraline", "fluoxetine", "citalopram", "escitalopram", "venlafaxine"],
        "Bronchodilator": ["albuterol", "salbutamol", "ipratropium", "inhaler"],
        "Anticoagulant": ["warfarin", "heparin", "enoxaparin", "rivaroxaban", "apixaban"],
        "Proton Pump Inhibitor": ["omeprazole", "lansoprazole", "pantoprazole", "esomeprazole"],
        "Corticosteroid": ["prednisone", "prednisolone", "dexamethasone", "hydrocortisone", "methylprednisolone"],
        "Vaccine": ["vaccine", "immunization", "influenza vaccine", "pneumococcal"],
        "Contraceptive": ["contraceptive", "etonogestrel", "levonorgestrel", "medroxyprogesterone"],
        "Opioid": ["oxycodone", "hydrocodone", "morphine", "fentanyl", "codeine", "tramadol"],
        "Antihistamine": ["cetirizine", "loratadine", "diphenhydramine", "fexofenadine"],
    }
    for t_class, keywords in classes.items():
        if any(kw in desc_lower for kw in keywords):
            return t_class
    return "Other"


# ── Verification ─────────────────────────────────────────

def verify():
    conn = rs_conn()
    cur = conn.cursor()
    try:
        tables = [
            "dim_patient", "dim_condition", "dim_medication", "dim_date",
            "fact_encounters", "fact_conditions", "fact_medications",
            "fact_patient_metrics",
        ]
        log.info("── Redshift verification ──")
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            log.info("  %-25s %s rows", table, f"{count:,}")
    finally:
        cur.close()
        conn.close()


# ── Main ─────────────────────────────────────────────────

def main():
    log.info("ETL: RDS → Redshift")
    log.info("Source: %s/%s", RDS["host"], RDS["database"])
    log.info("Target: %s/%s", REDSHIFT["host"], REDSHIFT["database"])

    # Dimensions first
    load_dim_patient()
    load_dim_condition()
    load_dim_medication()

    # Facts
    load_fact_encounters()
    load_fact_conditions()
    load_fact_medications()

    # Aggregates
    build_fact_patient_metrics()

    # Verify
    verify()

    log.info("ETL complete.")


if __name__ == "__main__":
    main()

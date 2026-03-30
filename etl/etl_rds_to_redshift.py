"""
ETL Pipeline: RDS PostgreSQL (OMOP CDM) → Redshift
Transforms OMOP operational data into Kimball dimensional models for analytics.

Stages:
    1. Load dimension tables (dim_patient, dim_condition, dim_medication, dim_procedure)
    2. Load fact tables (fact_encounters, fact_conditions, fact_medications, fact_procedures)
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
            if total % 5000 == 0:
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


# ── Dimension Loaders ────────────────────────────────────

def load_dim_patient():
    log.info("Loading dim_patient")

    rows = rds_fetch_all("""
        SELECT
            p.person_id,
            p.gender_source_value,
            p.race_source_value,
            p.ethnicity_source_value,
            p.year_of_birth,
            p.birth_datetime,
            op.observation_period_start_date,
            op.observation_period_end_date
        FROM person p
        LEFT JOIN observation_period op ON p.person_id = op.person_id
    """)

    rs_execute("DELETE FROM fact_patient_metrics")
    rs_execute("DELETE FROM dim_patient")

    insert_sql = """
        INSERT INTO dim_patient (
            person_id, gender, race, ethnicity,
            year_of_birth, birth_datetime, is_deceased,
            age, age_group, observation_start, observation_end
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    today = date.today()
    transformed = []
    for row in rows:
        pid, gender, race, ethnicity, yob, bdate, obs_start, obs_end = row
        # Compute age from year of birth
        age = today.year - yob if yob else None
        # If observation period ended well in the past, likely deceased
        is_deceased = obs_end is not None and (today - obs_end).days > 365 if obs_end else False
        if is_deceased and age is not None and obs_end:
            age = obs_end.year - yob

        transformed.append((
            pid, gender, race, ethnicity,
            yob, bdate, is_deceased,
            age, age_group(age), obs_start, obs_end,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_patient: %d rows loaded", count)
    return count


def load_dim_condition():
    log.info("Loading dim_condition")

    # Get distinct condition concepts from RDS, joining concept table for names
    rows = rds_fetch_all("""
        SELECT DISTINCT
            co.condition_concept_id,
            COALESCE(c.concept_name, co.condition_source_value, 'Unknown'),
            c.concept_code,
            c.vocabulary_id
        FROM condition_occurrence co
        LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
        WHERE co.condition_concept_id != 0
    """)

    rs_execute("DELETE FROM dim_condition")

    insert_sql = """
        INSERT INTO dim_condition (
            condition_concept_id, concept_name, concept_code, vocabulary_id,
            body_system, chronicity, severity_tier
        ) VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    transformed = []
    for concept_id, name, code, vocab in rows:
        body_sys = classify_body_system(name)
        chron = classify_chronicity(name)
        severity = classify_severity(name)
        transformed.append((concept_id, name, code, vocab, body_sys, chron, severity))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_condition: %d rows loaded", count)
    return count


def load_dim_medication():
    log.info("Loading dim_medication")

    rows = rds_fetch_all("""
        SELECT DISTINCT
            de.drug_concept_id,
            COALESCE(c.concept_name, de.drug_source_value, 'Unknown'),
            c.concept_code,
            c.vocabulary_id
        FROM drug_exposure de
        LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
        WHERE de.drug_concept_id != 0
    """)

    rs_execute("DELETE FROM dim_medication")

    insert_sql = """
        INSERT INTO dim_medication (
            drug_concept_id, concept_name, concept_code, vocabulary_id,
            therapeutic_class
        ) VALUES (%s,%s,%s,%s,%s)
    """

    transformed = []
    for concept_id, name, code, vocab in rows:
        t_class = classify_therapeutic_class(name)
        transformed.append((concept_id, name, code, vocab, t_class))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_medication: %d rows loaded", count)
    return count


def load_dim_procedure():
    log.info("Loading dim_procedure")

    rows = rds_fetch_all("""
        SELECT DISTINCT
            po.procedure_concept_id,
            COALESCE(c.concept_name, po.procedure_source_value, 'Unknown'),
            c.concept_code,
            c.vocabulary_id
        FROM procedure_occurrence po
        LEFT JOIN concept c ON po.procedure_concept_id = c.concept_id
        WHERE po.procedure_concept_id != 0
    """)

    rs_execute("DELETE FROM dim_procedure")

    insert_sql = """
        INSERT INTO dim_procedure (
            procedure_concept_id, concept_name, concept_code, vocabulary_id,
            procedure_category
        ) VALUES (%s,%s,%s,%s,%s)
    """

    transformed = []
    for concept_id, name, code, vocab in rows:
        cat = classify_procedure_category(name)
        transformed.append((concept_id, name, code, vocab, cat))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("dim_procedure: %d rows loaded", count)
    return count


# ── Fact Loaders ─────────────────────────────────────────

def load_fact_encounters():
    log.info("Loading fact_encounters")

    rows = rds_fetch_all("""
        SELECT
            vo.visit_occurrence_id,
            vo.person_id,
            vo.visit_concept_id,
            COALESCE(c.concept_name, 'Unknown') AS visit_class,
            vo.visit_start_date,
            vo.visit_end_date
        FROM visit_occurrence vo
        LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
    """)

    rs_execute("DELETE FROM fact_encounters")

    insert_sql = """
        INSERT INTO fact_encounters (
            visit_occurrence_id, person_id, date_key,
            visit_concept_id, visit_class,
            visit_start_date, visit_end_date, duration_days
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    transformed = []
    for row in rows:
        vid, pid, vcid, vclass, start_date, end_date = row
        date_key = int(start_date.strftime("%Y%m%d")) if start_date else None
        duration = None
        if start_date and end_date:
            duration = (end_date - start_date).days

        transformed.append((
            vid, pid, date_key, vcid, vclass,
            start_date, end_date, duration,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_encounters: %d rows loaded", count)
    return count


def load_fact_conditions():
    log.info("Loading fact_conditions")

    rows = rds_fetch_all("""
        SELECT
            co.person_id,
            co.visit_occurrence_id,
            co.condition_concept_id,
            co.condition_start_date,
            co.condition_end_date
        FROM condition_occurrence co
    """)

    rs_execute("DELETE FROM fact_conditions")

    insert_sql = """
        INSERT INTO fact_conditions (
            person_id, visit_occurrence_id, condition_concept_id, date_key,
            condition_start_date, condition_end_date, is_active, duration_days
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    today = date.today()
    transformed = []
    for pid, vid, concept_id, start, end in rows:
        date_key = int(start.strftime("%Y%m%d")) if start else None
        is_active = end is None or end > today
        duration = None
        if start and end:
            duration = (end - start).days
        elif start:
            duration = (today - start).days

        transformed.append((
            pid, vid, concept_id, date_key, start, end, is_active, duration,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_conditions: %d rows loaded", count)
    return count


def load_fact_medications():
    log.info("Loading fact_medications")

    rows = rds_fetch_all("""
        SELECT
            de.person_id,
            de.visit_occurrence_id,
            de.drug_concept_id,
            de.drug_exposure_start_date,
            de.drug_exposure_end_date,
            de.days_supply,
            de.refills,
            de.quantity
        FROM drug_exposure de
    """)

    rs_execute("DELETE FROM fact_medications")

    insert_sql = """
        INSERT INTO fact_medications (
            person_id, visit_occurrence_id, drug_concept_id, date_key,
            drug_exposure_start_date, drug_exposure_end_date,
            is_active, duration_days, days_supply, refills, quantity
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    today = date.today()
    transformed = []
    for pid, vid, concept_id, start, end, supply, refills, qty in rows:
        date_key = int(start.strftime("%Y%m%d")) if start else None
        is_active = end is None or end > today
        duration = None
        if start and end:
            duration = (end - start).days
        elif start:
            duration = (today - start).days

        transformed.append((
            pid, vid, concept_id, date_key, start, end,
            is_active, duration, supply, refills, qty,
        ))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_medications: %d rows loaded", count)
    return count


def load_fact_procedures():
    log.info("Loading fact_procedures")

    rows = rds_fetch_all("""
        SELECT
            po.person_id,
            po.visit_occurrence_id,
            po.procedure_concept_id,
            po.procedure_date,
            po.quantity
        FROM procedure_occurrence po
    """)

    rs_execute("DELETE FROM fact_procedures")

    insert_sql = """
        INSERT INTO fact_procedures (
            person_id, visit_occurrence_id, procedure_concept_id, date_key,
            procedure_date, quantity
        ) VALUES (%s,%s,%s,%s,%s,%s)
    """

    transformed = []
    for pid, vid, concept_id, proc_date, qty in rows:
        date_key = int(proc_date.strftime("%Y%m%d")) if proc_date else None
        transformed.append((pid, vid, concept_id, date_key, proc_date, qty))

    count = rs_insert_batch(insert_sql, transformed)
    log.info("fact_procedures: %d rows loaded", count)
    return count


# ── Aggregate Builder ────────────────────────────────────

def build_fact_patient_metrics():
    """Build the pre-aggregated patient metrics table from fact tables."""
    log.info("Building fact_patient_metrics")

    rs_execute("DELETE FROM fact_patient_metrics")

    # OMOP visit concept names for classification
    sql = """
    INSERT INTO fact_patient_metrics (
        person_id, age, gender, race,
        total_encounters, inpatient_visits, outpatient_visits, emergency_visits,
        total_conditions, active_conditions, unique_condition_concepts,
        chronic_condition_count,
        total_drug_exposures, active_drug_exposures, unique_drug_concepts,
        polypharmacy_flag,
        total_procedures, total_measurements,
        first_visit_date, last_visit_date,
        observation_years, visits_per_year, avg_days_between_visits,
        had_30_day_readmission
    )
    SELECT
        dp.person_id,
        dp.age,
        dp.gender,
        dp.race,

        -- Encounter counts
        COALESCE(enc.total_visits, 0),
        COALESCE(enc.inpatient, 0),
        COALESCE(enc.outpatient, 0),
        COALESCE(enc.emergency, 0),

        -- Condition counts
        COALESCE(cond.total_conditions, 0),
        COALESCE(cond.active_conditions, 0),
        COALESCE(cond.unique_concepts, 0),
        COALESCE(cond.chronic_conditions, 0),

        -- Drug counts
        COALESCE(med.total_drugs, 0),
        COALESCE(med.active_drugs, 0),
        COALESCE(med.unique_concepts, 0),
        COALESCE(med.active_drugs, 0) >= 5,

        -- Procedure / measurement counts
        COALESCE(proc.total_procedures, 0),
        COALESCE(meas.total_measurements, 0),

        -- Utilization
        enc.first_visit,
        enc.last_visit,
        CASE WHEN enc.first_visit IS NOT NULL AND enc.last_visit IS NOT NULL
             THEN DATEDIFF(day, enc.first_visit, enc.last_visit) / 365.25
             ELSE 0 END,
        CASE WHEN enc.first_visit IS NOT NULL AND enc.last_visit IS NOT NULL
                  AND DATEDIFF(day, enc.first_visit, enc.last_visit) > 0
             THEN enc.total_visits * 365.25
                  / DATEDIFF(day, enc.first_visit, enc.last_visit)
             ELSE 0 END,
        CASE WHEN COALESCE(enc.total_visits, 0) > 1
                  AND enc.first_visit IS NOT NULL AND enc.last_visit IS NOT NULL
             THEN DATEDIFF(day, enc.first_visit, enc.last_visit)
                  / (enc.total_visits - 1.0)
             ELSE NULL END,

        -- 30-day readmission
        COALESCE(readmit.had_readmission, FALSE)

    FROM dim_patient dp

    LEFT JOIN (
        SELECT
            person_id,
            COUNT(*) AS total_visits,
            SUM(CASE WHEN visit_class ILIKE '%inpatient%' THEN 1 ELSE 0 END) AS inpatient,
            SUM(CASE WHEN visit_class ILIKE '%outpatient%' THEN 1 ELSE 0 END) AS outpatient,
            SUM(CASE WHEN visit_class ILIKE '%emergency%' THEN 1 ELSE 0 END) AS emergency,
            MIN(visit_start_date) AS first_visit,
            MAX(visit_start_date) AS last_visit
        FROM fact_encounters
        GROUP BY person_id
    ) enc ON dp.person_id = enc.person_id

    LEFT JOIN (
        SELECT
            person_id,
            COUNT(*) AS total_conditions,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_conditions,
            COUNT(DISTINCT condition_concept_id) AS unique_concepts,
            SUM(CASE WHEN duration_days > 365 OR is_active THEN 1 ELSE 0 END) AS chronic_conditions
        FROM fact_conditions
        GROUP BY person_id
    ) cond ON dp.person_id = cond.person_id

    LEFT JOIN (
        SELECT
            person_id,
            COUNT(*) AS total_drugs,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_drugs,
            COUNT(DISTINCT drug_concept_id) AS unique_concepts
        FROM fact_medications
        GROUP BY person_id
    ) med ON dp.person_id = med.person_id

    LEFT JOIN (
        SELECT person_id, COUNT(*) AS total_procedures
        FROM fact_procedures
        GROUP BY person_id
    ) proc ON dp.person_id = proc.person_id

    LEFT JOIN (
        SELECT person_id, COUNT(*) AS total_measurements
        FROM fact_encounters fe
        JOIN (SELECT visit_occurrence_id, COUNT(*) AS cnt
              FROM fact_procedures GROUP BY visit_occurrence_id) x
            ON fe.visit_occurrence_id = x.visit_occurrence_id
        GROUP BY person_id
    ) meas ON dp.person_id = meas.person_id

    LEFT JOIN (
        SELECT DISTINCT e1.person_id, TRUE AS had_readmission
        FROM fact_encounters e1
        JOIN fact_encounters e2
            ON e1.person_id = e2.person_id
            AND e2.visit_start_date > e1.visit_start_date
            AND DATEDIFF(day, e1.visit_start_date, e2.visit_start_date) <= 30
            AND e1.visit_occurrence_id != e2.visit_occurrence_id
        WHERE e1.visit_class ILIKE '%inpatient%'
           OR e1.visit_class ILIKE '%emergency%'
    ) readmit ON dp.person_id = readmit.person_id
    """

    count = rs_execute(sql)
    log.info("fact_patient_metrics: %d rows built", count)
    return count


# ── Classification Helpers ───────────────────────────────
# Keyword matching on concept names to derive clinical categories.

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


def classify_body_system(name):
    if not name:
        return "Other"
    lower = name.lower()
    for system, keywords in BODY_SYSTEM_KEYWORDS.items():
        if any(kw in lower for kw in keywords):
            return system
    return "Other"


def classify_chronicity(name):
    if not name:
        return "acute"
    lower = name.lower()
    if any(kw in lower for kw in CHRONIC_KEYWORDS):
        return "chronic"
    return "acute"


def classify_severity(name):
    if not name:
        return "mild"
    lower = name.lower()
    if any(kw in lower for kw in SEVERE_KEYWORDS):
        return "severe"
    if any(kw in lower for kw in CHRONIC_KEYWORDS):
        return "moderate"
    return "mild"


def classify_therapeutic_class(name):
    if not name:
        return "Other"
    lower = name.lower()
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
        "Corticosteroid": ["prednisone", "prednisolone", "dexamethasone", "hydrocortisone"],
        "Vaccine": ["vaccine", "immunization", "influenza vaccine", "pneumococcal"],
        "Contraceptive": ["contraceptive", "etonogestrel", "levonorgestrel", "medroxyprogesterone"],
        "Opioid": ["oxycodone", "hydrocodone", "morphine", "fentanyl", "codeine", "tramadol"],
        "Antihistamine": ["cetirizine", "loratadine", "diphenhydramine", "fexofenadine"],
    }
    for t_class, keywords in classes.items():
        if any(kw in lower for kw in keywords):
            return t_class
    return "Other"


def classify_procedure_category(name):
    if not name:
        return "Other"
    lower = name.lower()
    categories = {
        "Diagnostic": ["screening", "assessment", "evaluation", "examination", "test", "review"],
        "Imaging": ["x-ray", "radiograph", "ct scan", "mri", "ultrasound", "mammograph", "imaging"],
        "Surgical": ["surgery", "surgical", "excision", "incision", "repair", "replacement", "removal"],
        "Therapeutic": ["therapy", "treatment", "transfusion", "dialysis", "chemotherapy", "radiation"],
        "Preventive": ["vaccination", "immunization", "prophylaxis", "counseling", "education"],
        "Laboratory": ["blood", "urine", "biopsy", "culture", "panel", "analysis", "specimen"],
    }
    for cat, keywords in categories.items():
        if any(kw in lower for kw in keywords):
            return cat
    return "Other"


# ── Verification ─────────────────────────────────────────

def verify():
    conn = rs_conn()
    cur = conn.cursor()
    try:
        tables = [
            "dim_patient", "dim_condition", "dim_medication", "dim_procedure",
            "dim_date",
            "fact_encounters", "fact_conditions", "fact_medications",
            "fact_procedures", "fact_patient_metrics",
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
    log.info("ETL: RDS (OMOP CDM) → Redshift")
    log.info("Source: %s/%s", RDS["host"], RDS["database"])
    log.info("Target: %s/%s", REDSHIFT["host"], REDSHIFT["database"])

    # Dimensions first
    load_dim_patient()
    load_dim_condition()
    load_dim_medication()
    load_dim_procedure()

    # Facts
    load_fact_encounters()
    load_fact_conditions()
    load_fact_medications()
    load_fact_procedures()

    # Aggregates
    build_fact_patient_metrics()

    # Verify
    verify()

    log.info("ETL complete.")


if __name__ == "__main__":
    main()

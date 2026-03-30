-- Redshift Data Warehouse Schema — Analytics (OLAP)
-- Kimball star schema sourced from OMOP CDM operational tables.
-- Optimized for healthcare analytics, patient segmentation, and ML pipelines.

-- Drop in dependency order
DROP TABLE IF EXISTS comorbidity_analysis CASCADE;
DROP TABLE IF EXISTS fact_patient_metrics CASCADE;
DROP TABLE IF EXISTS fact_procedures CASCADE;
DROP TABLE IF EXISTS fact_medications CASCADE;
DROP TABLE IF EXISTS fact_conditions CASCADE;
DROP TABLE IF EXISTS fact_encounters CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_procedure CASCADE;
DROP TABLE IF EXISTS dim_medication CASCADE;
DROP TABLE IF EXISTS dim_condition CASCADE;
DROP TABLE IF EXISTS dim_patient CASCADE;

-- ════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════════════════════════

-- ── dim_patient ─────────────────────────────────────────
-- Type 1 SCD — overwrites on update.
-- Sourced from OMOP person table. No PII (ADR-006: data minimization).

CREATE TABLE dim_patient (
    patient_key       INTEGER IDENTITY(1,1) PRIMARY KEY,
    person_id         BIGINT NOT NULL UNIQUE,

    -- Demographics (from source values for readability)
    gender            VARCHAR(50),
    race              VARCHAR(100),
    ethnicity         VARCHAR(100),

    -- Birth / death
    year_of_birth     INTEGER NOT NULL,
    birth_datetime    DATE,
    is_deceased       BOOLEAN DEFAULT FALSE,

    -- Derived age attributes
    age               INTEGER,
    age_group         VARCHAR(20),   -- '0-17','18-34','35-49','50-64','65-79','80+'

    -- Observation period
    observation_start DATE,
    observation_end   DATE,

    loaded_at         TIMESTAMP DEFAULT GETDATE(),
    updated_at        TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (person_id)
SORTKEY (person_id, age_group);


-- ── dim_condition ───────────────────────────────────────
-- OMOP concept-based condition codes with clinical categorization.

CREATE TABLE dim_condition (
    condition_key       INTEGER IDENTITY(1,1) PRIMARY KEY,
    condition_concept_id INTEGER NOT NULL UNIQUE,
    concept_name        VARCHAR(500),
    concept_code        VARCHAR(100),         -- Original SNOMED code
    vocabulary_id       VARCHAR(50),

    -- Clinical categorization (keyword-derived)
    body_system         VARCHAR(100),
    chronicity          VARCHAR(20),          -- 'acute','chronic','episodic'
    severity_tier       VARCHAR(20),          -- 'mild','moderate','severe'

    loaded_at           TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (condition_concept_id);


-- ── dim_medication ──────────────────────────────────────
-- OMOP concept-based drug codes with therapeutic classification.

CREATE TABLE dim_medication (
    medication_key      INTEGER IDENTITY(1,1) PRIMARY KEY,
    drug_concept_id     INTEGER NOT NULL UNIQUE,
    concept_name        VARCHAR(500),
    concept_code        VARCHAR(100),         -- Original RxNorm/SNOMED code
    vocabulary_id       VARCHAR(50),

    -- Therapeutic categorization (keyword-derived)
    therapeutic_class   VARCHAR(100),

    loaded_at           TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (drug_concept_id);


-- ── dim_procedure ───────────────────────────────────────
-- OMOP concept-based procedure codes.

CREATE TABLE dim_procedure (
    procedure_key         INTEGER IDENTITY(1,1) PRIMARY KEY,
    procedure_concept_id  INTEGER NOT NULL UNIQUE,
    concept_name          VARCHAR(500),
    concept_code          VARCHAR(100),
    vocabulary_id         VARCHAR(50),

    -- Categorization
    procedure_category    VARCHAR(100),

    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (procedure_concept_id);


-- ── dim_date ────────────────────────────────────────────
-- Calendar dimension for time-series analytics.
-- Pre-populated from 2000-01-01 to 2030-12-31.

CREATE TABLE dim_date (
    date_key          INTEGER PRIMARY KEY,            -- YYYYMMDD integer
    full_date         DATE NOT NULL UNIQUE,
    year              SMALLINT NOT NULL,
    quarter           SMALLINT NOT NULL,
    month             SMALLINT NOT NULL,
    month_name        VARCHAR(10) NOT NULL,
    week_of_year      SMALLINT NOT NULL,
    day_of_month      SMALLINT NOT NULL,
    day_of_week       SMALLINT NOT NULL,
    day_name          VARCHAR(10) NOT NULL,
    is_weekend        BOOLEAN NOT NULL
)
DISTSTYLE ALL
SORTKEY (full_date);


-- ════════════════════════════════════════════════════════
-- FACT TABLES
-- ════════════════════════════════════════════════════════

-- ── fact_encounters ─────────────────────────────────────
-- Grain: one row per visit_occurrence.
-- Uses patient_key (surrogate) — no direct person_id for de-identification.

CREATE TABLE fact_encounters (
    encounter_key         INTEGER IDENTITY(1,1) PRIMARY KEY,
    visit_occurrence_id   BIGINT NOT NULL,
    patient_key           INTEGER NOT NULL,               -- FK → dim_patient (surrogate)
    date_key              INTEGER,                       -- FK → dim_date

    -- Visit attributes
    visit_concept_id      INTEGER,
    visit_class           VARCHAR(100),                  -- Resolved concept name

    -- Timestamps
    visit_start_date      DATE NOT NULL,
    visit_end_date        DATE,
    duration_days         INTEGER,

    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_key)
SORTKEY (visit_start_date, patient_key);


-- ── fact_conditions ─────────────────────────────────────
-- Grain: one row per condition_occurrence.

CREATE TABLE fact_conditions (
    condition_key_id        INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_key             INTEGER NOT NULL,               -- FK → dim_patient (surrogate)
    visit_occurrence_id     BIGINT,
    condition_concept_id    INTEGER,                       -- FK → dim_condition
    date_key                INTEGER,                       -- FK → dim_date

    condition_start_date    DATE NOT NULL,
    condition_end_date      DATE,
    is_active               BOOLEAN DEFAULT TRUE,
    duration_days           INTEGER,

    loaded_at               TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_key)
SORTKEY (condition_start_date, patient_key, condition_concept_id);


-- ── fact_medications ────────────────────────────────────
-- Grain: one row per drug_exposure.

CREATE TABLE fact_medications (
    medication_key_id       INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_key             INTEGER NOT NULL,               -- FK → dim_patient (surrogate)
    visit_occurrence_id     BIGINT,
    drug_concept_id         INTEGER,                       -- FK → dim_medication
    date_key                INTEGER,                       -- FK → dim_date

    drug_exposure_start_date DATE NOT NULL,
    drug_exposure_end_date   DATE,
    is_active               BOOLEAN DEFAULT TRUE,
    duration_days           INTEGER,
    days_supply             INTEGER,
    refills                 INTEGER,
    quantity                NUMERIC,

    loaded_at               TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_key)
SORTKEY (drug_exposure_start_date, patient_key, drug_concept_id);


-- ── fact_procedures ─────────────────────────────────────
-- Grain: one row per procedure_occurrence.

CREATE TABLE fact_procedures (
    procedure_key_id        INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_key             INTEGER NOT NULL,               -- FK → dim_patient (surrogate)
    visit_occurrence_id     BIGINT,
    procedure_concept_id    INTEGER,                       -- FK → dim_procedure
    date_key                INTEGER,                       -- FK → dim_date

    procedure_date          DATE NOT NULL,
    quantity                INTEGER,

    loaded_at               TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_key)
SORTKEY (procedure_date, patient_key);


-- ── fact_patient_metrics ────────────────────────────────
-- Pre-aggregated patient-level metrics for ML and dashboards.
-- Rebuilt by the analytics ETL after each data load.

CREATE TABLE fact_patient_metrics (
    patient_key                   INTEGER PRIMARY KEY,       -- FK → dim_patient (surrogate, no person_id)

    -- Demographics (denormalized for ML feature vectors)
    age                           INTEGER,
    gender                        VARCHAR(50),
    race                          VARCHAR(100),

    -- Encounter metrics
    total_encounters              INTEGER DEFAULT 0,
    inpatient_visits              INTEGER DEFAULT 0,
    outpatient_visits             INTEGER DEFAULT 0,
    emergency_visits              INTEGER DEFAULT 0,

    -- Condition metrics
    total_conditions              INTEGER DEFAULT 0,
    active_conditions             INTEGER DEFAULT 0,
    unique_condition_concepts     INTEGER DEFAULT 0,
    chronic_condition_count       INTEGER DEFAULT 0,

    -- Medication metrics
    total_drug_exposures          INTEGER DEFAULT 0,
    active_drug_exposures         INTEGER DEFAULT 0,
    unique_drug_concepts          INTEGER DEFAULT 0,
    polypharmacy_flag             BOOLEAN DEFAULT FALSE,

    -- Procedure metrics
    total_procedures              INTEGER DEFAULT 0,

    -- Measurement metrics
    total_measurements            INTEGER DEFAULT 0,

    -- Utilization metrics
    first_visit_date              DATE,
    last_visit_date               DATE,
    observation_years             DECIMAL(5,2),
    visits_per_year               DECIMAL(8,2),
    avg_days_between_visits       DECIMAL(8,2),

    -- 30-day readmission flag (for supervised ML)
    had_30_day_readmission        BOOLEAN DEFAULT FALSE,

    -- ML clustering output (populated by ml_clustering.py)
    cluster_id                    INTEGER,
    cluster_label                 VARCHAR(100),

    -- ML risk scoring output (populated by ml_risk_scoring.py)
    risk_score                    DECIMAL(5,4),
    risk_tier                     VARCHAR(20),

    loaded_at                     TIMESTAMP DEFAULT GETDATE(),
    updated_at                    TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_key)
SORTKEY (risk_score, cluster_id, total_encounters);


-- ════════════════════════════════════════════════════════
-- COMORBIDITY ANALYSIS TABLE (populated by ML pipeline)
-- ════════════════════════════════════════════════════════

CREATE TABLE comorbidity_analysis (
    condition_concept_id_1    INTEGER NOT NULL,
    condition_concept_id_2    INTEGER NOT NULL,
    concept_name_1            VARCHAR(500),
    concept_name_2            VARCHAR(500),
    co_occurrence_count       INTEGER NOT NULL,
    patient_count_1           INTEGER,
    patient_count_2           INTEGER,
    support                   DECIMAL(8,6),
    lift                      DECIMAL(8,4),
    loaded_at                 TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (condition_concept_id_1, condition_concept_id_2)
)
DISTSTYLE ALL
SORTKEY (co_occurrence_count);


-- ════════════════════════════════════════════════════════
-- ANALYTICAL VIEWS
-- ════════════════════════════════════════════════════════

-- Patient segments summary (for cluster dashboards)
CREATE OR REPLACE VIEW vw_patient_segments AS
SELECT
    cluster_id,
    cluster_label,
    COUNT(*)                          AS patient_count,
    ROUND(AVG(age), 1)                AS avg_age,
    ROUND(AVG(total_encounters), 1)   AS avg_encounters,
    ROUND(AVG(total_conditions), 1)   AS avg_conditions,
    ROUND(AVG(total_drug_exposures), 1) AS avg_drug_exposures,
    ROUND(AVG(CAST(risk_score AS FLOAT)), 4) AS avg_risk_score
FROM fact_patient_metrics
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id, cluster_label
ORDER BY cluster_id;

-- Top conditions by frequency
CREATE OR REPLACE VIEW vw_top_conditions AS
SELECT
    fc.condition_concept_id,
    dc.concept_name,
    dc.body_system,
    dc.chronicity,
    COUNT(*)                             AS occurrence_count,
    COUNT(DISTINCT fc.patient_key)       AS unique_patients,
    ROUND(AVG(CASE WHEN fc.is_active THEN 1.0 ELSE 0.0 END), 3) AS pct_active
FROM fact_conditions fc
LEFT JOIN dim_condition dc ON fc.condition_concept_id = dc.condition_concept_id
GROUP BY fc.condition_concept_id, dc.concept_name, dc.body_system, dc.chronicity
ORDER BY occurrence_count DESC
LIMIT 50;

-- Encounter trends by month
CREATE OR REPLACE VIEW vw_encounter_trends AS
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    fe.visit_class,
    COUNT(*)                           AS encounter_count,
    COUNT(DISTINCT fe.patient_key)     AS unique_patients
FROM fact_encounters fe
JOIN dim_date dd ON fe.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name, fe.visit_class
ORDER BY dd.year, dd.month, fe.visit_class;

-- Risk tier distribution
CREATE OR REPLACE VIEW vw_risk_distribution AS
SELECT
    risk_tier,
    COUNT(*)                              AS patient_count,
    ROUND(AVG(age), 1)                    AS avg_age,
    ROUND(AVG(total_encounters), 1)       AS avg_encounters,
    ROUND(AVG(chronic_condition_count), 1) AS avg_chronic_conditions,
    ROUND(AVG(CAST(risk_score AS FLOAT)), 4) AS avg_risk_score
FROM fact_patient_metrics
WHERE risk_tier IS NOT NULL
GROUP BY risk_tier
ORDER BY avg_risk_score DESC;

-- Condition comorbidity pairs (from ML-generated table)
CREATE OR REPLACE VIEW vw_condition_comorbidity AS
SELECT
    condition_concept_id_1,
    condition_concept_id_2,
    concept_name_1,
    concept_name_2,
    co_occurrence_count,
    support,
    lift
FROM comorbidity_analysis
ORDER BY co_occurrence_count DESC
LIMIT 50;

-- Polypharmacy patients
CREATE OR REPLACE VIEW vw_polypharmacy AS
SELECT
    pm.patient_key,
    dp.age,
    dp.gender,
    pm.active_drug_exposures,
    pm.active_conditions,
    pm.risk_tier
FROM fact_patient_metrics pm
JOIN dim_patient dp ON pm.patient_key = dp.patient_key
WHERE pm.polypharmacy_flag = TRUE
ORDER BY pm.active_drug_exposures DESC;


-- ════════════════════════════════════════════════════════
-- POPULATE dim_date (2000-01-01 to 2030-12-31)
-- ════════════════════════════════════════════════════════
-- Run once after table creation. Uses a recursive CTE to generate dates.

INSERT INTO dim_date (date_key, full_date, year, quarter, month, month_name,
                      week_of_year, day_of_month, day_of_week, day_name, is_weekend)
WITH RECURSIVE date_series AS (
    SELECT '2000-01-01'::DATE AS d
    UNION ALL
    SELECT DATEADD(day, 1, d) FROM date_series WHERE d < '2030-12-31'
)
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'), '99999999')   AS date_key,
    d                                                 AS full_date,
    EXTRACT(YEAR FROM d)                              AS year,
    EXTRACT(QUARTER FROM d)                           AS quarter,
    EXTRACT(MONTH FROM d)                             AS month,
    TO_CHAR(d, 'Month')                               AS month_name,
    EXTRACT(WEEK FROM d)                              AS week_of_year,
    EXTRACT(DAY FROM d)                               AS day_of_month,
    EXTRACT(DOW FROM d)                               AS day_of_week,
    TO_CHAR(d, 'Day')                                 AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series;

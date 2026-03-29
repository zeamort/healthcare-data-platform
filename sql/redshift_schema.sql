-- Redshift Data Warehouse Schema — Analytics (OLAP)
-- Kimball star schema with dimension and fact tables optimized for
-- healthcare analytics, patient segmentation, and ML pipelines.

-- Drop in dependency order
DROP TABLE IF EXISTS fact_patient_metrics CASCADE;
DROP TABLE IF EXISTS fact_medications CASCADE;
DROP TABLE IF EXISTS fact_conditions CASCADE;
DROP TABLE IF EXISTS fact_encounters CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_medication CASCADE;
DROP TABLE IF EXISTS dim_condition CASCADE;
DROP TABLE IF EXISTS dim_patient CASCADE;

-- ════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════════════════════════

-- ── dim_patient ─────────────────────────────────────────
-- Type 1 SCD — overwrites on update.
-- Contains demographics and derived attributes for slicing/filtering.

CREATE TABLE dim_patient (
    patient_key       INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_id        VARCHAR(255) NOT NULL UNIQUE,

    -- Demographics
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    gender            VARCHAR(1),
    race              VARCHAR(50),
    ethnicity         VARCHAR(50),
    marital_status    VARCHAR(1),

    -- Birth / death
    birthdate         DATE NOT NULL,
    deathdate         DATE,
    is_deceased       BOOLEAN DEFAULT FALSE,

    -- Derived age attributes
    age               INTEGER,
    age_group         VARCHAR(20),   -- '0-17','18-34','35-49','50-64','65-79','80+'

    -- Geography
    state             VARCHAR(50),
    city              VARCHAR(100),
    county            VARCHAR(100),
    zip               VARCHAR(10),

    -- Socioeconomic
    income            INTEGER,
    income_bracket    VARCHAR(20),   -- 'low','middle','high'

    -- PII sensitivity classification
    -- SSN, drivers, passport, address are intentionally excluded from the
    -- warehouse. They live only in the operational RDS database and are never
    -- exposed through analytics or API responses (ADR-006: data minimization).

    loaded_at         TIMESTAMP DEFAULT GETDATE(),
    updated_at        TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (patient_id, age_group, state);


-- ── dim_condition ───────────────────────────────────────
-- SNOMED-CT condition codes with healthcare-relevant categorization.

CREATE TABLE dim_condition (
    condition_key     INTEGER IDENTITY(1,1) PRIMARY KEY,
    code              VARCHAR(50) NOT NULL UNIQUE,
    description       VARCHAR(500),

    -- Clinical categorization
    body_system       VARCHAR(100),  -- e.g. 'Cardiovascular','Respiratory','Musculoskeletal'
    chronicity        VARCHAR(20),   -- 'acute','chronic','episodic'
    severity_tier     VARCHAR(20),   -- 'mild','moderate','severe' (derived from code ranges)

    loaded_at         TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL          -- small table, replicate to every node for fast joins
SORTKEY (code);


-- ── dim_medication ──────────────────────────────────────
-- Medication codes with therapeutic classification.

CREATE TABLE dim_medication (
    medication_key    INTEGER IDENTITY(1,1) PRIMARY KEY,
    code              VARCHAR(50) NOT NULL UNIQUE,
    description       VARCHAR(500),

    -- Therapeutic categorization
    therapeutic_class VARCHAR(100),  -- e.g. 'Analgesic','Antihypertensive','Antibiotic'

    loaded_at         TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE ALL
SORTKEY (code);


-- ── dim_date ────────────────────────────────────────────
-- Calendar dimension for time-series analytics.
-- Pre-populated from 2000-01-01 to 2030-12-31.

CREATE TABLE dim_date (
    date_key          INTEGER PRIMARY KEY,            -- YYYYMMDD integer
    full_date         DATE NOT NULL UNIQUE,
    year              SMALLINT NOT NULL,
    quarter           SMALLINT NOT NULL,              -- 1-4
    month             SMALLINT NOT NULL,              -- 1-12
    month_name        VARCHAR(10) NOT NULL,           -- 'January'
    week_of_year      SMALLINT NOT NULL,
    day_of_month      SMALLINT NOT NULL,
    day_of_week       SMALLINT NOT NULL,              -- 0=Mon, 6=Sun
    day_name          VARCHAR(10) NOT NULL,            -- 'Monday'
    is_weekend        BOOLEAN NOT NULL
)
DISTSTYLE ALL
SORTKEY (full_date);


-- ════════════════════════════════════════════════════════
-- FACT TABLES
-- ════════════════════════════════════════════════════════

-- ── fact_encounters ─────────────────────────────────────
-- Grain: one row per healthcare encounter.

CREATE TABLE fact_encounters (
    encounter_key         INTEGER IDENTITY(1,1) PRIMARY KEY,
    encounter_id          VARCHAR(255) NOT NULL,
    patient_id            VARCHAR(255) NOT NULL,        -- FK → dim_patient
    date_key              INTEGER,                      -- FK → dim_date

    -- Encounter attributes
    encounter_class       VARCHAR(50),
    code                  VARCHAR(50),
    description           VARCHAR(500),

    -- Timestamps
    start_time            TIMESTAMP NOT NULL,
    stop_time             TIMESTAMP,
    duration_minutes      INTEGER,

    -- Cost measures
    base_encounter_cost   DECIMAL(10,2),
    total_claim_cost      DECIMAL(10,2),
    payer_coverage        DECIMAL(10,2),
    patient_out_of_pocket DECIMAL(10,2),

    -- Reason (if referral/follow-up)
    reason_code           VARCHAR(50),
    reason_description    VARCHAR(500),

    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (start_time, patient_id);


-- ── fact_conditions ─────────────────────────────────────
-- Grain: one row per diagnosed condition event.

CREATE TABLE fact_conditions (
    condition_key_id      INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_id            VARCHAR(255) NOT NULL,        -- FK → dim_patient
    encounter_id          VARCHAR(255),
    condition_code        VARCHAR(50),                  -- FK → dim_condition
    date_key              INTEGER,                      -- FK → dim_date

    start_date            DATE NOT NULL,
    stop_date             DATE,
    is_active             BOOLEAN DEFAULT TRUE,
    duration_days         INTEGER,

    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (start_date, patient_id, condition_code);


-- ── fact_medications ────────────────────────────────────
-- Grain: one row per prescription event.

CREATE TABLE fact_medications (
    medication_key_id     INTEGER IDENTITY(1,1) PRIMARY KEY,
    patient_id            VARCHAR(255) NOT NULL,        -- FK → dim_patient
    encounter_id          VARCHAR(255),
    medication_code       VARCHAR(50),                  -- FK → dim_medication
    date_key              INTEGER,                      -- FK → dim_date

    start_date            DATE NOT NULL,
    stop_date             DATE,
    is_active             BOOLEAN DEFAULT TRUE,
    duration_days         INTEGER,
    dispenses             INTEGER,

    -- Cost measures
    base_cost             DECIMAL(10,2),
    payer_coverage        DECIMAL(10,2),
    total_cost            DECIMAL(10,2),

    loaded_at             TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (start_date, patient_id, medication_code);


-- ── fact_patient_metrics ────────────────────────────────
-- Pre-aggregated patient-level metrics for ML and dashboards.
-- Rebuilt by the analytics ETL after each data load.

CREATE TABLE fact_patient_metrics (
    patient_id                VARCHAR(255) PRIMARY KEY,

    -- Demographics (denormalized for ML feature vectors)
    age                       INTEGER,
    gender                    VARCHAR(1),
    state                     VARCHAR(50),
    income                    INTEGER,

    -- Encounter metrics
    total_encounters          INTEGER DEFAULT 0,
    wellness_encounters       INTEGER DEFAULT 0,
    emergency_encounters      INTEGER DEFAULT 0,
    inpatient_encounters      INTEGER DEFAULT 0,
    outpatient_encounters     INTEGER DEFAULT 0,
    ambulatory_encounters     INTEGER DEFAULT 0,
    urgentcare_encounters     INTEGER DEFAULT 0,

    -- Condition metrics
    total_conditions          INTEGER DEFAULT 0,
    active_conditions         INTEGER DEFAULT 0,
    chronic_condition_count   INTEGER DEFAULT 0,

    -- Medication metrics
    total_medications         INTEGER DEFAULT 0,
    active_medications        INTEGER DEFAULT 0,
    unique_medication_codes   INTEGER DEFAULT 0,
    polypharmacy_flag         BOOLEAN DEFAULT FALSE,    -- 5+ concurrent medications

    -- Cost metrics
    total_encounter_cost      DECIMAL(12,2) DEFAULT 0,
    avg_encounter_cost        DECIMAL(10,2) DEFAULT 0,
    total_medication_cost     DECIMAL(12,2) DEFAULT 0,
    total_out_of_pocket       DECIMAL(12,2) DEFAULT 0,

    -- Utilization metrics
    first_encounter_date      DATE,
    last_encounter_date       DATE,
    years_in_system           DECIMAL(5,2),
    encounters_per_year       DECIMAL(8,2),
    avg_days_between_encounters DECIMAL(8,2),

    -- 30-day readmission flag (for supervised ML)
    had_30_day_readmission    BOOLEAN DEFAULT FALSE,

    -- ML clustering output (populated by ml_clustering.py)
    cluster_id                INTEGER,
    cluster_label             VARCHAR(100),

    -- ML risk scoring output (populated by ml_risk_scoring.py)
    risk_score                DECIMAL(5,4),              -- 0.0000 – 1.0000
    risk_tier                 VARCHAR(20),               -- 'low','medium','high','very_high'

    loaded_at                 TIMESTAMP DEFAULT GETDATE(),
    updated_at                TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (patient_id)
SORTKEY (risk_score, cluster_id, total_encounters);


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
    ROUND(AVG(total_medications), 1)  AS avg_medications,
    ROUND(AVG(total_encounter_cost), 2) AS avg_cost,
    ROUND(SUM(total_encounter_cost), 2) AS total_segment_cost,
    ROUND(AVG(risk_score), 4)         AS avg_risk_score
FROM fact_patient_metrics
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id, cluster_label
ORDER BY cluster_id;

-- Top conditions by frequency
CREATE OR REPLACE VIEW vw_top_conditions AS
SELECT
    fc.condition_code           AS code,
    dc.description,
    dc.body_system,
    dc.chronicity,
    COUNT(*)                    AS occurrence_count,
    COUNT(DISTINCT fc.patient_id) AS unique_patients,
    ROUND(AVG(CASE WHEN fc.is_active THEN 1.0 ELSE 0.0 END), 3) AS pct_active
FROM fact_conditions fc
LEFT JOIN dim_condition dc ON fc.condition_code = dc.code
GROUP BY fc.condition_code, dc.description, dc.body_system, dc.chronicity
ORDER BY occurrence_count DESC
LIMIT 50;

-- Encounter trends by month
CREATE OR REPLACE VIEW vw_encounter_trends AS
SELECT
    dd.year,
    dd.month,
    dd.month_name,
    fe.encounter_class,
    COUNT(*)                           AS encounter_count,
    COUNT(DISTINCT fe.patient_id)      AS unique_patients,
    ROUND(AVG(fe.total_claim_cost), 2) AS avg_cost,
    ROUND(SUM(fe.total_claim_cost), 2) AS total_cost
FROM fact_encounters fe
JOIN dim_date dd ON fe.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name, fe.encounter_class
ORDER BY dd.year, dd.month, fe.encounter_class;

-- Medication utilization summary
CREATE OR REPLACE VIEW vw_medication_summary AS
SELECT
    fm.medication_code              AS code,
    dm.description,
    dm.therapeutic_class,
    COUNT(*)                        AS prescription_count,
    COUNT(DISTINCT fm.patient_id)   AS unique_patients,
    ROUND(SUM(fm.total_cost), 2)    AS total_cost,
    ROUND(AVG(fm.total_cost), 2)    AS avg_cost_per_rx,
    SUM(fm.dispenses)               AS total_dispenses
FROM fact_medications fm
LEFT JOIN dim_medication dm ON fm.medication_code = dm.code
GROUP BY fm.medication_code, dm.description, dm.therapeutic_class
ORDER BY prescription_count DESC
LIMIT 50;

-- Risk tier distribution
CREATE OR REPLACE VIEW vw_risk_distribution AS
SELECT
    risk_tier,
    COUNT(*)                              AS patient_count,
    ROUND(AVG(age), 1)                    AS avg_age,
    ROUND(AVG(total_encounters), 1)       AS avg_encounters,
    ROUND(AVG(chronic_condition_count), 1) AS avg_chronic_conditions,
    ROUND(AVG(total_encounter_cost), 2)   AS avg_cost,
    ROUND(AVG(risk_score), 4)             AS avg_risk_score
FROM fact_patient_metrics
WHERE risk_tier IS NOT NULL
GROUP BY risk_tier
ORDER BY avg_risk_score DESC;

-- Disease co-occurrence (top pairs)
CREATE OR REPLACE VIEW vw_condition_comorbidity AS
SELECT
    a.condition_code AS condition_1,
    b.condition_code AS condition_2,
    dc1.description  AS description_1,
    dc2.description  AS description_2,
    COUNT(DISTINCT a.patient_id) AS shared_patients
FROM fact_conditions a
JOIN fact_conditions b
    ON a.patient_id = b.patient_id
    AND a.condition_code < b.condition_code
LEFT JOIN dim_condition dc1 ON a.condition_code = dc1.code
LEFT JOIN dim_condition dc2 ON b.condition_code = dc2.code
GROUP BY a.condition_code, b.condition_code, dc1.description, dc2.description
HAVING COUNT(DISTINCT a.patient_id) >= 5
ORDER BY shared_patients DESC
LIMIT 50;

-- Polypharmacy patients
CREATE OR REPLACE VIEW vw_polypharmacy AS
SELECT
    pm.patient_id,
    dp.first_name,
    dp.last_name,
    dp.age,
    pm.active_medications,
    pm.active_conditions,
    pm.total_encounter_cost,
    pm.risk_tier
FROM fact_patient_metrics pm
JOIN dim_patient dp ON pm.patient_id = dp.patient_id
WHERE pm.polypharmacy_flag = TRUE
ORDER BY pm.active_medications DESC;


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

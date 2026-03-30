-- RDS PostgreSQL Schema — OMOP Common Data Model (CDM) v5.4
-- Industry-standard clinical data model for observational health data.
-- Source: Synthea OMOP dataset from AWS Open Data (s3://synthea-omop/)

-- ── DROP EXISTING TABLES ──────────────────────────────────

DROP TABLE IF EXISTS drug_era CASCADE;
DROP TABLE IF EXISTS condition_era CASCADE;
DROP TABLE IF EXISTS observation CASCADE;
DROP TABLE IF EXISTS measurement CASCADE;
DROP TABLE IF EXISTS procedure_occurrence CASCADE;
DROP TABLE IF EXISTS drug_exposure CASCADE;
DROP TABLE IF EXISTS condition_occurrence CASCADE;
DROP TABLE IF EXISTS visit_occurrence CASCADE;
DROP TABLE IF EXISTS observation_period CASCADE;
DROP TABLE IF EXISTS person CASCADE;
DROP TABLE IF EXISTS concept CASCADE;

-- ── CONCEPT (Vocabulary Lookup) ───────────────────────────
-- Minimal concept table for resolving concept_id to human-readable names.
-- Populated by ETL from OMOP vocabulary or built from source data.

CREATE TABLE concept (
    concept_id        INTEGER PRIMARY KEY,
    concept_name      VARCHAR(500) NOT NULL,
    domain_id         VARCHAR(50),
    vocabulary_id     VARCHAR(50),
    concept_class_id  VARCHAR(50),
    concept_code      VARCHAR(100),
    valid_start_date  DATE,
    valid_end_date    DATE
);

CREATE INDEX idx_concept_domain ON concept(domain_id);
CREATE INDEX idx_concept_vocabulary ON concept(vocabulary_id);
CREATE INDEX idx_concept_code ON concept(concept_code);

-- ── PERSON ────────────────────────────────────────────────

CREATE TABLE person (
    person_id                   BIGINT PRIMARY KEY,
    gender_concept_id           INTEGER NOT NULL,
    year_of_birth               INTEGER NOT NULL,
    month_of_birth              INTEGER,
    day_of_birth                INTEGER,
    birth_datetime              DATE,
    race_concept_id             INTEGER NOT NULL,
    ethnicity_concept_id        INTEGER NOT NULL DEFAULT 0,
    location_id                 BIGINT,
    provider_id                 BIGINT,
    care_site_id                BIGINT,
    person_source_value         VARCHAR(255),
    gender_source_value         VARCHAR(50),
    gender_source_concept_id    INTEGER DEFAULT 0,
    race_source_value           VARCHAR(100),
    race_source_concept_id      INTEGER DEFAULT 0,
    ethnicity_source_value      VARCHAR(100),
    ethnicity_source_concept_id INTEGER DEFAULT 0
);

CREATE INDEX idx_person_gender ON person(gender_concept_id);
CREATE INDEX idx_person_race ON person(race_concept_id);
CREATE INDEX idx_person_year ON person(year_of_birth);

-- ── OBSERVATION PERIOD ────────────────────────────────────

CREATE TABLE observation_period (
    observation_period_id         BIGINT PRIMARY KEY,
    person_id                     BIGINT NOT NULL,
    observation_period_start_date DATE NOT NULL,
    observation_period_end_date   DATE NOT NULL,
    period_type_concept_id        INTEGER NOT NULL,
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE
);

CREATE INDEX idx_obs_period_person ON observation_period(person_id);

-- ── VISIT OCCURRENCE ──────────────────────────────────────

CREATE TABLE visit_occurrence (
    visit_occurrence_id           BIGINT PRIMARY KEY,
    person_id                     BIGINT NOT NULL,
    visit_concept_id              INTEGER NOT NULL,
    visit_start_date              DATE NOT NULL,
    visit_start_datetime          TIMESTAMP,
    visit_end_date                DATE,
    visit_end_datetime            TIMESTAMP,
    visit_type_concept_id         INTEGER NOT NULL,
    provider_id                   BIGINT,
    care_site_id                  BIGINT,
    visit_source_value            VARCHAR(255),
    visit_source_concept_id       INTEGER DEFAULT 0,
    admitting_source_concept_id   INTEGER DEFAULT 0,
    admitting_source_value        VARCHAR(255),
    discharge_to_concept_id       INTEGER DEFAULT 0,
    discharge_to_source_value     VARCHAR(255),
    preceding_visit_occurrence_id BIGINT,
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE
);

CREATE INDEX idx_visit_person ON visit_occurrence(person_id);
CREATE INDEX idx_visit_concept ON visit_occurrence(visit_concept_id);
CREATE INDEX idx_visit_start ON visit_occurrence(visit_start_date);

-- ── CONDITION OCCURRENCE ──────────────────────────────────

CREATE TABLE condition_occurrence (
    condition_occurrence_id       BIGINT PRIMARY KEY,
    person_id                     BIGINT NOT NULL,
    condition_concept_id          INTEGER NOT NULL,
    condition_start_date          DATE NOT NULL,
    condition_start_datetime      TIMESTAMP,
    condition_end_date            DATE,
    condition_end_datetime        TIMESTAMP,
    condition_type_concept_id     INTEGER NOT NULL,
    stop_reason                   VARCHAR(255),
    provider_id                   BIGINT,
    visit_occurrence_id           BIGINT,
    visit_detail_id               BIGINT,
    condition_source_value        VARCHAR(255),
    condition_source_concept_id   INTEGER DEFAULT 0,
    condition_status_source_value VARCHAR(255),
    condition_status_concept_id   INTEGER DEFAULT 0,
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_occurrence_id) REFERENCES visit_occurrence(visit_occurrence_id) ON DELETE SET NULL
);

CREATE INDEX idx_condition_person ON condition_occurrence(person_id);
CREATE INDEX idx_condition_concept ON condition_occurrence(condition_concept_id);
CREATE INDEX idx_condition_visit ON condition_occurrence(visit_occurrence_id);
CREATE INDEX idx_condition_start ON condition_occurrence(condition_start_date);

-- ── DRUG EXPOSURE ─────────────────────────────────────────

CREATE TABLE drug_exposure (
    drug_exposure_id            BIGINT PRIMARY KEY,
    person_id                   BIGINT NOT NULL,
    drug_concept_id             INTEGER NOT NULL,
    drug_exposure_start_date    DATE NOT NULL,
    drug_exposure_start_datetime TIMESTAMP,
    drug_exposure_end_date      DATE,
    drug_exposure_end_datetime  TIMESTAMP,
    verbatim_end_date           DATE,
    drug_type_concept_id        INTEGER NOT NULL,
    stop_reason                 VARCHAR(255),
    refills                     INTEGER,
    quantity                    NUMERIC,
    days_supply                 INTEGER,
    sig                         TEXT,
    route_concept_id            INTEGER DEFAULT 0,
    lot_number                  VARCHAR(100),
    provider_id                 BIGINT,
    visit_occurrence_id         BIGINT,
    visit_detail_id             BIGINT,
    drug_source_value           VARCHAR(255),
    drug_source_concept_id      INTEGER DEFAULT 0,
    route_source_value          VARCHAR(255),
    dose_unit_source_value      VARCHAR(255),
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_occurrence_id) REFERENCES visit_occurrence(visit_occurrence_id) ON DELETE SET NULL
);

CREATE INDEX idx_drug_person ON drug_exposure(person_id);
CREATE INDEX idx_drug_concept ON drug_exposure(drug_concept_id);
CREATE INDEX idx_drug_visit ON drug_exposure(visit_occurrence_id);
CREATE INDEX idx_drug_start ON drug_exposure(drug_exposure_start_date);

-- ── PROCEDURE OCCURRENCE ──────────────────────────────────

CREATE TABLE procedure_occurrence (
    procedure_occurrence_id     BIGINT PRIMARY KEY,
    person_id                   BIGINT NOT NULL,
    procedure_concept_id        INTEGER NOT NULL,
    procedure_date              DATE NOT NULL,
    procedure_datetime          TIMESTAMP,
    procedure_type_concept_id   INTEGER NOT NULL,
    modifier_concept_id         INTEGER DEFAULT 0,
    quantity                    INTEGER,
    provider_id                 BIGINT,
    visit_occurrence_id         BIGINT,
    visit_detail_id             BIGINT,
    procedure_source_value      VARCHAR(255),
    procedure_source_concept_id INTEGER DEFAULT 0,
    modifier_source_value       VARCHAR(255),
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_occurrence_id) REFERENCES visit_occurrence(visit_occurrence_id) ON DELETE SET NULL
);

CREATE INDEX idx_procedure_person ON procedure_occurrence(person_id);
CREATE INDEX idx_procedure_concept ON procedure_occurrence(procedure_concept_id);
CREATE INDEX idx_procedure_visit ON procedure_occurrence(visit_occurrence_id);
CREATE INDEX idx_procedure_date ON procedure_occurrence(procedure_date);

-- ── MEASUREMENT ───────────────────────────────────────────

CREATE TABLE measurement (
    measurement_id              BIGINT PRIMARY KEY,
    person_id                   BIGINT NOT NULL,
    measurement_concept_id      INTEGER NOT NULL,
    measurement_date            DATE NOT NULL,
    measurement_datetime        TIMESTAMP,
    measurement_time            VARCHAR(20),
    measurement_type_concept_id INTEGER NOT NULL,
    operator_concept_id         INTEGER DEFAULT 0,
    value_as_number             NUMERIC,
    value_as_concept_id         INTEGER DEFAULT 0,
    unit_concept_id             INTEGER DEFAULT 0,
    range_low                   NUMERIC,
    range_high                  NUMERIC,
    provider_id                 BIGINT,
    visit_occurrence_id         BIGINT,
    visit_detail_id             BIGINT,
    measurement_source_value    VARCHAR(255),
    measurement_source_concept_id INTEGER DEFAULT 0,
    unit_source_value           VARCHAR(255),
    value_source_value          VARCHAR(255),
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_occurrence_id) REFERENCES visit_occurrence(visit_occurrence_id) ON DELETE SET NULL
);

CREATE INDEX idx_measurement_person ON measurement(person_id);
CREATE INDEX idx_measurement_concept ON measurement(measurement_concept_id);
CREATE INDEX idx_measurement_visit ON measurement(visit_occurrence_id);
CREATE INDEX idx_measurement_date ON measurement(measurement_date);

-- ── OBSERVATION ───────────────────────────────────────────

CREATE TABLE observation (
    observation_id              BIGINT PRIMARY KEY,
    person_id                   BIGINT NOT NULL,
    observation_concept_id      INTEGER NOT NULL,
    observation_date            DATE NOT NULL,
    observation_datetime        TIMESTAMP,
    observation_type_concept_id INTEGER NOT NULL,
    value_as_number             NUMERIC,
    value_as_string             TEXT,
    value_as_concept_id         INTEGER DEFAULT 0,
    qualifier_concept_id        INTEGER DEFAULT 0,
    unit_concept_id             INTEGER DEFAULT 0,
    provider_id                 BIGINT,
    visit_occurrence_id         BIGINT,
    visit_detail_id             BIGINT,
    observation_source_value    VARCHAR(255),
    observation_source_concept_id INTEGER DEFAULT 0,
    unit_source_value           VARCHAR(255),
    qualifier_source_value      VARCHAR(255),
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE,
    FOREIGN KEY (visit_occurrence_id) REFERENCES visit_occurrence(visit_occurrence_id) ON DELETE SET NULL
);

CREATE INDEX idx_observation_person ON observation(person_id);
CREATE INDEX idx_observation_concept ON observation(observation_concept_id);
CREATE INDEX idx_observation_visit ON observation(visit_occurrence_id);

-- ── CONDITION ERA (Derived) ───────────────────────────────

CREATE TABLE condition_era (
    condition_era_id          BIGINT PRIMARY KEY,
    person_id                 BIGINT NOT NULL,
    condition_concept_id      INTEGER NOT NULL,
    condition_era_start_date  DATE NOT NULL,
    condition_era_end_date    DATE NOT NULL,
    condition_occurrence_count INTEGER,
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE
);

CREATE INDEX idx_condition_era_person ON condition_era(person_id);
CREATE INDEX idx_condition_era_concept ON condition_era(condition_concept_id);

-- ── DRUG ERA (Derived) ────────────────────────────────────

CREATE TABLE drug_era (
    drug_era_id          BIGINT PRIMARY KEY,
    person_id            BIGINT NOT NULL,
    drug_concept_id      INTEGER NOT NULL,
    drug_era_start_date  DATE NOT NULL,
    drug_era_end_date    DATE NOT NULL,
    drug_exposure_count  INTEGER,
    gap_days             INTEGER,
    FOREIGN KEY (person_id) REFERENCES person(person_id) ON DELETE CASCADE
);

CREATE INDEX idx_drug_era_person ON drug_era(person_id);
CREATE INDEX idx_drug_era_concept ON drug_era(drug_concept_id);

-- ── VIEWS ─────────────────────────────────────────────────

CREATE OR REPLACE VIEW person_summary AS
SELECT
    p.person_id,
    p.gender_source_value AS gender,
    p.race_source_value AS race,
    p.ethnicity_source_value AS ethnicity,
    p.year_of_birth,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birth_datetime)) AS age,
    op.observation_period_start_date,
    op.observation_period_end_date,
    COUNT(DISTINCT v.visit_occurrence_id) AS total_visits,
    COUNT(DISTINCT co.condition_occurrence_id) AS total_conditions,
    COUNT(DISTINCT de.drug_exposure_id) AS total_drug_exposures
FROM person p
LEFT JOIN observation_period op ON p.person_id = op.person_id
LEFT JOIN visit_occurrence v ON p.person_id = v.person_id
LEFT JOIN condition_occurrence co ON p.person_id = co.person_id
LEFT JOIN drug_exposure de ON p.person_id = de.person_id
GROUP BY p.person_id, p.gender_source_value, p.race_source_value,
         p.ethnicity_source_value, p.year_of_birth, p.birth_datetime,
         op.observation_period_start_date, op.observation_period_end_date;

CREATE OR REPLACE VIEW active_conditions AS
SELECT
    co.person_id,
    co.condition_concept_id,
    co.condition_source_value,
    c.concept_name AS condition_name,
    co.condition_start_date,
    co.condition_end_date
FROM condition_occurrence co
LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
WHERE co.condition_end_date IS NULL OR co.condition_end_date > CURRENT_DATE;

CREATE OR REPLACE VIEW data_quality_check AS
SELECT 'Visits without person' AS check_name, COUNT(*) AS issue_count
FROM visit_occurrence v LEFT JOIN person p ON v.person_id = p.person_id WHERE p.person_id IS NULL
UNION ALL
SELECT 'Conditions without person', COUNT(*)
FROM condition_occurrence co LEFT JOIN person p ON co.person_id = p.person_id WHERE p.person_id IS NULL
UNION ALL
SELECT 'Drug exposures without person', COUNT(*)
FROM drug_exposure de LEFT JOIN person p ON de.person_id = p.person_id WHERE p.person_id IS NULL
UNION ALL
SELECT 'Measurements without person', COUNT(*)
FROM measurement m LEFT JOIN person p ON m.person_id = p.person_id WHERE p.person_id IS NULL
UNION ALL
SELECT 'Visits with unknown concept', COUNT(*)
FROM visit_occurrence v WHERE v.visit_concept_id = 0
UNION ALL
SELECT 'Conditions with unknown concept', COUNT(*)
FROM condition_occurrence co WHERE co.condition_concept_id = 0
UNION ALL
SELECT 'Drug exposures with unknown concept', COUNT(*)
FROM drug_exposure de WHERE de.drug_concept_id = 0;

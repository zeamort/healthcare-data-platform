-- RDS PostgreSQL Schema — Operational Database (OLTP)
-- Normalized tables for transactional queries with referential integrity.

DROP TABLE IF EXISTS medications CASCADE;
DROP TABLE IF EXISTS conditions CASCADE;
DROP TABLE IF EXISTS encounters CASCADE;
DROP TABLE IF EXISTS patients CASCADE;

-- ── PATIENTS ────────────────────────────────────────────

CREATE TABLE patients (
    id VARCHAR(255) PRIMARY KEY,
    birthdate DATE NOT NULL,
    deathdate DATE,
    ssn VARCHAR(11),
    drivers VARCHAR(20),
    passport VARCHAR(20),
    prefix VARCHAR(10),
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    suffix VARCHAR(10),
    maiden VARCHAR(100),
    marital VARCHAR(1),
    race VARCHAR(50),
    ethnicity VARCHAR(50),
    gender VARCHAR(1),
    birthplace VARCHAR(255),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    county VARCHAR(100),
    fips VARCHAR(10),
    zip VARCHAR(10),
    lat DECIMAL(18, 14),
    lon DECIMAL(18, 14),
    healthcare_expenses DECIMAL(12, 2),
    healthcare_coverage DECIMAL(12, 2),
    income INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_patients_birthdate ON patients(birthdate);
CREATE INDEX idx_patients_state ON patients(state);
CREATE INDEX idx_patients_gender ON patients(gender);
CREATE INDEX idx_patients_race ON patients(race);

-- ── ENCOUNTERS ──────────────────────────────────────────

CREATE TABLE encounters (
    id VARCHAR(255) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    stop_time TIMESTAMP,
    patient_id VARCHAR(255) NOT NULL,
    organization_id VARCHAR(255),
    provider_id VARCHAR(255),
    payer_id VARCHAR(255),
    encounter_class VARCHAR(50),
    code VARCHAR(50),
    description VARCHAR(500),
    base_encounter_cost DECIMAL(10, 2),
    total_claim_cost DECIMAL(10, 2),
    payer_coverage DECIMAL(10, 2),
    reason_code VARCHAR(50),
    reason_description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE
);

CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_start ON encounters(start_time);
CREATE INDEX idx_encounters_class ON encounters(encounter_class);

-- ── CONDITIONS ──────────────────────────────────────────

CREATE TABLE conditions (
    id SERIAL PRIMARY KEY,
    start_date DATE NOT NULL,
    stop_date DATE,
    patient_id VARCHAR(255) NOT NULL,
    encounter_id VARCHAR(255),
    system VARCHAR(255),
    code VARCHAR(50),
    description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE CASCADE
);

CREATE INDEX idx_conditions_patient ON conditions(patient_id);
CREATE INDEX idx_conditions_encounter ON conditions(encounter_id);
CREATE INDEX idx_conditions_code ON conditions(code);

-- ── MEDICATIONS ─────────────────────────────────────────

CREATE TABLE medications (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    stop_time TIMESTAMP,
    patient_id VARCHAR(255) NOT NULL,
    payer_id VARCHAR(255),
    encounter_id VARCHAR(255),
    code VARCHAR(50),
    description VARCHAR(500),
    base_cost DECIMAL(10, 2),
    payer_coverage DECIMAL(10, 2),
    dispenses INTEGER,
    total_cost DECIMAL(10, 2),
    reason_code VARCHAR(50),
    reason_description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES patients(id) ON DELETE CASCADE,
    FOREIGN KEY (encounter_id) REFERENCES encounters(id) ON DELETE CASCADE
);

CREATE INDEX idx_medications_patient ON medications(patient_id);
CREATE INDEX idx_medications_encounter ON medications(encounter_id);
CREATE INDEX idx_medications_code ON medications(code);

-- ── VIEWS ───────────────────────────────────────────────

CREATE OR REPLACE VIEW patient_summary AS
SELECT
    p.id,
    p.first_name,
    p.last_name,
    p.gender,
    p.race,
    p.ethnicity,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, p.birthdate)) AS age,
    p.state,
    p.city,
    COUNT(DISTINCT e.id) AS total_encounters,
    COUNT(DISTINCT c.id) AS total_conditions,
    COUNT(DISTINCT m.id) AS total_medications,
    COALESCE(SUM(e.total_claim_cost), 0) AS total_healthcare_cost
FROM patients p
LEFT JOIN encounters e ON p.id = e.patient_id
LEFT JOIN conditions c ON p.id = c.patient_id
LEFT JOIN medications m ON p.id = m.patient_id
GROUP BY p.id, p.first_name, p.last_name, p.gender, p.race,
         p.ethnicity, p.birthdate, p.state, p.city;

CREATE OR REPLACE VIEW active_conditions AS
SELECT
    p.id AS patient_id,
    p.first_name,
    p.last_name,
    c.code,
    c.description,
    c.start_date,
    c.stop_date
FROM patients p
JOIN conditions c ON p.id = c.patient_id
WHERE c.stop_date IS NULL OR c.stop_date > CURRENT_DATE;

CREATE OR REPLACE VIEW data_quality_check AS
SELECT 'Encounters without patients' AS check_name, COUNT(*) AS issue_count
FROM encounters e LEFT JOIN patients p ON e.patient_id = p.id WHERE p.id IS NULL
UNION ALL
SELECT 'Conditions without patients', COUNT(*)
FROM conditions c LEFT JOIN patients p ON c.patient_id = p.id WHERE p.id IS NULL
UNION ALL
SELECT 'Medications without patients', COUNT(*)
FROM medications m LEFT JOIN patients p ON m.patient_id = p.id WHERE p.id IS NULL;

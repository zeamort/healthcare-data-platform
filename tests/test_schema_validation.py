"""
Schema and data validation tests.

Validates that SQL schemas conform to design requirements:
- OMOP CDM compliance for RDS
- De-identification enforcement for Redshift (no person_id in fact tables)
- Proper constraints and indexes
- Kimball star schema structure

These tests parse the SQL files directly — no database connection needed.
"""

import os
import re
import pytest

SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")
RDS_SCHEMA = os.path.join(SCHEMA_DIR, "rds_schema.sql")
REDSHIFT_SCHEMA = os.path.join(SCHEMA_DIR, "redshift_schema.sql")


def read_schema(path):
    with open(path, "r") as f:
        return f.read()


# ── RDS Schema (OMOP CDM) ───────────────────────────────

class TestRDSSchema:
    """Validate the RDS schema conforms to OMOP CDM requirements."""

    @pytest.fixture(autouse=True)
    def load_schema(self):
        self.sql = read_schema(RDS_SCHEMA)

    def test_person_table_exists(self):
        assert "CREATE TABLE person" in self.sql

    def test_person_has_required_columns(self):
        required = [
            "person_id", "gender_concept_id", "year_of_birth",
            "race_concept_id", "ethnicity_concept_id",
        ]
        for col in required:
            assert col in self.sql, f"Missing OMOP required column: {col}"

    def test_visit_occurrence_table_exists(self):
        assert "CREATE TABLE visit_occurrence" in self.sql

    def test_visit_occurrence_has_required_columns(self):
        required = [
            "visit_occurrence_id", "person_id", "visit_concept_id",
            "visit_start_date", "visit_type_concept_id",
        ]
        for col in required:
            assert col in self.sql, f"Missing column: {col}"

    def test_condition_occurrence_table_exists(self):
        assert "CREATE TABLE condition_occurrence" in self.sql

    def test_drug_exposure_table_exists(self):
        assert "CREATE TABLE drug_exposure" in self.sql

    def test_procedure_occurrence_table_exists(self):
        assert "CREATE TABLE procedure_occurrence" in self.sql

    def test_measurement_table_exists(self):
        assert "CREATE TABLE measurement" in self.sql

    def test_observation_table_exists(self):
        assert "CREATE TABLE observation" in self.sql

    def test_concept_table_exists(self):
        assert "CREATE TABLE concept" in self.sql

    def test_observation_period_table_exists(self):
        assert "CREATE TABLE observation_period" in self.sql

    def test_condition_era_table_exists(self):
        assert "CREATE TABLE condition_era" in self.sql

    def test_drug_era_table_exists(self):
        assert "CREATE TABLE drug_era" in self.sql

    def test_person_primary_key(self):
        assert "person_id" in self.sql
        # Check person_id is BIGINT PRIMARY KEY
        person_section = self.sql[self.sql.index("CREATE TABLE person"):
                                  self.sql.index("CREATE TABLE observation_period")]
        assert "BIGINT PRIMARY KEY" in person_section

    def test_foreign_keys_to_person(self):
        """Clinical tables should have FK to person."""
        fk_tables = [
            "visit_occurrence", "condition_occurrence",
            "drug_exposure", "procedure_occurrence",
            "measurement", "observation",
        ]
        for table in fk_tables:
            table_start = self.sql.find(f"CREATE TABLE {table}")
            assert table_start != -1, f"Table {table} not found"
            table_section = self.sql[table_start:self.sql.find(";", table_start)]
            assert "REFERENCES person(person_id)" in table_section, \
                f"{table} missing FK to person"

    def test_indexes_exist(self):
        """Key indexes should be present for query performance."""
        expected_indexes = [
            "idx_person_gender",
            "idx_visit_person",
            "idx_condition_person",
            "idx_drug_person",
            "idx_measurement_person",
        ]
        for idx in expected_indexes:
            assert idx in self.sql, f"Missing index: {idx}"

    def test_data_quality_view_exists(self):
        assert "data_quality_check" in self.sql

    def test_person_summary_view_exists(self):
        assert "person_summary" in self.sql


# ── Redshift Schema (Kimball Star) ──────────────────────

class TestRedshiftSchema:
    """Validate the Redshift schema follows Kimball conventions."""

    @pytest.fixture(autouse=True)
    def load_schema(self):
        self.sql = read_schema(REDSHIFT_SCHEMA)

    def test_dim_patient_exists(self):
        assert "CREATE TABLE dim_patient" in self.sql

    def test_dim_condition_exists(self):
        assert "CREATE TABLE dim_condition" in self.sql

    def test_dim_medication_exists(self):
        assert "CREATE TABLE dim_medication" in self.sql

    def test_dim_procedure_exists(self):
        assert "CREATE TABLE dim_procedure" in self.sql

    def test_dim_date_exists(self):
        assert "CREATE TABLE dim_date" in self.sql

    def test_fact_encounters_exists(self):
        assert "CREATE TABLE fact_encounters" in self.sql

    def test_fact_conditions_exists(self):
        assert "CREATE TABLE fact_conditions" in self.sql

    def test_fact_medications_exists(self):
        assert "CREATE TABLE fact_medications" in self.sql

    def test_fact_procedures_exists(self):
        assert "CREATE TABLE fact_procedures" in self.sql

    def test_fact_patient_metrics_exists(self):
        assert "CREATE TABLE fact_patient_metrics" in self.sql

    def test_comorbidity_analysis_exists(self):
        assert "CREATE TABLE comorbidity_analysis" in self.sql

    def test_dim_patient_has_surrogate_key(self):
        """dim_patient must have an auto-generated patient_key."""
        section = self._get_table_section("dim_patient")
        assert "patient_key" in section
        assert "IDENTITY" in section

    def test_dim_date_populated(self):
        """Schema should include INSERT INTO dim_date."""
        assert "INSERT INTO dim_date" in self.sql

    def test_analytical_views_exist(self):
        views = [
            "vw_patient_segments",
            "vw_top_conditions",
            "vw_encounter_trends",
            "vw_risk_distribution",
            "vw_condition_comorbidity",
            "vw_polypharmacy",
        ]
        for view in views:
            assert view in self.sql, f"Missing view: {view}"

    def _get_table_section(self, table_name):
        """Extract the CREATE TABLE section for a given table."""
        start = self.sql.find(f"CREATE TABLE {table_name}")
        if start == -1:
            return ""
        end = self.sql.find(";", start)
        return self.sql[start:end]


# ── De-identification Enforcement ────────────────────────

class TestDeIdentification:
    """
    Critical security tests: verify that person_id does NOT appear
    in Redshift fact tables, views, or patient metrics.

    This enforces the de-identification design — Redshift uses only
    surrogate patient_key, never the operational person_id.
    """

    @pytest.fixture(autouse=True)
    def load_schema(self):
        self.sql = read_schema(REDSHIFT_SCHEMA)

    def _get_table_section(self, table_name):
        start = self.sql.find(f"CREATE TABLE {table_name}")
        if start == -1:
            return ""
        end = self.sql.find(";", start)
        return self.sql[start:end]

    def _get_view_section(self, view_name):
        start = self.sql.find(view_name)
        if start == -1:
            return ""
        end = self.sql.find(";", start)
        return self.sql[start:end]

    def test_fact_encounters_no_person_id(self):
        section = self._get_table_section("fact_encounters")
        assert "person_id" not in section, \
            "fact_encounters must use patient_key, not person_id"

    def test_fact_conditions_no_person_id(self):
        section = self._get_table_section("fact_conditions")
        assert "person_id" not in section, \
            "fact_conditions must use patient_key, not person_id"

    def test_fact_medications_no_person_id(self):
        section = self._get_table_section("fact_medications")
        assert "person_id" not in section, \
            "fact_medications must use patient_key, not person_id"

    def test_fact_procedures_no_person_id(self):
        section = self._get_table_section("fact_procedures")
        assert "person_id" not in section, \
            "fact_procedures must use patient_key, not person_id"

    def test_fact_patient_metrics_no_person_id(self):
        section = self._get_table_section("fact_patient_metrics")
        assert "person_id" not in section, \
            "fact_patient_metrics must use patient_key, not person_id"

    def test_fact_encounters_has_patient_key(self):
        section = self._get_table_section("fact_encounters")
        assert "patient_key" in section

    def test_fact_conditions_has_patient_key(self):
        section = self._get_table_section("fact_conditions")
        assert "patient_key" in section

    def test_fact_medications_has_patient_key(self):
        section = self._get_table_section("fact_medications")
        assert "patient_key" in section

    def test_fact_procedures_has_patient_key(self):
        section = self._get_table_section("fact_procedures")
        assert "patient_key" in section

    def test_fact_patient_metrics_has_patient_key(self):
        section = self._get_table_section("fact_patient_metrics")
        assert "patient_key" in section

    def test_views_no_person_id(self):
        """No analytical view should expose person_id."""
        views = [
            "vw_patient_segments",
            "vw_top_conditions",
            "vw_encounter_trends",
            "vw_risk_distribution",
            "vw_condition_comorbidity",
            "vw_polypharmacy",
        ]
        for view in views:
            section = self._get_view_section(view)
            assert "person_id" not in section, \
                f"View {view} must not expose person_id"

    def test_dim_patient_is_only_table_with_person_id(self):
        """person_id should only appear in dim_patient (for ETL loading)."""
        # Find all CREATE TABLE blocks
        tables = re.findall(r"CREATE TABLE (\w+)", self.sql)

        for table in tables:
            if table == "dim_patient":
                continue
            section = self._get_table_section(table)
            assert "person_id" not in section, \
                f"Table {table} should not contain person_id (only dim_patient may)"


# ── ML Script De-identification ──────────────────────────

class TestMLDeIdentification:
    """Verify ML scripts use patient_key, not person_id."""

    ML_DIR = os.path.join(os.path.dirname(__file__), "..", "etl")

    def _read_file(self, filename):
        with open(os.path.join(self.ML_DIR, filename), "r") as f:
            return f.read()

    def test_clustering_no_person_id(self):
        code = self._read_file("ml_clustering.py")
        assert "person_id" not in code, \
            "ml_clustering.py must use patient_key, not person_id"

    def test_risk_scoring_no_person_id(self):
        code = self._read_file("ml_risk_scoring.py")
        assert "person_id" not in code, \
            "ml_risk_scoring.py must use patient_key, not person_id"

    def test_comorbidity_no_person_id(self):
        code = self._read_file("ml_comorbidity.py")
        assert "person_id" not in code, \
            "ml_comorbidity.py must use patient_key, not person_id"

    def test_dashboard_no_person_id(self):
        dashboard_path = os.path.join(os.path.dirname(__file__), "..", "dashboard", "app.py")
        with open(dashboard_path, "r") as f:
            code = f.read()
        assert "person_id" not in code, \
            "Dashboard must not reference person_id"


# ── Terraform Security ───────────────────────────────────

class TestTerraformSecurity:
    """Validate Terraform configs for security best practices."""

    TF_DIR = os.path.join(os.path.dirname(__file__), "..", "terraform")

    def _read_tf(self, filename):
        path = os.path.join(self.TF_DIR, filename)
        if not os.path.exists(path):
            pytest.skip(f"{filename} not found")
        with open(path, "r") as f:
            return f.read()

    def test_s3_bucket_exists(self):
        """Terraform should define an S3 bucket."""
        # Check across all .tf files
        for f in os.listdir(self.TF_DIR):
            if f.endswith(".tf"):
                content = self._read_tf(f)
                if "aws_s3_bucket" in content:
                    return
        pytest.fail("No S3 bucket resource found in Terraform")

    def test_rds_in_private_subnet(self):
        """RDS should reference private subnets."""
        for f in os.listdir(self.TF_DIR):
            if f.endswith(".tf"):
                content = self._read_tf(f)
                if "aws_db_instance" in content and "private" in content:
                    return
        pytest.fail("RDS should be in private subnets")

    def test_kinesis_stream_defined(self):
        content = self._read_tf("kinesis.tf")
        assert "aws_kinesis_stream" in content

    def test_iam_least_privilege(self):
        """IAM policy should scope resources, not use wildcard for data access."""
        content = self._read_tf("iam.tf")
        # S3 access should be scoped to specific bucket
        assert "aws_s3_bucket.data.arn" in content, \
            "S3 access should be scoped to the data bucket ARN"

    def test_dashboard_ecs_defined(self):
        content = self._read_tf("dashboard.tf")
        assert "aws_ecs_service" in content
        assert "aws_ecs_task_definition" in content

    def test_dashboard_not_public_ip(self):
        """ECS tasks should not have public IPs (ALB handles traffic)."""
        content = self._read_tf("dashboard.tf")
        assert "assign_public_ip = false" in content or \
               "assign_public_ip  = false" in content

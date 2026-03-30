"""
Unit tests for ETL helper functions.

Tests the data cleaning, classification, and transformation functions
used across the ETL pipelines. No database connection required.
"""

import sys
import os
import pytest

# Add etl/ to path so we can import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl"))


# ── S3-to-RDS ETL helpers ───────────────────────────────

class TestCleanFunctions:
    """Tests for etl_s3_to_rds.clean, clean_int, clean_numeric, clean_date."""

    def setup_method(self):
        # Import without triggering module-level env var reads
        import importlib
        self.mod = importlib.import_module("etl_s3_to_rds")

    def test_clean_empty_string(self):
        assert self.mod.clean("") is None

    def test_clean_null_string(self):
        assert self.mod.clean("NULL") is None

    def test_clean_none(self):
        assert self.mod.clean(None) is None

    def test_clean_valid_string(self):
        assert self.mod.clean("hello") == "hello"

    def test_clean_whitespace_preserved(self):
        assert self.mod.clean("  spaces  ") == "  spaces  "

    def test_clean_int_valid(self):
        assert self.mod.clean_int("42") == 42

    def test_clean_int_float_string(self):
        # int("42.7") raises ValueError, so clean_int returns None
        assert self.mod.clean_int("42.7") is None

    def test_clean_int_none(self):
        assert self.mod.clean_int(None) is None

    def test_clean_int_empty(self):
        assert self.mod.clean_int("") is None

    def test_clean_int_invalid(self):
        assert self.mod.clean_int("abc") is None

    def test_clean_int_null_string(self):
        assert self.mod.clean_int("NULL") is None

    def test_clean_numeric_valid(self):
        assert self.mod.clean_numeric("3.14") == pytest.approx(3.14)

    def test_clean_numeric_integer(self):
        assert self.mod.clean_numeric("100") == pytest.approx(100.0)

    def test_clean_numeric_none(self):
        assert self.mod.clean_numeric(None) is None

    def test_clean_numeric_empty(self):
        assert self.mod.clean_numeric("") is None

    def test_clean_numeric_invalid(self):
        assert self.mod.clean_numeric("abc") is None

    def test_clean_date_valid(self):
        from datetime import date
        result = self.mod.clean_date("2020-06-15")
        assert result == date(2020, 6, 15)

    def test_clean_date_none(self):
        assert self.mod.clean_date(None) is None

    def test_clean_date_empty(self):
        assert self.mod.clean_date("") is None

    def test_clean_date_invalid(self):
        assert self.mod.clean_date("not-a-date") is None


# ── RDS-to-Redshift ETL helpers ──────────────────────────

class TestAgeGroup:
    """Tests for etl_rds_to_redshift.age_group."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_child(self):
        assert self.mod.age_group(5) == "0-17"

    def test_teenager(self):
        assert self.mod.age_group(17) == "0-17"

    def test_young_adult(self):
        assert self.mod.age_group(18) == "18-34"

    def test_adult_boundary(self):
        assert self.mod.age_group(34) == "18-34"

    def test_middle_age(self):
        assert self.mod.age_group(45) == "35-49"

    def test_senior_boundary(self):
        assert self.mod.age_group(65) == "65-79"

    def test_elderly(self):
        assert self.mod.age_group(85) == "80+"

    def test_none(self):
        assert self.mod.age_group(None) is None

    def test_zero(self):
        assert self.mod.age_group(0) == "0-17"

    def test_exact_boundaries(self):
        assert self.mod.age_group(49) == "35-49"
        assert self.mod.age_group(50) == "50-64"
        assert self.mod.age_group(64) == "50-64"
        assert self.mod.age_group(79) == "65-79"
        assert self.mod.age_group(80) == "80+"


class TestClassifyBodySystem:
    """Tests for body system classification."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_cardiovascular(self):
        assert self.mod.classify_body_system("Coronary artery disease") == "Cardiovascular"

    def test_respiratory(self):
        assert self.mod.classify_body_system("Asthma") == "Respiratory"

    def test_endocrine(self):
        assert self.mod.classify_body_system("Diabetes mellitus type 2") == "Endocrine"

    def test_neurological(self):
        assert self.mod.classify_body_system("Alzheimer's disease") == "Neurological"

    def test_mental_health(self):
        assert self.mod.classify_body_system("Major depression") == "Mental Health"

    def test_oncology(self):
        assert self.mod.classify_body_system("Breast carcinoma") == "Oncology"

    def test_musculoskeletal(self):
        assert self.mod.classify_body_system("Osteoarthritis") == "Musculoskeletal"

    def test_gastrointestinal(self):
        assert self.mod.classify_body_system("Gastroesophageal reflux disease") == "Gastrointestinal"

    def test_renal(self):
        assert self.mod.classify_body_system("Chronic kidney disease") == "Renal"

    def test_infectious(self):
        assert self.mod.classify_body_system("Viral infection") == "Infectious"

    def test_other(self):
        assert self.mod.classify_body_system("Some unknown condition") == "Other"

    def test_none(self):
        assert self.mod.classify_body_system(None) == "Other"

    def test_case_insensitive(self):
        assert self.mod.classify_body_system("HYPERTENSION") == "Cardiovascular"


class TestClassifyChronicity:
    """Tests for chronic vs acute classification."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_chronic_diabetes(self):
        assert self.mod.classify_chronicity("Diabetes mellitus") == "chronic"

    def test_chronic_hypertension(self):
        assert self.mod.classify_chronicity("Essential hypertension") == "chronic"

    def test_chronic_asthma(self):
        assert self.mod.classify_chronicity("Asthma") == "chronic"

    def test_acute(self):
        assert self.mod.classify_chronicity("Acute appendicitis") == "acute"

    def test_acute_unknown(self):
        assert self.mod.classify_chronicity("Some random condition") == "acute"

    def test_none(self):
        assert self.mod.classify_chronicity(None) == "acute"


class TestClassifySeverity:
    """Tests for severity classification."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_severe_cancer(self):
        assert self.mod.classify_severity("Lung cancer") == "severe"

    def test_severe_stroke(self):
        assert self.mod.classify_severity("Ischemic stroke") == "severe"

    def test_moderate_chronic(self):
        assert self.mod.classify_severity("Diabetes mellitus") == "moderate"

    def test_mild(self):
        assert self.mod.classify_severity("Common cold") == "mild"

    def test_none(self):
        assert self.mod.classify_severity(None) == "mild"


class TestClassifyTherapeuticClass:
    """Tests for medication therapeutic class classification."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_analgesic(self):
        assert self.mod.classify_therapeutic_class("Acetaminophen 500mg") == "Analgesic"

    def test_antibiotic(self):
        assert self.mod.classify_therapeutic_class("Amoxicillin 250mg") == "Antibiotic"

    def test_statin(self):
        assert self.mod.classify_therapeutic_class("Simvastatin 20mg") == "Statin"

    def test_antihypertensive(self):
        assert self.mod.classify_therapeutic_class("Lisinopril 10mg") == "Antihypertensive"

    def test_vaccine(self):
        assert self.mod.classify_therapeutic_class("Influenza vaccine") == "Vaccine"

    def test_opioid(self):
        assert self.mod.classify_therapeutic_class("Oxycodone 5mg") == "Opioid"

    def test_other(self):
        assert self.mod.classify_therapeutic_class("Some generic med") == "Other"

    def test_none(self):
        assert self.mod.classify_therapeutic_class(None) == "Other"


class TestClassifyProcedureCategory:
    """Tests for procedure category classification."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("etl_rds_to_redshift")

    def test_diagnostic(self):
        assert self.mod.classify_procedure_category("Cancer screening") == "Diagnostic"

    def test_imaging(self):
        assert self.mod.classify_procedure_category("CT scan of abdomen") == "Imaging"

    def test_surgical(self):
        assert self.mod.classify_procedure_category("Hip replacement surgery") == "Surgical"

    def test_preventive(self):
        assert self.mod.classify_procedure_category("Annual vaccination") == "Preventive"

    def test_laboratory(self):
        assert self.mod.classify_procedure_category("Blood panel analysis") == "Laboratory"

    def test_therapeutic(self):
        assert self.mod.classify_procedure_category("Chemotherapy treatment") == "Therapeutic"

    def test_other(self):
        assert self.mod.classify_procedure_category("Something else") == "Other"

    def test_none(self):
        assert self.mod.classify_procedure_category(None) == "Other"


# ── Stream consumer helpers ──────────────────────────────

class TestStreamConsumerClean:
    """Tests for stream_consumer clean functions."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("stream_consumer")

    def test_clean_empty(self):
        assert self.mod.clean("") is None

    def test_clean_null_string(self):
        assert self.mod.clean("NULL") is None

    def test_clean_valid(self):
        assert self.mod.clean("value") == "value"

    def test_clean_int_valid(self):
        assert self.mod.clean_int("123") == 123

    def test_clean_int_none(self):
        assert self.mod.clean_int(None) is None

    def test_clean_int_invalid(self):
        assert self.mod.clean_int("abc") is None

    def test_clean_numeric_valid(self):
        assert self.mod.clean_numeric("2.5") == pytest.approx(2.5)

    def test_clean_numeric_none(self):
        assert self.mod.clean_numeric(None) is None


# ── Risk scoring helpers ─────────────────────────────────

class TestRiskTier:
    """Tests for ml_risk_scoring.risk_tier."""

    def setup_method(self):
        import importlib
        self.mod = importlib.import_module("ml_risk_scoring")

    def test_low(self):
        assert self.mod.risk_tier(0.10) == "low"

    def test_low_boundary(self):
        assert self.mod.risk_tier(0.24) == "low"

    def test_medium(self):
        assert self.mod.risk_tier(0.25) == "medium"

    def test_medium_upper(self):
        assert self.mod.risk_tier(0.49) == "medium"

    def test_high(self):
        assert self.mod.risk_tier(0.50) == "high"

    def test_high_upper(self):
        assert self.mod.risk_tier(0.74) == "high"

    def test_very_high(self):
        assert self.mod.risk_tier(0.75) == "very_high"

    def test_very_high_max(self):
        assert self.mod.risk_tier(1.0) == "very_high"

    def test_zero(self):
        assert self.mod.risk_tier(0.0) == "low"

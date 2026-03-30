"""Analytics endpoints — aggregate statistics from OMOP operational data."""

from fastapi import APIRouter
from db import get_connection, put_connection

router = APIRouter()


@router.get("/overview")
def platform_overview():
    """Get high-level platform statistics."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        stats = {}
        tables = [
            "person", "visit_occurrence", "condition_occurrence",
            "drug_exposure", "procedure_occurrence", "measurement",
            "observation",
        ]
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[f"total_{table}"] = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COUNT(DISTINCT gender_source_value) AS gender_count,
                COUNT(DISTINCT race_source_value) AS race_count,
                MIN(year_of_birth) AS oldest_birth_year,
                MAX(year_of_birth) AS youngest_birth_year
            FROM person
        """)
        row = cur.fetchone()
        stats["distinct_genders"] = row[0]
        stats["distinct_races"] = row[1]
        stats["oldest_birth_year"] = row[2]
        stats["youngest_birth_year"] = row[3]

        return stats
    finally:
        cur.close()
        put_connection(conn)


@router.get("/demographics")
def demographics():
    """Person demographics breakdown."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Gender distribution
        cur.execute("""
            SELECT gender_source_value AS gender, COUNT(*) AS count
            FROM person GROUP BY gender_source_value ORDER BY count DESC
        """)
        gender = [dict(zip(["gender", "count"], r)) for r in cur.fetchall()]

        # Race distribution
        cur.execute("""
            SELECT race_source_value AS race, COUNT(*) AS count
            FROM person GROUP BY race_source_value ORDER BY count DESC
        """)
        race = [dict(zip(["race", "count"], r)) for r in cur.fetchall()]

        # Ethnicity distribution
        cur.execute("""
            SELECT ethnicity_source_value AS ethnicity, COUNT(*) AS count
            FROM person GROUP BY ethnicity_source_value ORDER BY count DESC
        """)
        ethnicity = [dict(zip(["ethnicity", "count"], r)) for r in cur.fetchall()]

        # Age distribution
        cur.execute("""
            SELECT
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 17 THEN '0-17'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 34 THEN '18-34'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 49 THEN '35-49'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 64 THEN '50-64'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_datetime)) <= 79 THEN '65-79'
                    ELSE '80+'
                END AS age_group,
                COUNT(*) AS count
            FROM person
            WHERE birth_datetime IS NOT NULL
            GROUP BY age_group
            ORDER BY age_group
        """)
        age = [dict(zip(["age_group", "count"], r)) for r in cur.fetchall()]

        return {
            "by_gender": gender,
            "by_race": race,
            "by_ethnicity": ethnicity,
            "by_age_group": age,
        }
    finally:
        cur.close()
        put_connection(conn)


@router.get("/visit-types")
def visit_type_breakdown():
    """Visit volume by type (concept)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT COALESCE(c.concept_name, 'Unknown') AS visit_type,
                   vo.visit_concept_id,
                   COUNT(*) AS count,
                   COUNT(DISTINCT vo.person_id) AS unique_patients
            FROM visit_occurrence vo
            LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
            GROUP BY c.concept_name, vo.visit_concept_id
            ORDER BY count DESC
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"visit_types": rows}
    finally:
        cur.close()
        put_connection(conn)


@router.get("/data-quality")
def data_quality():
    """Run data quality checks."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM data_quality_check")
        checks = [{"check": r[0], "issue_count": r[1]} for r in cur.fetchall()]
        all_pass = all(c["issue_count"] == 0 for c in checks)
        return {"status": "pass" if all_pass else "issues_found", "checks": checks}
    finally:
        cur.close()
        put_connection(conn)

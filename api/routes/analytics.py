"""Analytics endpoints — aggregate statistics from operational data."""

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
        for table in ["patients", "encounters", "conditions", "medications"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[f"total_{table}"] = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COUNT(DISTINCT gender) AS gender_count,
                COUNT(DISTINCT state) AS state_count,
                MIN(birthdate) AS oldest_birthdate,
                MAX(birthdate) AS youngest_birthdate
            FROM patients
        """)
        row = cur.fetchone()
        stats["distinct_genders"] = row[0]
        stats["distinct_states"] = row[1]
        stats["oldest_birthdate"] = row[2]
        stats["youngest_birthdate"] = row[3]

        return stats
    finally:
        cur.close()
        put_connection(conn)


@router.get("/demographics")
def demographics():
    """Patient demographics breakdown."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Gender distribution
        cur.execute("""
            SELECT gender, COUNT(*) AS count
            FROM patients GROUP BY gender ORDER BY count DESC
        """)
        gender = [dict(zip(["gender", "count"], r)) for r in cur.fetchall()]

        # Race distribution
        cur.execute("""
            SELECT race, COUNT(*) AS count
            FROM patients GROUP BY race ORDER BY count DESC
        """)
        race = [dict(zip(["race", "count"], r)) for r in cur.fetchall()]

        # State distribution (top 10)
        cur.execute("""
            SELECT state, COUNT(*) AS count
            FROM patients GROUP BY state ORDER BY count DESC LIMIT 10
        """)
        state = [dict(zip(["state", "count"], r)) for r in cur.fetchall()]

        # Age distribution
        cur.execute("""
            SELECT
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) <= 17 THEN '0-17'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) <= 34 THEN '18-34'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) <= 49 THEN '35-49'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) <= 64 THEN '50-64'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, birthdate)) <= 79 THEN '65-79'
                    ELSE '80+'
                END AS age_group,
                COUNT(*) AS count
            FROM patients
            GROUP BY age_group
            ORDER BY age_group
        """)
        age = [dict(zip(["age_group", "count"], r)) for r in cur.fetchall()]

        return {
            "by_gender": gender,
            "by_race": race,
            "by_state": state,
            "by_age_group": age,
        }
    finally:
        cur.close()
        put_connection(conn)


@router.get("/encounter-classes")
def encounter_class_breakdown():
    """Encounter volume by class."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT encounter_class,
                   COUNT(*) AS count,
                   COUNT(DISTINCT patient_id) AS unique_patients,
                   COALESCE(AVG(total_claim_cost), 0) AS avg_cost
            FROM encounters
            GROUP BY encounter_class
            ORDER BY count DESC
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"encounter_classes": rows}
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

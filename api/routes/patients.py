"""Patient endpoints — demographics and summary data (PII excluded)."""

from fastapi import APIRouter, HTTPException, Query
from db import get_connection, put_connection

router = APIRouter()

# Columns safe to return (SSN, drivers, passport, full address excluded)
SAFE_COLUMNS = """
    id, first_name, last_name, gender, race, ethnicity,
    birthdate, deathdate, marital, city, state, county, zip,
    healthcare_expenses, healthcare_coverage, income
"""


@router.get("")
def list_patients(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    state: str = Query(None, description="Filter by state"),
    gender: str = Query(None, description="Filter by gender (M/F)"),
):
    """List patients with pagination and optional filters."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if state:
            where_clauses.append("state = %s")
            params.append(state)
        if gender:
            where_clauses.append("gender = %s")
            params.append(gender)

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM patients {where}", params)
        total = cur.fetchone()[0]

        cur.execute(
            f"SELECT {SAFE_COLUMNS} FROM patients {where} ORDER BY last_name, first_name LIMIT %s OFFSET %s",
            params + [per_page, offset],
        )
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]

        return {
            "data": rows,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page,
            },
        }
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{patient_id}")
def get_patient(patient_id: str):
    """Get a single patient by ID."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {SAFE_COLUMNS} FROM patients WHERE id = %s", (patient_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{patient_id}/summary")
def get_patient_summary(patient_id: str):
    """Get aggregated summary for a patient (encounters, conditions, costs)."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM patient_summary WHERE id = %s", (patient_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{patient_id}/encounters")
def get_patient_encounters(patient_id: str):
    """Get all encounters for a patient."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        # Verify patient exists
        cur.execute("SELECT id FROM patients WHERE id = %s", (patient_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")

        cur.execute("""
            SELECT id, start_time, stop_time, encounter_class, code,
                   description, base_encounter_cost, total_claim_cost, payer_coverage,
                   reason_code, reason_description
            FROM encounters
            WHERE patient_id = %s
            ORDER BY start_time DESC
        """, (patient_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"patient_id": patient_id, "count": len(rows), "encounters": rows}
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{patient_id}/conditions")
def get_patient_conditions(patient_id: str):
    """Get all conditions for a patient."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM patients WHERE id = %s", (patient_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")

        cur.execute("""
            SELECT code, description, start_date, stop_date,
                   CASE WHEN stop_date IS NULL OR stop_date > CURRENT_DATE
                        THEN true ELSE false END AS is_active
            FROM conditions
            WHERE patient_id = %s
            ORDER BY start_date DESC
        """, (patient_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"patient_id": patient_id, "count": len(rows), "conditions": rows}
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{patient_id}/medications")
def get_patient_medications(patient_id: str):
    """Get all medications for a patient."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM patients WHERE id = %s", (patient_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Patient {patient_id} not found")

        cur.execute("""
            SELECT code, description, start_time, stop_time,
                   base_cost, payer_coverage, total_cost, dispenses,
                   reason_code, reason_description
            FROM medications
            WHERE patient_id = %s
            ORDER BY start_time DESC
        """, (patient_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"patient_id": patient_id, "count": len(rows), "medications": rows}
    finally:
        cur.close()
        put_connection(conn)

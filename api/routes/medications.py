"""Medication endpoints — search prescriptions by code, patient, or description."""

from fastapi import APIRouter, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def search_medications(
    patient_id: str = Query(None, description="Filter by patient ID"),
    code: str = Query(None, description="Filter by medication code"),
    description: str = Query(None, description="Search description (case-insensitive, partial match)"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Search medications with optional filters."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if patient_id:
            where_clauses.append("m.patient_id = %s")
            params.append(patient_id)
        if code:
            where_clauses.append("m.code = %s")
            params.append(code)
        if description:
            where_clauses.append("m.description ILIKE %s")
            params.append(f"%{description}%")

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM medications m {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT m.patient_id, m.code, m.description,
                   m.start_time, m.stop_time,
                   m.base_cost, m.payer_coverage, m.total_cost, m.dispenses,
                   p.first_name, p.last_name
            FROM medications m
            JOIN patients p ON m.patient_id = p.id
            {where}
            ORDER BY m.start_time DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
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


@router.get("/summary")
def medications_summary():
    """Get top medications by prescription frequency."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT code, description,
                   COUNT(*) AS prescription_count,
                   COUNT(DISTINCT patient_id) AS unique_patients,
                   COALESCE(SUM(total_cost), 0) AS total_cost,
                   COALESCE(AVG(total_cost), 0) AS avg_cost_per_rx
            FROM medications
            GROUP BY code, description
            ORDER BY prescription_count DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"top_medications": rows}
    finally:
        cur.close()
        put_connection(conn)

"""Condition endpoints — search diagnoses by code or description."""

from fastapi import APIRouter, HTTPException, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def search_conditions(
    code: str = Query(None, description="Filter by SNOMED-CT code"),
    description: str = Query(None, description="Search description (case-insensitive, partial match)"),
    active_only: bool = Query(False, description="Only return active (ongoing) conditions"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Search conditions by code or description."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if code:
            where_clauses.append("c.code = %s")
            params.append(code)
        if description:
            where_clauses.append("c.description ILIKE %s")
            params.append(f"%{description}%")
        if active_only:
            where_clauses.append("(c.stop_date IS NULL OR c.stop_date > CURRENT_DATE)")

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM conditions c {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT c.patient_id, c.code, c.description, c.start_date, c.stop_date,
                   p.first_name, p.last_name
            FROM conditions c
            JOIN patients p ON c.patient_id = p.id
            {where}
            ORDER BY c.start_date DESC
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
def conditions_summary():
    """Get top conditions by frequency."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT code, description,
                   COUNT(*) AS occurrence_count,
                   COUNT(DISTINCT patient_id) AS unique_patients
            FROM conditions
            GROUP BY code, description
            ORDER BY occurrence_count DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"top_conditions": rows}
    finally:
        cur.close()
        put_connection(conn)

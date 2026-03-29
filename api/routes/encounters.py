"""Encounter endpoints — search and filter healthcare visits."""

from fastapi import APIRouter, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def list_encounters(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    encounter_class: str = Query(None, description="Filter by class (wellness, emergency, inpatient, outpatient, ambulatory, urgentcare)"),
):
    """List encounters with pagination and optional class filter."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where = ""
        params = []
        if encounter_class:
            where = "WHERE encounter_class = %s"
            params.append(encounter_class)

        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM encounters {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT id, start_time, stop_time, patient_id, encounter_class,
                   code, description, total_claim_cost
            FROM encounters {where}
            ORDER BY start_time DESC
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

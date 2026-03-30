"""Visit occurrence endpoints — search and filter healthcare visits (OMOP CDM)."""

from fastapi import APIRouter, Query, Depends
from db import get_connection, put_connection
from auth import require_role

router = APIRouter()


@router.get("")
def list_visits(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    visit_concept_id: int = Query(None, description="Filter by visit concept ID (9201=Inpatient, 9202=Outpatient, 9203=ER)"),
    user=Depends(require_role("analyst")),
):
    """List visit occurrences with pagination and optional concept filter."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where = ""
        params = []
        if visit_concept_id:
            where = "WHERE vo.visit_concept_id = %s"
            params.append(visit_concept_id)

        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM visit_occurrence vo {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT vo.visit_occurrence_id, vo.person_id,
                   vo.visit_start_date, vo.visit_end_date,
                   vo.visit_concept_id,
                   COALESCE(c.concept_name, 'Unknown') AS visit_class
            FROM visit_occurrence vo
            LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
            {where}
            ORDER BY vo.visit_start_date DESC
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

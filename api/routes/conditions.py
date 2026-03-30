"""Condition occurrence endpoints — search diagnoses (OMOP CDM)."""

from fastapi import APIRouter, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def search_conditions(
    condition_concept_id: int = Query(None, description="Filter by condition concept ID"),
    source_value: str = Query(None, description="Search by source value (SNOMED code, partial match)"),
    active_only: bool = Query(False, description="Only return active (ongoing) conditions"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Search condition occurrences with optional filters."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if condition_concept_id:
            where_clauses.append("co.condition_concept_id = %s")
            params.append(condition_concept_id)
        if source_value:
            where_clauses.append("co.condition_source_value ILIKE %s")
            params.append(f"%{source_value}%")
        if active_only:
            where_clauses.append("(co.condition_end_date IS NULL OR co.condition_end_date > CURRENT_DATE)")

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM condition_occurrence co {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT co.condition_occurrence_id, co.person_id,
                   co.condition_concept_id,
                   COALESCE(c.concept_name, co.condition_source_value) AS condition_name,
                   co.condition_source_value,
                   co.condition_start_date, co.condition_end_date
            FROM condition_occurrence co
            LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
            {where}
            ORDER BY co.condition_start_date DESC
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
            SELECT co.condition_concept_id,
                   COALESCE(c.concept_name, co.condition_source_value) AS condition_name,
                   co.condition_source_value,
                   COUNT(*) AS occurrence_count,
                   COUNT(DISTINCT co.person_id) AS unique_patients
            FROM condition_occurrence co
            LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
            GROUP BY co.condition_concept_id, c.concept_name, co.condition_source_value
            ORDER BY occurrence_count DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"top_conditions": rows}
    finally:
        cur.close()
        put_connection(conn)

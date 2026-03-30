"""Drug exposure endpoints — search prescriptions (OMOP CDM)."""

from fastapi import APIRouter, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def search_drug_exposures(
    person_id: int = Query(None, description="Filter by person ID"),
    drug_concept_id: int = Query(None, description="Filter by drug concept ID"),
    source_value: str = Query(None, description="Search by source value (partial match)"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
):
    """Search drug exposures with optional filters."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if person_id:
            where_clauses.append("de.person_id = %s")
            params.append(person_id)
        if drug_concept_id:
            where_clauses.append("de.drug_concept_id = %s")
            params.append(drug_concept_id)
        if source_value:
            where_clauses.append("de.drug_source_value ILIKE %s")
            params.append(f"%{source_value}%")

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM drug_exposure de {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT de.drug_exposure_id, de.person_id,
                   de.drug_concept_id,
                   COALESCE(c.concept_name, de.drug_source_value) AS drug_name,
                   de.drug_source_value,
                   de.drug_exposure_start_date, de.drug_exposure_end_date,
                   de.days_supply, de.refills, de.quantity
            FROM drug_exposure de
            LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
            {where}
            ORDER BY de.drug_exposure_start_date DESC
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
def drug_exposures_summary():
    """Get top drugs by prescription frequency."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT de.drug_concept_id,
                   COALESCE(c.concept_name, de.drug_source_value) AS drug_name,
                   de.drug_source_value,
                   COUNT(*) AS prescription_count,
                   COUNT(DISTINCT de.person_id) AS unique_patients
            FROM drug_exposure de
            LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
            GROUP BY de.drug_concept_id, c.concept_name, de.drug_source_value
            ORDER BY prescription_count DESC
            LIMIT 20
        """)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"top_drugs": rows}
    finally:
        cur.close()
        put_connection(conn)

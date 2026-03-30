"""Person endpoints — demographics and summary data (OMOP CDM)."""

from fastapi import APIRouter, HTTPException, Query
from db import get_connection, put_connection

router = APIRouter()


@router.get("")
def list_persons(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    gender: str = Query(None, description="Filter by gender (M/F)"),
    race: str = Query(None, description="Filter by race"),
    year_of_birth: int = Query(None, description="Filter by birth year"),
):
    """List persons with pagination and optional filters."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        where_clauses = []
        params = []

        if gender:
            where_clauses.append("p.gender_source_value = %s")
            params.append(gender)
        if race:
            where_clauses.append("p.race_source_value ILIKE %s")
            params.append(f"%{race}%")
        if year_of_birth:
            where_clauses.append("p.year_of_birth = %s")
            params.append(year_of_birth)

        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        offset = (page - 1) * per_page

        cur.execute(f"SELECT COUNT(*) FROM person p {where}", params)
        total = cur.fetchone()[0]

        cur.execute(f"""
            SELECT p.person_id, p.gender_source_value AS gender,
                   p.race_source_value AS race,
                   p.ethnicity_source_value AS ethnicity,
                   p.year_of_birth, p.birth_datetime
            FROM person p
            {where}
            ORDER BY p.person_id
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


@router.get("/{person_id}")
def get_person(person_id: int):
    """Get a single person by ID."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT p.person_id, p.gender_source_value AS gender,
                   p.race_source_value AS race,
                   p.ethnicity_source_value AS ethnicity,
                   p.year_of_birth, p.birth_datetime,
                   op.observation_period_start_date,
                   op.observation_period_end_date
            FROM person p
            LEFT JOIN observation_period op ON p.person_id = op.person_id
            WHERE p.person_id = %s
        """, (person_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{person_id}/summary")
def get_person_summary(person_id: int):
    """Get aggregated summary for a person."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM person_summary WHERE person_id = %s", (person_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")
        columns = [desc[0] for desc in cur.description]
        return dict(zip(columns, row))
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{person_id}/visits")
def get_person_visits(person_id: int):
    """Get all visits for a person."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT person_id FROM person WHERE person_id = %s", (person_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        cur.execute("""
            SELECT vo.visit_occurrence_id, vo.visit_start_date, vo.visit_end_date,
                   vo.visit_concept_id,
                   COALESCE(c.concept_name, 'Unknown') AS visit_class
            FROM visit_occurrence vo
            LEFT JOIN concept c ON vo.visit_concept_id = c.concept_id
            WHERE vo.person_id = %s
            ORDER BY vo.visit_start_date DESC
        """, (person_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"person_id": person_id, "count": len(rows), "visits": rows}
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{person_id}/conditions")
def get_person_conditions(person_id: int):
    """Get all conditions for a person."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT person_id FROM person WHERE person_id = %s", (person_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        cur.execute("""
            SELECT co.condition_concept_id,
                   COALESCE(c.concept_name, co.condition_source_value) AS condition_name,
                   co.condition_source_value,
                   co.condition_start_date, co.condition_end_date,
                   CASE WHEN co.condition_end_date IS NULL
                             OR co.condition_end_date > CURRENT_DATE
                        THEN true ELSE false END AS is_active
            FROM condition_occurrence co
            LEFT JOIN concept c ON co.condition_concept_id = c.concept_id
            WHERE co.person_id = %s
            ORDER BY co.condition_start_date DESC
        """, (person_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"person_id": person_id, "count": len(rows), "conditions": rows}
    finally:
        cur.close()
        put_connection(conn)


@router.get("/{person_id}/drugs")
def get_person_drugs(person_id: int):
    """Get all drug exposures for a person."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT person_id FROM person WHERE person_id = %s", (person_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

        cur.execute("""
            SELECT de.drug_concept_id,
                   COALESCE(c.concept_name, de.drug_source_value) AS drug_name,
                   de.drug_source_value,
                   de.drug_exposure_start_date, de.drug_exposure_end_date,
                   de.days_supply, de.refills, de.quantity
            FROM drug_exposure de
            LEFT JOIN concept c ON de.drug_concept_id = c.concept_id
            WHERE de.person_id = %s
            ORDER BY de.drug_exposure_start_date DESC
        """, (person_id,))
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        return {"person_id": person_id, "count": len(rows), "drug_exposures": rows}
    finally:
        cur.close()
        put_connection(conn)

"""
Healthcare Data Platform — REST API (OMOP CDM)
FastAPI application providing operational data access to OMOP-formatted
RDS PostgreSQL database.

OMOP CDM tables are exposed via REST endpoints with concept resolution
for human-readable names. No PII is stored in OMOP person table by design.

Configuration via environment variables:
    RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD
    API_KEYS — comma-separated key:role pairs (e.g. "abc123:admin,def456:analyst")
               If unset, auth is disabled (dev mode).
"""

import os
import logging

from fastapi import FastAPI, HTTPException, Query
from contextlib import asynccontextmanager

from db import get_pool, release_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_pool()
    log.info("Database connection pool initialized")
    yield
    release_pool()
    log.info("Database connection pool released")


app = FastAPI(
    title="Healthcare Data Platform API (OMOP CDM)",
    description="REST API for OMOP-formatted healthcare data. Synthetic Synthea data only.",
    version="2.0.0",
    lifespan=lifespan,
)


# ── Routes are split into separate modules ───────────────

from routes.patients import router as persons_router
from routes.encounters import router as visits_router
from routes.conditions import router as conditions_router
from routes.medications import router as drugs_router
from routes.analytics import router as analytics_router

app.include_router(persons_router, prefix="/persons", tags=["Persons"])
app.include_router(visits_router, prefix="/visits", tags=["Visits"])
app.include_router(conditions_router, prefix="/conditions", tags=["Conditions"])
app.include_router(drugs_router, prefix="/drugs", tags=["Drug Exposures"])
app.include_router(analytics_router, prefix="/analytics", tags=["Analytics"])


@app.get("/health", tags=["System"])
def health_check():
    """API health check."""
    from db import get_connection
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")

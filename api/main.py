"""
Healthcare Data Platform — REST API
FastAPI application providing operational data access to RDS PostgreSQL.

PII fields (SSN, drivers license, passport, full address) are excluded
from all responses per ADR-006 (data minimization).

Configuration via environment variables:
    RDS_HOST, RDS_PORT, RDS_DATABASE, RDS_USER, RDS_PASSWORD
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
    title="Healthcare Data Platform API",
    description="REST API for operational healthcare data. Synthetic Synthea data only.",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Routes are split into separate modules ───────────────

from routes.patients import router as patients_router
from routes.encounters import router as encounters_router
from routes.conditions import router as conditions_router
from routes.medications import router as medications_router
from routes.analytics import router as analytics_router

app.include_router(patients_router, prefix="/patients", tags=["Patients"])
app.include_router(encounters_router, prefix="/encounters", tags=["Encounters"])
app.include_router(conditions_router, prefix="/conditions", tags=["Conditions"])
app.include_router(medications_router, prefix="/medications", tags=["Medications"])
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

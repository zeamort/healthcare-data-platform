"""
Lambda entry point for the FastAPI REST API.

Mangum adapts ASGI (FastAPI) to API Gateway's Lambda proxy integration
(payload format 2.0 for HTTP API v2).
"""

import os
import sys

# The package script copies api/ contents and api_handler.py to the same
# directory, so the FastAPI app imports resolve directly from there.
sys.path.insert(0, os.path.dirname(__file__))

from main import app  # noqa: E402
from mangum import Mangum  # noqa: E402

# lifespan="off" — the FastAPI lifespan opens a psycopg2 pool, but we let
# the pool be lazy-initialised on the first request instead. Lifespan events
# on Lambda are awkward (they'd fire per cold start and re-open connections
# unnecessarily).
handler = Mangum(app, lifespan="off")

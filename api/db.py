"""
Database connection pool for the API.
Uses psycopg2 connection pooling for efficient connection reuse.
"""

import os
from psycopg2 import pool

_pool = None

RDS_HOST = os.environ["RDS_HOST"]
RDS_PORT = int(os.environ.get("RDS_PORT", "5432"))
RDS_DATABASE = os.environ.get("RDS_DATABASE", "healthcare")
RDS_USER = os.environ["RDS_USER"]
RDS_PASSWORD = os.environ["RDS_PASSWORD"]


def get_pool():
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=RDS_HOST,
            port=RDS_PORT,
            database=RDS_DATABASE,
            user=RDS_USER,
            password=RDS_PASSWORD,
        )
    return _pool


def get_connection():
    """Get a connection from the pool."""
    return get_pool().getconn()


def put_connection(conn):
    """Return a connection to the pool."""
    get_pool().putconn(conn)


def release_pool():
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None

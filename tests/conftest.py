"""
Test configuration — sets dummy environment variables and mocks
third-party packages so ETL modules can be imported without
real AWS/database credentials or installed dependencies.
"""

import os
import sys
from unittest.mock import MagicMock

# ── Mock third-party packages that may not be installed ──────
# These stubs let ETL modules import without error.

_numpy_mock = MagicMock()
_numpy_mock.bool_ = bool  # pytest.approx checks numpy.bool_

for mod_name, mock_obj in [
    ("psycopg2", None), ("psycopg2.pool", None), ("psycopg2.extras", None),
    ("boto3", None),
    ("numpy", _numpy_mock),
    ("sklearn", None), ("sklearn.ensemble", None), ("sklearn.model_selection", None),
    ("sklearn.preprocessing", None), ("sklearn.metrics", None), ("sklearn.cluster", None),
    ("redshift_connector", None),
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = mock_obj if mock_obj is not None else MagicMock()

# Set dummy env vars BEFORE any ETL modules are imported.
# These are required at module import time by several scripts.
_DUMMY_ENV = {
    "S3_BUCKET": "test-bucket",
    "S3_PREFIX": "omop/",
    "RDS_HOST": "localhost",
    "RDS_PORT": "5432",
    "RDS_DATABASE": "test_db",
    "RDS_USER": "test_user",
    "RDS_PASSWORD": "test_pass",
    "REDSHIFT_HOST": "localhost",
    "REDSHIFT_PORT": "5439",
    "REDSHIFT_DATABASE": "test_dw",
    "REDSHIFT_USER": "test_user",
    "REDSHIFT_PASSWORD": "test_pass",
    "KINESIS_STREAM": "test-stream",
    "AWS_REGION": "us-east-1",
    "API_KEYS": "test-admin:admin,test-analyst:analyst",
}

for key, value in _DUMMY_ENV.items():
    os.environ.setdefault(key, value)

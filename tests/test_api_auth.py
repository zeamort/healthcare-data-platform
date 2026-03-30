"""
Unit tests for API authentication and authorization.

Tests API key validation, role hierarchy, and endpoint access control.
Uses FastAPI TestClient — no real database needed (mocked).
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock

# Add api/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))


@pytest.fixture(autouse=True)
def mock_db():
    """Mock database so API can import without real RDS."""
    mock_pool = MagicMock()
    with patch.dict(os.environ, {
        "RDS_HOST": "localhost",
        "RDS_PORT": "5432",
        "RDS_DATABASE": "test",
        "RDS_USER": "test",
        "RDS_PASSWORD": "test",
        "API_KEYS": "admin-key-123:admin,analyst-key-456:analyst",
    }):
        with patch("db.get_pool", return_value=mock_pool):
            yield


@pytest.fixture
def reload_auth():
    """Reload auth module to pick up env vars."""
    import importlib
    import auth
    importlib.reload(auth)
    return auth


class TestAuthModule:
    """Tests for auth.py internals."""

    def test_load_api_keys(self, reload_auth):
        keys = reload_auth._load_api_keys()
        assert keys == {"admin-key-123": "admin", "analyst-key-456": "analyst"}

    def test_load_api_keys_empty(self):
        with patch.dict(os.environ, {"API_KEYS": ""}, clear=False):
            import importlib
            import auth
            importlib.reload(auth)
            keys = auth._load_api_keys()
            assert keys == {}

    def test_role_hierarchy(self, reload_auth):
        assert reload_auth.ROLE_HIERARCHY["admin"] > reload_auth.ROLE_HIERARCHY["analyst"]


class TestAPIEndpointAuth:
    """Tests for endpoint authentication via TestClient."""

    @pytest.fixture
    def client(self, reload_auth):
        import importlib
        import main
        importlib.reload(main)
        from fastapi.testclient import TestClient
        return TestClient(main.app)

    def test_health_no_auth_required(self, client):
        """Health endpoint should work without API key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/health")
            assert resp.status_code == 200

    def test_persons_no_key_rejected(self, client):
        """Persons endpoint should reject requests without API key."""
        resp = client.get("/persons")
        assert resp.status_code == 401

    def test_persons_invalid_key_rejected(self, client):
        """Persons endpoint should reject invalid API keys."""
        resp = client.get("/persons", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 403

    def test_persons_analyst_rejected(self, client):
        """Persons endpoint requires admin — analyst should be rejected."""
        resp = client.get("/persons", headers={"X-API-Key": "analyst-key-456"})
        assert resp.status_code == 403

    def test_persons_admin_accepted(self, client):
        """Persons endpoint should work with admin key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.fetchall.return_value = []
            mock_cur.description = []
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/persons", headers={"X-API-Key": "admin-key-123"})
            assert resp.status_code == 200

    def test_analytics_overview_analyst_accepted(self, client):
        """Analytics overview should work with analyst key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.description = [("cnt",)]
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/analytics/overview",
                              headers={"X-API-Key": "analyst-key-456"})
            assert resp.status_code == 200

    def test_analytics_overview_admin_accepted(self, client):
        """Analytics overview should also work with admin key (higher role)."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.description = [("cnt",)]
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/analytics/overview",
                              headers={"X-API-Key": "admin-key-123"})
            assert resp.status_code == 200

    def test_data_quality_analyst_rejected(self, client):
        """Data quality endpoint requires admin."""
        resp = client.get("/analytics/data-quality",
                          headers={"X-API-Key": "analyst-key-456"})
        assert resp.status_code == 403

    def test_visits_analyst_accepted(self, client):
        """Visits listing should work with analyst key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.fetchall.return_value = []
            mock_cur.description = []
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/visits", headers={"X-API-Key": "analyst-key-456"})
            assert resp.status_code == 200

    def test_conditions_analyst_accepted(self, client):
        """Conditions search should work with analyst key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.fetchall.return_value = []
            mock_cur.description = []
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/conditions", headers={"X-API-Key": "analyst-key-456"})
            assert resp.status_code == 200

    def test_drugs_analyst_accepted(self, client):
        """Drug exposures search should work with analyst key."""
        with patch("db.get_connection") as mock_conn:
            mock_cur = MagicMock()
            mock_cur.fetchone.return_value = (0,)
            mock_cur.fetchall.return_value = []
            mock_cur.description = []
            mock_conn.return_value.cursor.return_value = mock_cur
            resp = client.get("/drugs", headers={"X-API-Key": "analyst-key-456"})
            assert resp.status_code == 200


class TestAuthDisabledMode:
    """Tests for dev mode when API_KEYS is not set."""

    def test_no_keys_allows_all(self):
        with patch.dict(os.environ, {"API_KEYS": ""}, clear=False):
            import importlib
            import auth
            importlib.reload(auth)
            # When no keys configured, _authenticate should return admin
            result = auth._authenticate(api_key=None)
            assert result["role"] == "admin"
            assert result["authenticated"] is False

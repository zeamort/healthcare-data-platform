"""
API Authentication & Authorization

API key-based auth with role-based access control.
Keys are configured via the API_KEYS environment variable as comma-separated
key:role pairs, e.g.: "abc123:admin,def456:analyst"

Roles:
    admin   — Full access to all endpoints including individual patient data
    analyst — Aggregate/summary endpoints only (no individual patient lookups)

Usage in routes:
    from auth import require_role
    @router.get("/endpoint")
    def my_endpoint(user=Depends(require_role("analyst"))):
        ...
"""

import os
import logging
from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

log = logging.getLogger(__name__)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _load_api_keys():
    """Parse API_KEYS env var into {key: role} dict."""
    raw = os.environ.get("API_KEYS", "")
    if not raw:
        return {}
    keys = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        key, role = entry.split(":", 1)
        keys[key.strip()] = role.strip()
    return keys


# Load once at import time
_API_KEYS = _load_api_keys()

if _API_KEYS:
    log.info("API auth enabled: %d key(s) configured", len(_API_KEYS))
else:
    log.warning("API_KEYS not set — authentication is DISABLED (all requests allowed)")


ROLE_HIERARCHY = {
    "admin": 2,
    "analyst": 1,
}


def _authenticate(api_key: str = Security(API_KEY_HEADER)):
    """Validate API key and return the associated role."""
    # If no keys configured, allow all (dev mode)
    if not _API_KEYS:
        return {"role": "admin", "authenticated": False}

    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="Missing API key. Provide X-API-Key header.",
        )

    role = _API_KEYS.get(api_key)
    if role is None:
        raise HTTPException(status_code=403, detail="Invalid API key.")

    return {"role": role, "authenticated": True}


def require_role(minimum_role: str):
    """Dependency that enforces a minimum role level."""
    min_level = ROLE_HIERARCHY.get(minimum_role, 0)

    def _check(user=Security(_authenticate)):
        user_level = ROLE_HIERARCHY.get(user["role"], 0)
        if user_level < min_level:
            raise HTTPException(
                status_code=403,
                detail=f"Insufficient permissions. Required: {minimum_role}, have: {user['role']}",
            )
        return user

    return _check

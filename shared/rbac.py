"""
shared/rbac.py -- Role-based access control for the API gateway.

Permission matrix maps roles to allowed (method, path_prefix) pairs.
The gateway hashes incoming API keys and looks up the associated role.
"""

import hashlib


PERMISSION_MATRIX: dict[str, list[tuple[str, str]] | None] = {
    "admin": None,
    "operator": [
        ("POST", "/v1/loans"),
        ("POST", "/v1/collateral"),
        ("POST", "/v1/margin"),
        ("POST", "/v1/liquidations"),
        ("POST", "/v1/prices"),
        ("GET", "/v1/"),
    ],
    "signer": [
        ("POST", "/v1/signing"),
        ("GET", "/v1/"),
    ],
    "viewer": [
        ("GET", "/v1/"),
    ],
    "system": [
        ("POST", "/v1/prices"),
        ("POST", "/v1/margin"),
        ("GET", "/v1/"),
    ],
}


def hash_key(raw: str) -> str:
    """SHA-256 hash of a raw API key, returned as hex."""
    return hashlib.sha256(raw.encode()).hexdigest()


def check_permission(
    role: str,
    method: str,
    path: str,
) -> bool:
    """Check whether a role is allowed to access method+path.

    Returns True if the role has permission, False otherwise.
    """
    allowed = PERMISSION_MATRIX.get(role)
    if allowed is None:
        return True

    method_upper = method.upper()
    for allowed_method, allowed_prefix in allowed:
        if (
            method_upper == allowed_method
            and path.startswith(allowed_prefix)
        ):
            return True

    return False

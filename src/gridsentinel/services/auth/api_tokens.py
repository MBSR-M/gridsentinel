"""Audited opaque API tokens with explicit scope and bounded expiration."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
import json
from typing import Any

from gridsentinel.core.exceptions import AuthenticationError, NotFoundError, ValidationError
from gridsentinel.core.security import hash_token, new_token, require_permission
from .management import utcnow


def api_token_digest(token: str) -> bytes:
    """Require the API-token namespace before hashing its random secret."""
    if not isinstance(token, str) or not token.startswith("gs_api_"):
        raise AuthenticationError("Invalid or expired credentials")
    return hash_token(token[7:])


class ApiTokenManager:
    """Scopes may only reduce the current permissions of the token owner."""

    def __init__(self, database: Any, audit: Any,
                 resolve_principal: Callable[[Any, dict[str, Any], str | None], dict[str, Any]]) -> None:
        self.database = database
        self.audit = audit
        self.resolve_principal = resolve_principal

    def create(self, principal: dict[str, Any], data: dict[str, Any],
               correlation_id: str) -> dict[str, Any]:
        """Issue an expiring token, revealing its secret exactly once."""
        require_permission(principal, "api_tokens.create_own")
        name, scopes = data.get("token_name"), data.get("scopes")
        if not isinstance(name, str) or not 1 <= len(name) <= 128 or any(ord(char) < 32 for char in name):
            raise ValidationError("Invalid API token name")
        if not isinstance(scopes, list) or not 1 <= len(scopes) <= 100 or any(not isinstance(scope, str) or scope not in principal["permissions"] for scope in scopes):
            raise ValidationError("API token scopes must be a subset of your permissions")
        ttl = data.get("ttl_seconds", 86400)
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not 60 <= ttl <= 2592000:
            raise ValidationError("API token TTL must be 60 to 2592000 seconds")
        token, now = "gs_api_" + new_token(), utcnow()
        expires = now + timedelta(seconds=ttl)
        with self.database.transaction() as connection:
            cursor = connection.execute("INSERT INTO api_tokens(tenant_id,owner_user_id,token_name,token_hash,scopes,created_at,expires_at) VALUES(%s,%s,%s,%s,%s,%s,%s)", (principal["tenant_id"], principal["id"], name, api_token_digest(token), json.dumps(sorted(set(scopes))), now, expires))
            result = {"id": cursor.lastrowid, "token_name": name, "scopes": sorted(set(scopes)), "expires_at": expires}
            self.audit.append(connection, principal["tenant_id"], principal["id"], "API_TOKEN_CREATE", "api_token", cursor.lastrowid, correlation_id, after=result, session_id=principal.get("session_id"))
        return {**result, "token": token}

    def list(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """Return safe owned token metadata with no raw credentials or hashes."""
        require_permission(principal, "api_tokens.read_own")
        all_tokens = "api_tokens.read_all" in principal["permissions"]
        params = (principal["tenant_id"],) if all_tokens else (principal["tenant_id"], principal["id"])
        with self.database.read() as connection:
            rows = connection.fetch_all("SELECT id,owner_user_id,token_name,scopes,created_at,expires_at,last_used_at,revoked_at FROM api_tokens WHERE tenant_id=%s" + ("" if all_tokens else " AND owner_user_id=%s") + " ORDER BY id DESC LIMIT 500", params)
        for row in rows:
            row["scopes"] = json.loads(row["scopes"])
        return rows

    def revoke(self, principal: dict[str, Any], token_id: int,
               correlation_id: str) -> None:
        """Revoke only owned tokens unless a tenant administration grant exists."""
        require_permission(principal, "api_tokens.revoke_own")
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT owner_user_id FROM api_tokens WHERE tenant_id=%s AND id=%s FOR UPDATE", (principal["tenant_id"], token_id))
            if not row:
                raise NotFoundError("API token not found")
            if row["owner_user_id"] != principal["id"]:
                require_permission(principal, "api_tokens.revoke_all")
            connection.execute("UPDATE api_tokens SET revoked_at=%s WHERE tenant_id=%s AND id=%s AND revoked_at IS NULL", (utcnow(), principal["tenant_id"], token_id))
            self.audit.append(connection, principal["tenant_id"], principal["id"], "API_TOKEN_REVOKE", "api_token", token_id, correlation_id, session_id=principal.get("session_id"))

    def authenticate(self, token: str, tenant_id: str | None = None) -> dict[str, Any]:
        """Check durable revocation, tenant, expiry, status and owner scope intersection."""
        digest, now = api_token_digest(token), utcnow()
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT t.id AS api_token_id,t.tenant_id,t.owner_user_id,t.scopes,t.expires_at,t.revoked_at,u.username,u.status FROM api_tokens t JOIN users u ON u.tenant_id=t.tenant_id AND u.id=t.owner_user_id WHERE t.token_hash=%s FOR UPDATE", (digest,))
            if not row or row["revoked_at"] or row["expires_at"] <= now or row["status"] != "ACTIVE" or (tenant_id is not None and row["tenant_id"] != tenant_id):
                raise AuthenticationError("Invalid or expired credentials")
            principal = self.resolve_principal(connection, {"id": row["owner_user_id"], "username": row["username"], "tenant_id": row["tenant_id"]}, None)
            principal["permissions"] = sorted(set(principal["permissions"]) & set(json.loads(row["scopes"])))
            principal["api_token_id"] = row["api_token_id"]
            connection.execute("UPDATE api_tokens SET last_used_at=%s WHERE tenant_id=%s AND id=%s", (now, row["tenant_id"], row["api_token_id"]))
            return principal

"""Tenant-scoped user and role administration with atomic audit records."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

from gridsentinel.core.exceptions import AuthorizationError, ConflictError, NotFoundError, ValidationError
from gridsentinel.core.security import require_permission, validate_identifier


def utcnow() -> datetime:
    """Return UTC as a naive MySQL DATETIME value."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class UserManager:
    """Keep user provisioning independent of credential and session handling."""

    def __init__(self, database: Any, hasher: Any, audit: Any) -> None:
        self.database = database
        self.hasher = hasher
        self.audit = audit

    def list_users(self, principal: dict[str, Any], limit: int = 100,
                   after_id: int = 0) -> list[dict[str, Any]]:
        """Read safe account metadata with bounded keyset pagination."""
        require_permission(principal, "users.read")
        with self.database.read() as connection:
            return connection.fetch_all("SELECT id,username,email,status,created_at,updated_at,last_login_at FROM users WHERE tenant_id=%s AND id>%s ORDER BY id LIMIT %s", (principal["tenant_id"], max(0, after_id), max(1, min(limit, 500))))

    def _roles(self, connection: Any, principal: dict[str, Any], codes: Any) -> list[int]:
        if not isinstance(codes, list) or not codes or len(codes) > 20 or any(not isinstance(code, str) for code in codes):
            raise ValidationError("Provide one to twenty role codes")
        result = []
        for code in set(codes):
            role = connection.fetch_one("SELECT id FROM roles WHERE tenant_id=%s AND role_code=%s", (principal["tenant_id"], code))
            if not role:
                raise ValidationError("Unknown role")
            # A user creator may assign only roles whose permissions they hold.
            grants = connection.fetch_all("SELECT p.permission_code FROM permissions p JOIN role_permissions rp ON rp.tenant_id=p.tenant_id AND rp.permission_id=p.id WHERE rp.tenant_id=%s AND rp.role_id=%s", (principal["tenant_id"], role["id"]))
            if any(row["permission_code"] not in principal["permissions"] for row in grants):
                raise AuthorizationError("Cannot assign a role with additional privileges")
            result.append(role["id"])
        return result

    def create_user(self, principal: dict[str, Any], data: dict[str, Any],
                    correlation_id: str) -> dict[str, Any]:
        """Create a tenant account and its authorized role assignments atomically."""
        require_permission(principal, "users.create")
        username = validate_identifier(data.get("username"))
        email = self._email(data.get("email"))
        salt, digest, iterations = self.hasher.hash(data.get("password"))
        with self.database.transaction() as connection:
            roles = self._roles(connection, principal, data.get("roles", ["VIEWER"]))
            if connection.fetch_one("SELECT id FROM users WHERE tenant_id=%s AND (username=%s OR email=%s)", (principal["tenant_id"], username, email)):
                raise ConflictError("Account identifier already exists")
            now = utcnow()
            cursor = connection.execute("INSERT INTO users(tenant_id,username,email,password_hash,password_salt,password_algorithm,password_iterations,status,created_at,updated_at,created_by) VALUES(%s,%s,%s,%s,%s,'PBKDF2-HMAC-SHA256',%s,'ACTIVE',%s,%s,%s)", (principal["tenant_id"], username, email, digest, salt, iterations, now, now, principal["id"]))
            user_id = cursor.lastrowid
            for role_id in roles:
                connection.execute("INSERT INTO user_roles(tenant_id,user_id,role_id,assigned_by,assigned_at) VALUES(%s,%s,%s,%s,%s)", (principal["tenant_id"], user_id, role_id, principal["id"], now))
            after = {"id": user_id, "username": username, "email": email, "status": "ACTIVE", "roles": data.get("roles", ["VIEWER"])}
            self.audit.append(connection, principal["tenant_id"], principal["id"], "USER_CREATE", "user", user_id, correlation_id, after=after, session_id=principal["session_id"])
            return after

    def update_user(self, principal: dict[str, Any], user_id: int,
                    data: dict[str, Any], correlation_id: str) -> dict[str, Any]:
        """Update an account, revoke its sessions, and preserve a final admin."""
        require_permission(principal, "users.update")
        if not data or set(data) - {"email", "status", "password", "roles"}:
            raise ValidationError("Unsupported account update")
        if "status" in data:
            require_permission(principal, "users.disable")
            if data["status"] not in ("ACTIVE", "DISABLED"):
                raise ValidationError("Invalid account status")
        hashed = self.hasher.hash(data["password"]) if "password" in data else None
        with self.database.transaction() as connection:
            tenant = principal["tenant_id"]
            # Serialize privileged account updates to prevent concurrent final-admin removal.
            connection.fetch_one("SELECT tenant_id FROM tenants WHERE tenant_id=%s FOR UPDATE", (tenant,))
            before = connection.fetch_one("SELECT id,username,email,status FROM users WHERE tenant_id=%s AND id=%s FOR UPDATE", (tenant, user_id))
            if not before:
                raise NotFoundError("Account not found")
            current_roles = connection.fetch_all("SELECT r.role_code FROM roles r JOIN user_roles ur ON ur.tenant_id=r.tenant_id AND ur.role_id=r.id WHERE ur.tenant_id=%s AND ur.user_id=%s", (tenant, user_id))
            # Editing a more privileged account can bypass role assignment rules
            # through password resets or deactivation. Check its current grants.
            self._roles(connection, principal, [role["role_code"] for role in current_roles])
            desired_roles = data.get("roles", [role["role_code"] for role in current_roles])
            role_ids = self._roles(connection, principal, desired_roles)
            if any(role["role_code"] == "SUPER_ADMIN" for role in current_roles) and (data.get("status") == "DISABLED" or "SUPER_ADMIN" not in desired_roles):
                count = connection.fetch_one("SELECT COUNT(*) AS count FROM users u JOIN user_roles ur ON ur.tenant_id=u.tenant_id AND ur.user_id=u.id JOIN roles r ON r.tenant_id=ur.tenant_id AND r.id=ur.role_id WHERE u.tenant_id=%s AND u.status='ACTIVE' AND r.role_code='SUPER_ADMIN'", (tenant,))
                if count["count"] <= 1:
                    raise ConflictError("Cannot remove the final active super administrator")
            fields, params = [], []
            for key in ("email", "status"):
                if key in data:
                    fields.append(f"{key}=%s")
                    params.append(self._email(data[key]) if key == "email" else data[key])
            if hashed:
                salt, digest, iterations = hashed
                fields.extend(["password_salt=%s", "password_hash=%s", "password_iterations=%s"])
                params.extend([salt, digest, iterations])
            fields.append("updated_at=%s")
            params.extend([utcnow(), tenant, user_id])
            connection.execute("UPDATE users SET " + ",".join(fields) + " WHERE tenant_id=%s AND id=%s", params)
            if "roles" in data:
                connection.execute("DELETE FROM user_roles WHERE tenant_id=%s AND user_id=%s", (tenant, user_id))
                for role_id in role_ids:
                    connection.execute("INSERT INTO user_roles(tenant_id,user_id,role_id,assigned_by,assigned_at) VALUES(%s,%s,%s,%s,%s)", (tenant, user_id, role_id, principal["id"], utcnow()))
            connection.execute("UPDATE user_sessions SET revoked_at=%s,revoke_reason='account_changed' WHERE tenant_id=%s AND user_id=%s AND revoked_at IS NULL", (utcnow(), tenant, user_id))
            connection.execute("UPDATE api_tokens SET revoked_at=%s WHERE tenant_id=%s AND owner_user_id=%s AND revoked_at IS NULL", (utcnow(), tenant, user_id))
            after = {**before, **{key: value for key, value in data.items() if key != "password"}, "password_changed": bool(hashed)}
            self.audit.append(connection, tenant, principal["id"], "USER_UPDATE", "user", user_id, correlation_id, before=before, after=after, session_id=principal["session_id"])
            return after

    def roles(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """List tenant roles and their grants."""
        require_permission(principal, "roles.read")
        with self.database.read() as connection:
            roles = connection.fetch_all("SELECT id,role_code,display_name,description,is_system_role FROM roles WHERE tenant_id=%s ORDER BY id LIMIT 500", (principal["tenant_id"],))
            for role in roles:
                role["permissions"] = [row["permission_code"] for row in connection.fetch_all("SELECT p.permission_code FROM permissions p JOIN role_permissions rp ON rp.tenant_id=p.tenant_id AND rp.permission_id=p.id WHERE rp.tenant_id=%s AND rp.role_id=%s ORDER BY p.permission_code", (principal["tenant_id"], role["id"]))]
            return roles

    def permissions(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """List the tenant permission catalog."""
        require_permission(principal, "roles.read")
        with self.database.read() as connection:
            return connection.fetch_all("SELECT id,permission_code,resource,action,description FROM permissions WHERE tenant_id=%s ORDER BY permission_code LIMIT 500", (principal["tenant_id"],))

    def create_role(self, principal: dict[str, Any], data: dict[str, Any],
                    correlation_id: str) -> dict[str, Any]:
        """Create a bounded custom role without granting beyond the actor."""
        require_permission(principal, "roles.manage")
        code = validate_identifier(data.get("role_code"), 64)
        grants = data.get("permissions")
        if not isinstance(grants, list) or len(grants) > 100 or any(grant not in principal["permissions"] for grant in grants):
            raise ValidationError("Invalid role permission grants")
        name = data.get("display_name", code)
        if not isinstance(name, str) or not 1 <= len(name) <= 128:
            raise ValidationError("Invalid role display name")
        description = data.get("description", "")
        if not isinstance(description, str) or len(description) > 2000:
            raise ValidationError("Invalid role description")
        tenant = principal["tenant_id"]
        with self.database.transaction() as connection:
            if connection.fetch_one("SELECT id FROM roles WHERE tenant_id=%s AND role_code=%s", (tenant, code)):
                raise ConflictError("Role already exists")
            cursor = connection.execute("INSERT INTO roles(tenant_id,role_code,display_name,description,is_system_role) VALUES(%s,%s,%s,%s,FALSE)", (tenant, code, name, description))
            for grant in set(grants):
                connection.execute("INSERT INTO role_permissions(tenant_id,role_id,permission_id) SELECT %s,%s,id FROM permissions WHERE tenant_id=%s AND permission_code=%s", (tenant, cursor.lastrowid, tenant, grant))
            after = {"id": cursor.lastrowid, "role_code": code, "permissions": grants}
            self.audit.append(connection, tenant, principal["id"], "ROLE_CREATE", "role", cursor.lastrowid, correlation_id, after=after, session_id=principal["session_id"])
            return after

    @staticmethod
    def _email(value: Any) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or len(value) > 255 or not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value):
            raise ValidationError("Invalid email address")
        return value

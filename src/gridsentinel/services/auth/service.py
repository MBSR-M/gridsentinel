"""Opaque credential lifecycle with durable rotation and reuse revocation."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import hmac
import ipaddress
import secrets
from typing import Any
import uuid

from gridsentinel.core.exceptions import AuthenticationError, ConflictError, NotFoundError, ValidationError
from gridsentinel.core.security import PasswordHasher, hash_token, new_token, require_permission, validate_identifier
from .management import UserManager, utcnow
from .roles import seed_roles
from .api_tokens import ApiTokenManager


class AuthService:
    """Resolve identity from MySQL on every request for immediate revocation."""

    def __init__(self, database: Any, config: Any, audit: Any) -> None:
        self.database = database
        self.config = config
        self.audit = audit
        self.hasher = PasswordHasher(config.get_int("security", "password_iterations", 600000),
                                     config.get_int("security", "password_min_length", 12))
        self.users = UserManager(database, self.hasher, audit)
        self.api_tokens = ApiTokenManager(database, audit, self._principal)
        self._dummy_salt = secrets.token_bytes(32)
        self._dummy_hash = secrets.token_bytes(32)

    def bootstrap_admin(self, username: str, password: str,
                        tenant_id: str) -> dict[str, Any]:
        """Provision the first administrator through a trusted CLI, once per tenant."""
        username = validate_identifier(username)
        tenant_id = validate_identifier(tenant_id, 64)
        salt, digest, iterations = self.hasher.hash(password)
        with self.database.transaction() as connection:
            connection.execute("INSERT IGNORE INTO tenants(tenant_id,display_name,created_at) VALUES(%s,%s,%s)", (tenant_id, tenant_id, utcnow()))
            connection.fetch_one("SELECT tenant_id FROM tenants WHERE tenant_id=%s FOR UPDATE", (tenant_id,))
            if connection.fetch_one("SELECT id FROM users WHERE tenant_id=%s LIMIT 1", (tenant_id,)):
                raise ConflictError("Tenant already has an account; use authorized user management")
            seed_roles(connection, tenant_id)
            now = utcnow()
            cursor = connection.execute("INSERT INTO users(tenant_id,username,password_hash,password_salt,password_algorithm,password_iterations,status,created_at,updated_at) VALUES(%s,%s,%s,%s,'PBKDF2-HMAC-SHA256',%s,'ACTIVE',%s,%s)", (tenant_id, username, digest, salt, iterations, now, now))
            user_id = cursor.lastrowid
            connection.execute("INSERT INTO user_roles(tenant_id,user_id,role_id,assigned_at) SELECT %s,%s,id,%s FROM roles WHERE tenant_id=%s AND role_code='SUPER_ADMIN'", (tenant_id, user_id, now, tenant_id))
            result = {"id": user_id, "username": username, "tenant_id": tenant_id}
            self.audit.append(connection, tenant_id, user_id, "ADMIN_BOOTSTRAP", "user", user_id, str(uuid.uuid4()), after=result)
            return result

    def login(self, username: str, password: str, tenant_id: str,
              ip: str, user_agent: str, correlation_id: str) -> dict[str, Any]:
        """Authenticate with tenant/account/IP throttling and generic failures."""
        try:
            username = validate_identifier(username)
            tenant_id = validate_identifier(tenant_id, 64)
        except ValidationError as error:
            raise AuthenticationError("Invalid username or password") from error
        if not isinstance(password, str) or len(password) > 128:
            raise AuthenticationError("Invalid username or password")
        packed_ip = self._ip(ip)
        denied = True
        result = None
        with self.database.transaction() as connection:
            tenant = connection.fetch_one("SELECT tenant_id FROM tenants WHERE tenant_id=%s", (tenant_id,))
            if not tenant:
                self.hasher.verify(password, self._dummy_salt, self._dummy_hash, self.hasher.iterations)
                raise AuthenticationError("Invalid username or password")
            now = utcnow()
            throttle = self._throttle(connection, tenant_id, "ip:" + ip,
                                      self.config.get_int("security", "login_ip_max_attempts", 30), now)
            throttle |= self._throttle(connection, tenant_id, "user:" + username,
                                       self.config.get_int("security", "login_max_attempts", 5), now)
            user = connection.fetch_one("SELECT id,username,tenant_id,password_salt,password_hash,password_iterations,status,failed_login_count,locked_until FROM users WHERE tenant_id=%s AND username=%s FOR UPDATE", (tenant_id, username))
            password_ok = self.hasher.verify(password, user["password_salt"] if user else self._dummy_salt,
                                             user["password_hash"] if user else self._dummy_hash,
                                             user["password_iterations"] if user else self.hasher.iterations)
            locked = bool(user and user["locked_until"] and user["locked_until"] > now)
            denied = not user or not password_ok or user["status"] != "ACTIVE" or locked or throttle
            reason = "THROTTLED" if throttle or locked else "INVALID_CREDENTIALS"
            if denied:
                if user and not locked and not throttle:
                    count = user["failed_login_count"] + 1
                    lock_until = now + timedelta(seconds=self.config.get_int("security", "lockout_seconds", 900)) if count >= self.config.get_int("security", "login_max_attempts", 5) else None
                    connection.execute("UPDATE users SET failed_login_count=%s,locked_until=%s WHERE tenant_id=%s AND id=%s", (count, lock_until, tenant_id, user["id"]))
                self.audit.append(connection, tenant_id, user["id"] if user else None, "LOGIN", "user", user["id"] if user else "unknown", correlation_id, outcome="DENIED", metadata={"reason": reason})
            else:
                connection.execute("UPDATE users SET failed_login_count=0,locked_until=NULL,last_login_at=%s WHERE tenant_id=%s AND id=%s", (now, tenant_id, user["id"]))
                result = self._create_session(connection, user, packed_ip, user_agent, now)
                self.audit.append(connection, tenant_id, user["id"], "LOGIN", "user", user["id"], correlation_id, session_id=result["session_id"])
                self.audit.append(connection, tenant_id, user["id"], "SESSION_CREATE", "session", result["session_id"], correlation_id, session_id=result["session_id"])
            connection.execute("INSERT INTO login_attempts(tenant_id,attempted_at,username_identifier,ip_address,outcome,reason_code,correlation_id) VALUES(%s,%s,%s,%s,%s,%s,%s)", (tenant_id, now, hashlib.sha256(username.encode()).digest(), packed_ip, "DENIED" if denied else "SUCCESS", reason if denied else "OK", correlation_id))
        if denied:
            raise AuthenticationError("Invalid username or password")
        return result

    def _throttle(self, connection: Any, tenant: str, key: str,
                  maximum: int, now: datetime) -> bool:
        digest = hashlib.sha256(key.encode()).digest()
        connection.execute("INSERT IGNORE INTO login_throttle(tenant_id,bucket_hash,window_started_at,attempts) VALUES(%s,%s,%s,0)", (tenant, digest, now))
        bucket = connection.fetch_one("SELECT window_started_at,attempts FROM login_throttle WHERE tenant_id=%s AND bucket_hash=%s FOR UPDATE", (tenant, digest))
        expired = (now - bucket["window_started_at"]).total_seconds() >= self.config.get_int("security", "login_window_seconds", 300)
        attempts = 1 if expired else bucket["attempts"] + 1
        connection.execute("UPDATE login_throttle SET window_started_at=%s,attempts=%s WHERE tenant_id=%s AND bucket_hash=%s", (now if expired else bucket["window_started_at"], min(attempts, maximum + 1), tenant, digest))
        return attempts > maximum

    def _create_session(self, connection: Any, user: dict[str, Any], ip: bytes | None,
                        user_agent: str, now: datetime) -> dict[str, Any]:
        tokens = self._tokens()
        session_id, family = str(uuid.uuid4()), str(uuid.uuid4())
        access_expiry = now + timedelta(seconds=self.config.get_int("security", "access_ttl_seconds", 900))
        absolute = now + timedelta(seconds=self.config.get_int("security", "absolute_ttl_seconds", 43200))
        connection.execute("INSERT INTO user_sessions(id,tenant_id,user_id,session_token_hash,refresh_token_hash,csrf_token_hash,token_family_id,created_at,last_activity_at,access_expires_at,absolute_expires_at,ip_address,user_agent_hash,device_label) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (session_id, user["tenant_id"], user["id"], hash_token(tokens["access_token"]), hash_token(tokens["refresh_token"]), hash_token(tokens["csrf_token"]), family, now, now, access_expiry, absolute, ip, hashlib.sha256(str(user_agent)[:1024].encode()).digest(), "Browser or API client"))
        return {**tokens, "session_id": session_id, "access_expires_at": access_expiry,
                "absolute_expires_at": absolute, "user": self._principal(connection, user, session_id)}

    @staticmethod
    def _tokens() -> dict[str, str]:
        return {key: new_token() for key in ("access_token", "refresh_token", "csrf_token")}

    def _principal(self, connection: Any, user: dict[str, Any],
                   session_id: str | None) -> dict[str, Any]:
        roles = connection.fetch_all("SELECT r.role_code FROM user_roles ur JOIN roles r ON r.tenant_id=ur.tenant_id AND r.id=ur.role_id WHERE ur.tenant_id=%s AND ur.user_id=%s", (user["tenant_id"], user["id"]))
        grants = connection.fetch_all("SELECT DISTINCT p.permission_code,r.role_code FROM user_roles ur JOIN roles r ON r.tenant_id=ur.tenant_id AND r.id=ur.role_id JOIN role_permissions rp ON rp.tenant_id=ur.tenant_id AND rp.role_id=ur.role_id JOIN permissions p ON p.tenant_id=rp.tenant_id AND p.id=rp.permission_id WHERE ur.tenant_id=%s AND ur.user_id=%s", (user["tenant_id"], user["id"]))
        return {"id": user["id"], "user_id": user["id"], "username": user["username"],
                "tenant_id": user["tenant_id"], "session_id": session_id,
                "roles": [row["role_code"] for row in roles],
                "permissions": sorted({row["permission_code"] for row in grants}),
                "incident_scope": "tenant" if any(row["permission_code"] == "incidents.read" and row["role_code"] != "FIELD_OPERATOR" for row in grants) else "assigned"}

    def _session_valid(self, session: dict[str, Any], now: datetime,
                       require_access: bool = True) -> bool:
        return not session["revoked_at"] and session["status"] == "ACTIVE" and session["absolute_expires_at"] > now and (not require_access or session["access_expires_at"] > now) and (now - session["last_activity_at"]).total_seconds() < self.config.get_int("security", "idle_timeout_seconds", 1800)

    def authenticate(self, access_token: str,
                     tenant_id: str | None = None) -> dict[str, Any]:
        """Validate expiry, idle deadline, account status and current permissions."""
        digest = hash_token(access_token)
        with self.database.transaction() as connection:
            session = connection.fetch_one("SELECT s.*,u.username,u.status FROM user_sessions s JOIN users u ON u.tenant_id=s.tenant_id AND u.id=s.user_id WHERE s.session_token_hash=%s FOR UPDATE", (digest,))
            now = utcnow()
            if not session or not self._session_valid(session, now) or (tenant_id is not None and session["tenant_id"] != tenant_id):
                raise AuthenticationError("Invalid or expired credentials")
            connection.execute("UPDATE user_sessions SET last_activity_at=%s WHERE tenant_id=%s AND id=%s", (now, session["tenant_id"], session["id"]))
            return self._principal(connection, {"id": session["user_id"], "username": session["username"], "tenant_id": session["tenant_id"]}, session["id"])

    def validate_csrf(self, principal: dict[str, Any], csrf_token: str) -> None:
        """Compare a synchronizer token to its durable session hash."""
        digest = hash_token(csrf_token)
        with self.database.read() as connection:
            session = connection.fetch_one("SELECT csrf_token_hash,revoked_at FROM user_sessions WHERE tenant_id=%s AND id=%s AND user_id=%s", (principal["tenant_id"], principal["session_id"], principal["id"]))
            if not session or session["revoked_at"] or not hmac.compare_digest(session["csrf_token_hash"], digest):
                raise AuthenticationError("Invalid request credentials")

    def refresh(self, refresh_token: str, csrf_token: str,
                correlation_id: str) -> dict[str, Any]:
        """Rotate all tokens atomically; replay revokes the entire durable family."""
        digest, csrf_digest = hash_token(refresh_token), hash_token(csrf_token)
        result = None
        denied = False
        with self.database.transaction() as connection:
            session = connection.fetch_one("SELECT s.*,u.username,u.status FROM user_sessions s JOIN users u ON u.tenant_id=s.tenant_id AND u.id=s.user_id WHERE s.refresh_token_hash=%s FOR UPDATE", (digest,))
            if not session:
                # Query after the current-token lock read: concurrent rotations may have committed.
                used = connection.fetch_one("SELECT tenant_id,session_id,token_family_id FROM refresh_token_history WHERE token_hash=%s FOR UPDATE", (digest,))
                if used:
                    connection.execute("UPDATE user_sessions SET revoked_at=%s,revoke_reason='refresh_reuse' WHERE tenant_id=%s AND token_family_id=%s AND revoked_at IS NULL", (utcnow(), used["tenant_id"], used["token_family_id"]))
                    self.audit.append(connection, used["tenant_id"], None, "REFRESH_REUSE", "session", used["session_id"], correlation_id, outcome="DENIED", session_id=used["session_id"])
                denied = True
            elif not self._session_valid(session, utcnow(), False) or not hmac.compare_digest(session["csrf_token_hash"], csrf_digest):
                denied = True
                self.audit.append(connection, session["tenant_id"], session["user_id"], "SESSION_REFRESH", "session", session["id"], correlation_id, outcome="DENIED", session_id=session["id"])
            else:
                now = utcnow()
                tokens = self._tokens()
                access_expiry = min(now + timedelta(seconds=self.config.get_int("security", "access_ttl_seconds", 900)), session["absolute_expires_at"])
                connection.execute("INSERT INTO refresh_token_history(token_hash,tenant_id,session_id,token_family_id,consumed_at,expires_at) VALUES(%s,%s,%s,%s,%s,%s)", (digest, session["tenant_id"], session["id"], session["token_family_id"], now, session["absolute_expires_at"]))
                connection.execute("UPDATE user_sessions SET session_token_hash=%s,refresh_token_hash=%s,csrf_token_hash=%s,access_expires_at=%s,last_activity_at=%s WHERE tenant_id=%s AND id=%s", (hash_token(tokens["access_token"]), hash_token(tokens["refresh_token"]), hash_token(tokens["csrf_token"]), access_expiry, now, session["tenant_id"], session["id"]))
                self.audit.append(connection, session["tenant_id"], session["user_id"], "SESSION_REFRESH", "session", session["id"], correlation_id, session_id=session["id"])
                result = {**tokens, "session_id": session["id"], "access_expires_at": access_expiry,
                          "absolute_expires_at": session["absolute_expires_at"],
                          "user": self._principal(connection, {"id": session["user_id"], "tenant_id": session["tenant_id"], "username": session["username"]}, session["id"])}
        if denied:
            raise AuthenticationError("Invalid or expired credentials")
        return result

    def logout(self, principal: dict[str, Any], all_sessions: bool = False,
               correlation_id: str = "logout") -> None:
        """Revoke the current session or all sessions for the same account."""
        require_permission(principal, "sessions.revoke_own")
        with self.database.transaction() as connection:
            selector = "user_id" if all_sessions else "id"
            target = principal["id"] if all_sessions else principal["session_id"]
            connection.execute(f"UPDATE user_sessions SET revoked_at=%s,revoke_reason='logout' WHERE tenant_id=%s AND {selector}=%s AND revoked_at IS NULL", (utcnow(), principal["tenant_id"], target))
            self.audit.append(connection, principal["tenant_id"], principal["id"], "LOGOUT_ALL" if all_sessions else "LOGOUT", "session", target, correlation_id, session_id=principal["session_id"])

    def list_sessions(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """List bounded session metadata within the authorized tenant/account."""
        require_permission(principal, "sessions.read_own")
        all_sessions = "sessions.read_all" in principal["permissions"]
        with self.database.read() as connection:
            return connection.fetch_all("SELECT id,user_id,created_at,last_activity_at,access_expires_at,absolute_expires_at,revoked_at,revoke_reason,device_label FROM user_sessions WHERE tenant_id=%s" + ("" if all_sessions else " AND user_id=%s") + " ORDER BY created_at DESC LIMIT 500", (principal["tenant_id"],) if all_sessions else (principal["tenant_id"], principal["id"]))

    def revoke_session(self, principal: dict[str, Any], session_id: str,
                       correlation_id: str) -> None:
        """Revoke a tenant session only when ownership or admin grants allow."""
        require_permission(principal, "sessions.revoke_own")
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT user_id FROM user_sessions WHERE tenant_id=%s AND id=%s FOR UPDATE", (principal["tenant_id"], session_id))
            if not row:
                raise NotFoundError("Session not found")
            if row["user_id"] != principal["id"]:
                require_permission(principal, "sessions.revoke_all")
            connection.execute("UPDATE user_sessions SET revoked_at=%s,revoke_reason='operator_revocation' WHERE tenant_id=%s AND id=%s AND revoked_at IS NULL", (utcnow(), principal["tenant_id"], session_id))
            self.audit.append(connection, principal["tenant_id"], principal["id"], "SESSION_REVOKE", "session", session_id, correlation_id, session_id=principal["session_id"])

    def list_users(self, principal: dict[str, Any], limit: int = 100,
                   after_id: int = 0) -> list[dict[str, Any]]:
        """Delegate tenant account listing to the administration service."""
        return self.users.list_users(principal, limit, after_id)

    def create_user(self, principal: dict[str, Any], data: dict[str, Any],
                    correlation_id: str) -> dict[str, Any]:
        """Delegate audited account provisioning."""
        return self.users.create_user(principal, data, correlation_id)

    def update_user(self, principal: dict[str, Any], user_id: int, data: dict[str, Any],
                    correlation_id: str) -> dict[str, Any]:
        """Delegate audited account and role updates."""
        return self.users.update_user(principal, user_id, data, correlation_id)

    def roles(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """Delegate authorized role listing."""
        return self.users.roles(principal)

    def permissions(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """Delegate authorized permission listing."""
        return self.users.permissions(principal)

    def create_role(self, principal: dict[str, Any], data: dict[str, Any],
                    correlation_id: str) -> dict[str, Any]:
        """Delegate custom role provisioning."""
        return self.users.create_role(principal, data, correlation_id)

    def create_api_token(self, principal: dict[str, Any], data: dict[str, Any],
                         correlation_id: str) -> dict[str, Any]:
        """Issue an explicitly scoped opaque token."""
        return self.api_tokens.create(principal, data, correlation_id)

    def list_api_tokens(self, principal: dict[str, Any]) -> list[dict[str, Any]]:
        """List safe API token metadata."""
        return self.api_tokens.list(principal)

    def revoke_api_token(self, principal: dict[str, Any], token_id: int,
                         correlation_id: str) -> None:
        """Revoke an API token atomically with its audit."""
        self.api_tokens.revoke(principal, token_id, correlation_id)

    def authenticate_api_token(self, token: str,
                               tenant_id: str | None = None) -> dict[str, Any]:
        """Authenticate with live owner permissions intersected with token scope."""
        return self.api_tokens.authenticate(token, tenant_id)

    @staticmethod
    def _ip(value: str) -> bytes | None:
        try:
            return ipaddress.ip_address(value).packed
        except ValueError:
            return None

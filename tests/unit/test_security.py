"""Security gates for tenant authorization, credential shape and session expiry."""

from datetime import timedelta
from unittest.mock import Mock
import unittest

from gridsentinel.core.exceptions import AuthenticationError, AuthorizationError, ValidationError
from gridsentinel.core.security import PasswordHasher, hash_token, new_token, require_permission, validate_identifier
from gridsentinel.services.audit import audit_digest
from gridsentinel.services.auth import AuthService
from gridsentinel.services.auth.management import utcnow
from gridsentinel.services.auth.roles import ROLE_GRANTS


class SecurityTests(unittest.TestCase):
    def test_permission_and_tenant_are_deny_default(self) -> None:
        principal = {"id": 1, "tenant_id": "tenant-a", "permissions": ["meters.read"]}
        require_permission(principal, "meters.read", "tenant-a")
        for identity, permission, tenant in [(principal, "users.create", "tenant-a"),
                                               (principal, "meters.read", "tenant-b"),
                                               ({}, "meters.read", None)]:
            with self.assertRaises(AuthorizationError):
                require_permission(identity, permission, tenant)

    def test_roles_do_not_grant_implied_privileges(self) -> None:
        self.assertNotIn("system.logs.read", ROLE_GRANTS["ADMIN"])
        self.assertNotIn("incidents.resolve", ROLE_GRANTS["DEVELOPER"])
        self.assertNotIn("models.activate", ROLE_GRANTS["ML_ENGINEER"])
        self.assertNotIn("users.update", ROLE_GRANTS["AUDITOR"])
        self.assertNotIn("incidents.resolve", ROLE_GRANTS["FIELD_OPERATOR"])

    def test_password_salt_and_constant_work_validation(self) -> None:
        hasher = PasswordHasher()
        salt, digest, iterations = hasher.hash("a long example passphrase")
        other_salt, other_digest, _ = hasher.hash("a long example passphrase")
        self.assertNotEqual(salt, other_salt)
        self.assertNotEqual(digest, other_digest)
        self.assertTrue(hasher.verify("a long example passphrase", salt, digest, iterations))
        self.assertFalse(hasher.verify("a wrong example passphrase", salt, digest, iterations))
        for password in ("tiny", "aaaaaaaaaaaa", " " * 20, "example\npassword"):
            with self.assertRaises(ValidationError):
                hasher.hash(password)

    def test_opaque_token_shape_and_sql_injection(self) -> None:
        token = new_token()
        self.assertEqual(len(hash_token(token)), 32)
        self.assertNotEqual(new_token(), token)
        for invalid in ("", "x" * 44, "' OR 1=1 --", None):
            with self.assertRaises(AuthenticationError):
                hash_token(invalid)
        with self.assertRaises(ValidationError):
            validate_identifier("admin' OR '1'='1")

    def test_idle_access_absolute_revocation_and_disabled_account(self) -> None:
        config = Mock()
        config.get_int.side_effect = lambda section, option, default: default
        service = AuthService(Mock(), config, Mock())
        now = utcnow()
        session = {"revoked_at": None, "status": "ACTIVE",
                   "last_activity_at": now, "access_expires_at": now + timedelta(seconds=900),
                   "absolute_expires_at": now + timedelta(seconds=43200)}
        self.assertTrue(service._session_valid(session, now))
        for updates in ({"revoked_at": now}, {"status": "DISABLED"},
                        {"access_expires_at": now}, {"absolute_expires_at": now},
                        {"last_activity_at": now - timedelta(seconds=1800)}):
            self.assertFalse(service._session_valid({**session, **updates}, now))
        self.assertTrue(service._session_valid({**session, "access_expires_at": now}, now, False))

    def test_audit_hash_depends_on_order_and_content(self) -> None:
        first = audit_digest(bytes(32), '{"action":"LOGIN"}')
        self.assertNotEqual(first, audit_digest(bytes(32), '{"action":"LOGOUT"}'))
        self.assertNotEqual(audit_digest(first, "second"), audit_digest(bytes(32), "second"))

    def test_role_assignment_cannot_escalate_actor(self) -> None:
        config = Mock()
        config.get_int.side_effect = lambda section, option, default: default
        service = AuthService(Mock(), config, Mock())
        principal = {"id": 1, "tenant_id": "demo", "permissions": ["users.create"]}
        connection = Mock()
        connection.fetch_one.return_value = {"id": 3}
        connection.fetch_all.return_value = [{"permission_code": "roles.manage"}]
        with self.assertRaises(AuthorizationError):
            service.users._roles(connection, principal, ["SUPER_ADMIN"])


if __name__ == "__main__":
    unittest.main()

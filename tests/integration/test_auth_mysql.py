"""Opt-in authentication lifecycle verified against a real MySQL deployment."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import os
from pathlib import Path
import shutil
import tempfile
from unittest.mock import Mock
import unittest
import uuid

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import AuthenticationError, AuthorizationError, ConflictError
from gridsentinel.services.auth import AuthService
from gridsentinel.services.audit import AuditService
from gridsentinel.services.auth.management import utcnow
from gridsentinel.infrastructure.mysql import Database


@unittest.skipUnless(os.environ.get("GS_RUN_INTEGRATION") == "1", "set GS_RUN_INTEGRATION=1 with configured MySQL")
class AuthMySQLTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[2]
        cls.config = ConfigManager(cls.root / "config")
        cls.database = Database(cls.config)
        # This suite owns only foundation migrations; other modules test theirs.
        with tempfile.TemporaryDirectory() as directory:
            shutil.copyfile(cls.root / "database" / "migrations" / "001_foundation.sql",
                            Path(directory) / "001_foundation.sql")
            cls.database.migrate(directory)
        cls.audit = AuditService(cls.database)
        cls.auth = AuthService(cls.database, cls.config, cls.audit)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.database.close()

    def setUp(self) -> None:
        self.tenant = "security-test-" + uuid.uuid4().hex[:12]
        self.password = "Synthetic verification passphrase 42"
        self.admin = self.auth.bootstrap_admin("administrator", self.password, self.tenant)

    def login(self) -> dict:
        return self.auth.login("administrator", self.password, self.tenant,
                               "127.0.0.1", "integration-tests", str(uuid.uuid4()))

    def test_login_rotation_reuse_and_no_plaintext_storage(self) -> None:
        credentials = self.login()
        principal = self.auth.authenticate(credentials["access_token"], self.tenant)
        self.assertEqual(principal["tenant_id"], self.tenant)
        self.auth.validate_csrf(principal, credentials["csrf_token"])
        with self.database.read() as connection:
            row = connection.fetch_one("SELECT session_token_hash,refresh_token_hash,csrf_token_hash FROM user_sessions WHERE tenant_id=%s AND id=%s", (self.tenant, credentials["session_id"]))
        self.assertEqual(len(row["session_token_hash"]), 32)
        self.assertNotEqual(row["session_token_hash"], credentials["access_token"].encode())
        rotated = self.auth.refresh(credentials["refresh_token"], credentials["csrf_token"], "rotate")
        self.assertNotEqual(rotated["access_token"], credentials["access_token"])
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate(credentials["access_token"])
        with self.assertRaises(AuthenticationError):
            self.auth.refresh(credentials["refresh_token"], credentials["csrf_token"], "reuse")
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate(rotated["access_token"])
        self.assertTrue(self.audit.verify(self.tenant)["valid"])

    def test_tenant_boundary_csrf_and_user_role_escalation(self) -> None:
        credentials = self.login()
        principal = self.auth.authenticate(credentials["access_token"])
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate(credentials["access_token"], "another-tenant")
        with self.assertRaises(AuthenticationError):
            self.auth.validate_csrf(principal, "A" * 43)
        created = self.auth.create_user(principal, {"username": "viewer", "password": self.password, "roles": ["VIEWER"]}, "create-viewer")
        viewer_credentials = self.auth.login("viewer", self.password, self.tenant, "127.0.0.2", "tests", "viewer-login")
        viewer = self.auth.authenticate(viewer_credentials["access_token"])
        with self.assertRaises(AuthorizationError):
            self.auth.list_users(viewer)
        with self.assertRaises(AuthorizationError):
            self.auth.create_user(viewer, {"username": "escalation", "password": self.password, "roles": ["SUPER_ADMIN"]}, "escalation")
        with self.assertRaises(AuthorizationError):
            self.auth.revoke_session(viewer, credentials["session_id"], "revoke-admin")
        self.auth.update_user(principal, created["id"], {"status": "DISABLED"}, "disable-viewer")
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate(viewer_credentials["access_token"])
        with self.assertRaises(ConflictError):
            self.auth.update_user(principal, principal["id"], {"roles": ["VIEWER"]}, "remove-last-admin")

    def test_account_lockout_and_generic_enumeration_response(self) -> None:
        messages = []
        for index in range(5):
            with self.assertRaises(AuthenticationError) as result:
                self.auth.login("administrator", "incorrect passphrase 42", self.tenant, "127.0.0.1", "tests", f"bad-{index}")
            messages.append(str(result.exception))
        with self.assertRaises(AuthenticationError) as locked:
            self.login()
        with self.assertRaises(AuthenticationError) as absent:
            self.auth.login("missing-account", self.password, self.tenant, "127.0.0.1", "tests", "missing")
        self.assertEqual(str(locked.exception), str(absent.exception))
        self.assertEqual(messages[0], str(absent.exception))
        with self.database.read() as connection:
            row = connection.fetch_one("SELECT failed_login_count,locked_until FROM users WHERE tenant_id=%s AND id=%s", (self.tenant, self.admin["id"]))
        self.assertEqual(row["failed_login_count"], 5)
        self.assertIsNotNone(row["locked_until"])

    def test_expiry_logout_all_and_revocation(self) -> None:
        credentials = self.login()
        principal = self.auth.authenticate(credentials["access_token"])
        extra = self.login()
        self.auth.logout(principal, True, "logout-all")
        for token in (credentials["access_token"], extra["access_token"]):
            with self.assertRaises(AuthenticationError):
                self.auth.authenticate(token)
        third = self.login()
        with self.database.transaction() as connection:
            connection.execute("UPDATE user_sessions SET last_activity_at=%s WHERE tenant_id=%s AND id=%s", (utcnow() - timedelta(hours=1), self.tenant, third["session_id"]))
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate(third["access_token"])

    def test_audit_failure_rolls_back_security_mutation(self) -> None:
        failing_audit = Mock()
        failing_audit.append.side_effect = RuntimeError("injected audit failure")
        auth = AuthService(self.database, self.config, failing_audit)
        with self.assertRaises(RuntimeError):
            auth.login("administrator", self.password, self.tenant, "127.0.0.1", "tests", "audit-failure")
        with self.database.read() as connection:
            count = connection.fetch_one("SELECT COUNT(*) AS count FROM user_sessions WHERE tenant_id=%s", (self.tenant,))
            user = connection.fetch_one("SELECT last_login_at FROM users WHERE tenant_id=%s AND id=%s", (self.tenant, self.admin["id"]))
        self.assertEqual(count["count"], 0)
        self.assertIsNone(user["last_login_at"])

    def test_concurrent_audit_writes_form_one_chain(self) -> None:
        def write(index: int) -> None:
            with self.database.transaction() as connection:
                self.audit.append(connection, self.tenant, self.admin["id"], "CONCURRENCY_TEST", "test", index, f"parallel-{index}")
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(write, range(12)))
        result = self.audit.verify(self.tenant)
        self.assertTrue(result["valid"])
        self.assertEqual(result["checked"], 13)

    def test_api_tokens_are_scoped_expiring_and_revocable(self) -> None:
        principal = self.auth.authenticate(self.login()["access_token"])
        token = self.auth.create_api_token(principal, {"token_name": "synthetic ingestion", "scopes": ["meters.read"], "ttl_seconds": 3600}, "api-token-create")
        api_principal = self.auth.authenticate_api_token(token["token"], self.tenant)
        self.assertEqual(api_principal["permissions"], ["meters.read"])
        self.assertIsNone(api_principal["session_id"])
        with self.assertRaises(AuthorizationError):
            self.auth.list_users(api_principal)
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate_api_token(token["token"], "another-tenant")
        listed = self.auth.list_api_tokens(principal)
        self.assertEqual(len(listed), 1)
        self.assertNotIn("token_hash", listed[0])
        self.auth.revoke_api_token(principal, token["id"], "api-token-revoke")
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate_api_token(token["token"])
        self.assertTrue(self.audit.verify(self.tenant)["valid"])

    def test_api_token_privilege_escalation_and_account_change(self) -> None:
        from gridsentinel.core.exceptions import ValidationError

        principal = self.auth.authenticate(self.login()["access_token"])
        user = self.auth.create_user(principal, {"username": "api-owner", "password": self.password, "roles": ["VIEWER"]}, "create-api-owner")
        credentials = self.auth.login("api-owner", self.password, self.tenant, "127.0.0.2", "tests", "login-api-owner")
        owner = self.auth.authenticate(credentials["access_token"])
        with self.assertRaises(ValidationError):
            self.auth.create_api_token(owner, {"token_name": "escalation", "scopes": ["roles.manage"]}, "api-escalation")
        token = self.auth.create_api_token(owner, {"token_name": "meter reader", "scopes": ["meters.read"]}, "api-meter-token")
        self.auth.update_user(principal, user["id"], {"roles": ["AUDITOR"]}, "change-api-owner-role")
        with self.assertRaises(AuthenticationError):
            self.auth.authenticate_api_token(token["token"])

    def test_audit_column_mutation_is_detected(self) -> None:
        with self.database.transaction() as connection:
            connection.execute("UPDATE audit_events SET action='TAMPER_TEST' WHERE tenant_id=%s AND sequence_number=1", (self.tenant,))
        result = self.audit.verify(self.tenant)
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "COLUMN_MISMATCH")


if __name__ == "__main__":
    unittest.main()

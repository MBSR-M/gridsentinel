"""Authorization and browser security regressions at the real route boundary."""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock
import uuid

from gridsentinel.api.application import ApiApplication
from gridsentinel.api.server import Request
from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import AuthenticationError, AuthorizationError, NotFoundError
from gridsentinel.infrastructure.mysql import Database
from gridsentinel.services.audit import AuditService
from gridsentinel.services.auth import AuthService

ROOT = Path(__file__).resolve().parents[2]


def request(method: str, path: str, body: dict | None = None,
            headers: dict | None = None, cookies: dict | None = None,
            query: dict | None = None) -> Request:
    """Construct transport-validated synthetic requests for route dispatch tests."""
    return Request(method, "/api/v1" + path, query or {}, body or {},
                   headers or {}, cookies or {}, "test-correlation", "127.0.0.1")


class ApiAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ConfigManager(ROOT / "config", environ={})
        self.principal = {"id": 1, "user_id": 1, "username": "synthetic", "tenant_id": "demo",
                          "session_id": "synthetic-session", "roles": ["VIEWER"], "permissions": [],
                          "incident_scope": "tenant"}
        self.auth = Mock()
        self.auth.authenticate.return_value = self.principal
        self.operations, self.models, self.platform, self.jobs = Mock(), Mock(), Mock(), Mock()
        self.application = ApiApplication(self.config, Mock(), self.auth, Mock(), self.operations,
                                          self.models, self.platform, self.jobs)

    def test_all_sensitive_routes_deny_without_required_permission(self) -> None:
        routes = [("GET", "/sessions"), ("DELETE", "/sessions/session-id"),
                  ("GET", "/users"), ("POST", "/users"), ("PATCH", "/users/1"),
                  ("GET", "/roles"), ("POST", "/roles"), ("GET", "/permissions"),
                  ("GET", "/api-tokens"), ("POST", "/api-tokens"), ("DELETE", "/api-tokens/1"),
                  ("GET", "/meters"), ("GET", "/meters/meter-1"), ("GET", "/meters/meter-1/events"),
                  ("GET", "/analytics/overview"), ("GET", "/analytics/sla"),
                  ("GET", "/incidents"), ("GET", "/incidents/incident-1"),
                  ("POST", "/incidents/incident-1/acknowledge"), ("POST", "/incidents/incident-1/assign"),
                  ("POST", "/incidents/incident-1/resolve"), ("POST", "/incidents/incident-1/feedback"),
                  ("GET", "/ml/models"), ("GET", "/ml/training-runs"), ("POST", "/ml/training-runs"),
                  ("POST", "/ml/models/model-1/approve"), ("POST", "/ml/models/model-1/activate"),
                  ("POST", "/ml/models/model-1/rollback"), ("GET", "/ml/drift"),
                  ("GET", "/developer/logs"), ("GET", "/developer/logs/export"),
                  ("GET", "/developer/jobs"), ("POST", "/developer/jobs/job-1/retry"),
                  ("POST", "/developer/jobs/job-1/cancel"), ("GET", "/connectors"), ("POST", "/connectors"),
                  ("GET", "/audit/events"), ("GET", "/audit/exports"), ("GET", "/audit/verify"),
                  ("GET", "/system/config"), ("GET", "/system/metrics"),
                  ("GET", "/reports"), ("GET", "/reports/overview"),
                  ("POST", "/auth/logout"), ("POST", "/auth/logout-all")]
        for method, path in routes:
            with self.subTest(method=method, path=path), self.assertRaises(AuthorizationError):
                self.application.dispatch(request(method, path, cookies={"gs_access": "opaque"}))

    def test_cookie_mutation_requires_durable_csrf_validation(self) -> None:
        self.principal["permissions"] = ["users.create"]
        self.auth.validate_csrf.side_effect = AuthenticationError("Invalid request credentials")
        with self.assertRaises(AuthenticationError):
            self.application.dispatch(request("POST", "/users", cookies={"gs_access": "opaque"}))
        self.auth.create_user.assert_not_called()

    def test_authorized_bearer_mutation_uses_scope_without_browser_csrf(self) -> None:
        self.principal["permissions"] = ["users.create"]
        self.auth.create_user.return_value = {"id": 3}
        response = self.application.dispatch(request("POST", "/users", {"username": "test"},
                                                    {"authorization": "Bearer opaque"}))
        self.assertEqual(response.data, {"id": 3})
        self.auth.validate_csrf.assert_not_called()

    def test_cross_origin_and_cross_site_requests_fail_before_authentication(self) -> None:
        for headers in ({"origin": "https://attacker.invalid"}, {"sec-fetch-site": "cross-site"}):
            with self.assertRaises(AuthorizationError):
                self.application.dispatch(request("GET", "/auth/me", headers=headers))
        self.auth.authenticate.assert_not_called()

    def test_tenant_body_and_query_cannot_override_identity(self) -> None:
        self.principal["permissions"] = ["meters.read"]
        for body, query in [({"tenant_id": "other-tenant"}, {}), ({}, {"tenant_id": "other-tenant"})]:
            with self.assertRaises(AuthorizationError):
                self.application.dispatch(request("GET", "/meters", body=body, query=query))
        self.operations.list_meters.assert_not_called()

    def test_field_scope_survives_unrelated_additional_role(self) -> None:
        self.principal.update(roles=["FIELD_OPERATOR", "AUDITOR"], permissions=["incidents.read", "incidents.feedback"], incident_scope="assigned")
        self.operations.incident_detail.return_value = {"id": "incident-1", "assigned_to": 2}
        with self.assertRaises(NotFoundError):
            self.application.dispatch(request("GET", "/incidents/incident-1"))
        self.application.dispatch(request("GET", "/incidents", query={"assigned_to": "2"}))
        self.operations.list_incidents.assert_called_once_with("demo", {"assigned_to": "1"})

    def test_config_diagnostics_redact_secret_and_secret_file_path(self) -> None:
        self.config = ConfigManager(ROOT / "config", overrides={"mysql.password": "private-value", "mysql.password_file": "absent-file"}, environ={})
        self.application.config = self.config
        self.principal["permissions"] = ["config.read"]
        response = self.application.dispatch(request("GET", "/system/config"))
        values = response.data["configuration"]["mysql"]
        self.assertEqual(values["password"], "[REDACTED]")
        self.assertEqual(values["password_file"], "[REDACTED]")

    def test_secure_cookie_transport_requires_trusted_https_proxy(self) -> None:
        self.application.config = ConfigManager(ROOT / "config", overrides={"security.cookie_secure": "true"}, environ={})
        with self.assertRaises(AuthorizationError):
            self.application.dispatch(request("GET", "/auth/me"))


@unittest.skipUnless(os.environ.get("GS_RUN_INTEGRATION") == "1", "set GS_RUN_INTEGRATION=1 with configured MySQL")
class ApiMySQLAuthorizationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tenant = "api-security-" + uuid.uuid4().hex[:12]
        cls.config = ConfigManager(ROOT / "config", overrides={"application.tenant_id": cls.tenant})
        cls.database = Database(cls.config)
        with tempfile.TemporaryDirectory() as directory:
            for name in ("001_foundation.sql", "004_platform.sql"):
                shutil.copyfile(ROOT / "database" / "migrations" / name, Path(directory) / name)
            cls.database.migrate(directory)
        cls.audit = AuditService(cls.database)
        cls.auth = AuthService(cls.database, cls.config, cls.audit)
        cls.password = "Synthetic API verification passphrase 42"
        cls.auth.bootstrap_admin("api-admin", cls.password, cls.tenant)
        cls.operations, cls.models, cls.platform, cls.jobs = Mock(), Mock(), Mock(), Mock()
        cls.operations.list_meters.return_value = {"items": [], "next_cursor": None}
        cls.application = ApiApplication(cls.config, cls.database, cls.auth, cls.audit,
                                        cls.operations, cls.models, cls.platform, cls.jobs)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.database.close()

    def test_browser_login_uses_http_only_cookies_and_session_csrf(self) -> None:
        response = self.application.dispatch(request("POST", "/auth/login", {"username": "api-admin", "password": self.password}))
        self.assertNotIn("access_token", response.data)
        self.assertNotIn("refresh_token", response.data)
        cookies = [value for key, value in response.headers if key == "Set-Cookie"]
        self.assertEqual(len(cookies), 3)
        self.assertTrue(any("gs_access=" in cookie and "HttpOnly" in cookie and "SameSite=Strict" in cookie for cookie in cookies))
        access = next(cookie.split(";", 1)[0].split("=", 1)[1] for cookie in cookies if cookie.startswith("gs_access="))
        with self.assertRaises(AuthenticationError):
            self.application.dispatch(request("POST", "/users", {"username": "blocked"}, cookies={"gs_access": access}))

    def test_real_scoped_api_token_authenticates_and_denies_privileged_route(self) -> None:
        credentials = self.auth.login("api-admin", self.password, self.tenant, "127.0.0.1", "tests", "admin-token-login")
        principal = self.auth.authenticate(credentials["access_token"])
        token = self.auth.create_api_token(principal, {"token_name": "API meter reader", "scopes": ["meters.read"]}, "issue-api-reader")
        response = self.application.dispatch(request("GET", "/meters", headers={"authorization": "Bearer " + token["token"]}))
        self.assertEqual(response.data["items"], [])
        with self.assertRaises(AuthorizationError):
            self.application.dispatch(request("GET", "/users", headers={"authorization": "Bearer " + token["token"]}))
        self.auth.revoke_api_token(principal, token["id"], "revoke-api-reader")
        with self.assertRaises(AuthenticationError):
            self.application.dispatch(request("GET", "/meters", headers={"authorization": "Bearer " + token["token"]}))

    def test_live_field_and_auditor_roles_preserve_assignment_scope(self) -> None:
        credentials = self.auth.login("api-admin", self.password, self.tenant, "127.0.0.1", "tests", "field-create-login")
        admin = self.auth.authenticate(credentials["access_token"])
        user = self.auth.create_user(admin, {"username": "field-auditor", "password": self.password, "roles": ["FIELD_OPERATOR", "AUDITOR"]}, "field-create")
        field_credentials = self.auth.login("field-auditor", self.password, self.tenant, "127.0.0.2", "tests", "field-login")
        field = self.auth.authenticate(field_credentials["access_token"])
        self.assertEqual(field["incident_scope"], "assigned")
        self.operations.incident_detail.return_value = {"id": "another-users-incident", "assigned_to": admin["id"]}
        with self.assertRaises(NotFoundError):
            self.application.dispatch(request("GET", "/incidents/another-users-incident", headers={"authorization": "Bearer " + field_credentials["access_token"]}))

    def test_api_token_idempotency_receipt_never_persists_plaintext_secret(self) -> None:
        credentials = self.auth.login("api-admin", self.password, self.tenant, "127.0.0.1", "tests", "receipt-login")
        principal = self.auth.authenticate(credentials["access_token"])
        key = "token-receipt-" + uuid.uuid4().hex
        create = request("POST", "/api-tokens", {"token_name": "receipt verification", "scopes": ["meters.read"]},
                         {"authorization": "Bearer " + credentials["access_token"], "idempotency-key": key})
        first = self.application.dispatch(create)
        secret = first.data["token"]
        self.assertTrue(secret.startswith("gs_api_"))
        with self.database.read() as connection:
            receipt = connection.fetch_one("SELECT response_json FROM api_requests WHERE tenant_id=%s AND actor_user_id=%s AND idempotency_key=%s", (self.tenant, principal["id"], key))
        self.assertNotIn(secret, receipt["response_json"])
        replay = self.application.dispatch(create)
        self.assertEqual(replay.data["token"], "[REDACTED]")


if __name__ == "__main__":
    unittest.main()

"""Exercise real HTTP cookies, authorization and persisted demo workflows."""

from __future__ import annotations

from http.cookiejar import CookieJar
import json
import os
from pathlib import Path
import unittest
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener
import uuid


@unittest.skipUnless(os.environ.get("GS_RUN_HTTP_INTEGRATION") == "1", "set GS_RUN_HTTP_INTEGRATION=1 with a running API and local test administrator")
class ApiWorkflowTests(unittest.TestCase):
    """Use generated local test credentials; never print passwords or tokens."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.url = os.environ.get("GS_TEST_API_URL", "http://127.0.0.1:8080/api/v1")
        cls.client = build_opener(HTTPCookieProcessor(CookieJar()))
        password = Path(os.environ["GS_TEST_ADMIN_PASSWORD_FILE"]).read_text().strip()
        cls.login = cls.call("POST", "/auth/login", {"username": os.environ.get("GS_TEST_ADMIN_USERNAME", "pilotadmin"),
                             "password": password, "tenant_id": "demo"})["data"]
        cls.csrf = cls.login["csrf_token"]

    @classmethod
    def call(cls, method: str, path: str, body: dict | None = None,
             csrf: bool = True, key: str | None = None) -> dict:
        headers = {"Content-Type": "application/json"}
        if csrf and hasattr(cls, "csrf"):
            headers["X-CSRF-Token"] = cls.csrf
        if key:
            headers["Idempotency-Key"] = key
        request = Request(cls.url + path, data=json.dumps(body).encode() if body is not None else None,
                          headers=headers, method=method)
        with cls.client.open(request, timeout=15) as response:
            return json.load(response)

    def test_authenticated_drilldowns_and_audited_export(self) -> None:
        overview = self.call("GET", "/analytics/overview")["data"]
        self.assertEqual(overview["data_mode"], "synthetic")
        self.assertGreater(overview["metrics"]["registered_meters"], 0)
        meters = self.call("GET", "/meters?limit=10")["data"]["items"]
        detail = self.call("GET", "/meters/" + meters[0]["meter_id"])["data"]
        self.assertIn("health", detail)
        self.assertTrue(self.call("GET", "/reports/overview")["data"]["export_id"])
        self.assertTrue(self.call("GET", "/audit/verify")["data"]["valid"])

    def test_csrf_and_cross_tenant_request_fail_closed(self) -> None:
        with self.assertRaises(HTTPError) as error:
            self.call("POST", "/roles", {"role_code": "SHOULD_NOT_EXIST"}, csrf=False)
        self.assertIn(error.exception.code, (401, 403))
        with self.assertRaises(HTTPError) as error:
            self.call("GET", "/meters?tenant_id=other")
        self.assertEqual(error.exception.code, 403)

    def test_one_time_api_token_receipt_does_not_reveal_secret_twice(self) -> None:
        key = str(uuid.uuid4())
        body = {"token_name": "http-workflow-test", "scopes": ["meters.read"], "ttl_seconds": 60}
        result = self.call("POST", "/api-tokens", body, key=key)["data"]
        self.assertTrue(result["token"].startswith("gs_api_"))
        replay = self.call("POST", "/api-tokens", body, key=key)["data"]
        self.assertEqual(replay["token"], "[REDACTED]")
        self.call("DELETE", "/api-tokens/" + str(result["id"]))


if __name__ == "__main__":
    unittest.main()

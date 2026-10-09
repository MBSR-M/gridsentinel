"""Live evidence for restricted Grafana reporting views and all panel SQL."""

from __future__ import annotations

import json
import base64
import os
from pathlib import Path
import unittest
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.infrastructure.mysql import Database

ROOT = Path(__file__).resolve().parents[2]


@unittest.skipUnless(os.environ.get("GS_RUN_REPORTING_INTEGRATION") == "1",
                     "set GS_RUN_REPORTING_INTEGRATION=1 after provision_reporting.py")
class ReportingIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.app = Database(ConfigManager(ROOT / "config", overrides={
            "mysql.password_file": str(ROOT / ".local/secrets/mysql_password")}))
        cls.reporter = Database(ConfigManager(ROOT / "config", overrides={
            "mysql.user": "gs_reporter", "mysql.password_file": str(ROOT / ".local/secrets/grafana_mysql_password")}))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.reporter.close()
        cls.app.close()

    def test_all_dashboards_panel_queries_execute_as_restricted_reporter(self) -> None:
        paths = sorted((ROOT / "grafana/dashboards").glob("*.json"))
        self.assertEqual(len(paths), 9)
        panel_count = 0
        with self.reporter.read() as connection:
            for path in paths:
                dashboard = json.loads(path.read_text(encoding="utf-8"))
                self.assertFalse(dashboard["editable"])
                self.assertEqual(dashboard["timezone"], "utc")
                for panel in dashboard["panels"]:
                    for target in panel["targets"]:
                        sql = target["rawSql"].replace("$__timeFilter(observed_at)", "observed_at >= UTC_TIMESTAMP() - INTERVAL 1 DAY AND observed_at <= UTC_TIMESTAMP()")
                        with self.subTest(dashboard=dashboard["uid"], panel=panel["title"]):
                            connection.fetch_all(sql)
                        panel_count += 1
        self.assertEqual(panel_count, 27)

    def test_reporter_cannot_read_identity_audit_or_underlying_payloads(self) -> None:
        with self.reporter.read() as connection:
            for table in ("users", "user_sessions", "audit_events", "communication_events", "ml_model_registry", "application_logs"):
                with self.subTest(table=table), self.assertRaises(Exception) as denied:
                    connection.fetch_one(f"SELECT COUNT(*) AS count FROM `{table}`")
                self.assertEqual(denied.exception.args[0], 1142)
            grants = connection.fetch_all("SHOW GRANTS")
        statements = [next(iter(row.values())) for row in grants]
        self.assertEqual(sum(statement.startswith("GRANT SELECT ON") for statement in statements), 9)
        self.assertTrue(all(statement.startswith("GRANT SELECT ON") or statement.startswith("GRANT USAGE ON") for statement in statements))

    def test_reporting_views_exclude_a_second_tenant(self) -> None:
        with self.reporter.read() as connection:
            before = connection.fetch_one("SELECT COALESCE(SUM(meter_count),0) AS count FROM v_fleet_health")["count"]
        other = "reporting-other-" + uuid.uuid4().hex[:12]
        with self.app.transaction() as connection:
            connection.execute("INSERT INTO tenants(tenant_id,display_name,created_at) VALUES(%s,%s,UTC_TIMESTAMP(6))", (other, "Synthetic isolation verification"))
            connection.execute("INSERT INTO meters(tenant_id,meter_id,installation_status,created_at,updated_at) VALUES(%s,'synthetic-restricted','ACTIVE',UTC_TIMESTAMP(6),UTC_TIMESTAMP(6))", (other,))
        try:
            with self.reporter.read() as connection:
                after = connection.fetch_one("SELECT COALESCE(SUM(meter_count),0) AS count FROM v_fleet_health")["count"]
            self.assertEqual(after, before)
        finally:
            with self.app.transaction() as connection:
                connection.execute("DELETE FROM meters WHERE tenant_id=%s", (other,))
                connection.execute("DELETE FROM tenants WHERE tenant_id=%s", (other,))


@unittest.skipUnless(os.environ.get("GS_RUN_GRAFANA_INTEGRATION") == "1",
                     "set GS_RUN_GRAFANA_INTEGRATION=1 with provisioned Grafana on loopback")
class GrafanaIntegrationTests(unittest.TestCase):
    """Verify native provisioning, independent login and actual datasource health."""

    def authenticated_request(self, path: str, body: dict | None = None) -> dict | list:
        password = (ROOT / ".local/secrets/grafana_password").read_text(encoding="utf-8").strip()
        authorization = base64.b64encode(("gs_grafana_admin:" + password).encode()).decode()
        headers = {"Authorization": "Basic " + authorization}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode()
        with urlopen(Request("http://127.0.0.1:3000" + path, data=data, headers=headers), timeout=15) as response:
            return json.load(response)

    def test_nine_dashboards_and_mysql_secret_file_authentication(self) -> None:
        result = self.authenticated_request("/api/search?type=dash-db")
        self.assertEqual(len([row for row in result if row["uid"].startswith("gs-")]), 9)
        datasource = self.authenticated_request("/api/datasources/uid/gs-reporting/health")
        self.assertEqual(datasource["status"], "OK")
        query = self.authenticated_request("/api/ds/query", {
            "from": "0", "to": "4102444800000",
            "queries": [{"refId": "A", "datasource": {"type": "mysql", "uid": "gs-reporting"},
                         "rawSql": "SELECT SUM(meter_count) AS registered_meters FROM v_fleet_health",
                         "format": "table"}]})
        self.assertNotIn("error", query["results"]["A"])
        self.assertTrue(query["results"]["A"]["frames"])

    def test_anonymous_dashboard_access_is_denied(self) -> None:
        with self.assertRaises(HTTPError) as denied:
            urlopen("http://127.0.0.1:3000/api/search?type=dash-db", timeout=10)
        self.assertIn(denied.exception.code, (401, 403))


if __name__ == "__main__":
    unittest.main()

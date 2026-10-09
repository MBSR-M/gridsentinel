"""Configuration, retries, logging and bounded resource regression tests."""

import io
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import ConfigurationError, PoolTimeoutError, ValidationError
from gridsentinel.core.logging import JsonFormatter, sanitize
from gridsentinel.core.retry import RetryPolicy
from gridsentinel.infrastructure.mysql.database import Connection, ConnectionPool, Database


class ConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        (self.root / "base.ini").write_text("[mysql]\nhost=base\nport=3306\n[example]\nnumber=4\nflag=yes\nlist=a, b\n", encoding="utf-8")
        (self.root / "development.ini").write_text("[mysql]\nhost=development\n", encoding="utf-8")

    def test_precedence_allowlist_and_defaults(self) -> None:
        config = ConfigManager(self.root, overrides={"mysql.host": "override"},
                               environ={"GS_MYSQL_HOST": "environment", "GS_EXAMPLE_NUMBER": "999"})
        self.assertEqual(config.get("mysql", "host"), "override")
        self.assertEqual(config.get_int("example", "number"), 4)
        self.assertTrue(config.get_bool("example", "flag"))
        self.assertTrue(config.get("example", "flag", cast=bool))
        self.assertEqual(config.get_list("example", "list"), ["a", "b"])
        self.assertIsNone(config.get("missing", "option"))
        self.assertEqual(config.get("missing", "option", 9), 9)
        self.assertEqual(ConfigManager(self.root, environ={}).get("mysql", "host"), "development")
        self.assertEqual(ConfigManager(self.root, environ={"GS_MYSQL_HOST": "env"}).get("mysql", "host"), "env")

    def test_malformed_values_never_become_defaults(self) -> None:
        config = ConfigManager(self.root, overrides={"example.number": "broken"}, environ={})
        with self.assertRaises(ConfigurationError):
            config.get_int("example", "number", 9)
        with self.assertRaises(ConfigurationError):
            ConfigManager(self.root, overrides={"mysql.port": "invalid"}, environ={})
        with self.assertRaises(ConfigurationError):
            config.get_required("missing", "option")

    def test_atomic_reload_and_immutable_snapshot(self) -> None:
        config = ConfigManager(self.root, environ={})
        snapshot = config.snapshot(False)
        with self.assertRaises(TypeError):
            snapshot["mysql"]["host"] = "mutate"
        (self.root / "development.ini").write_text("[mysql]\nhost=new\n", encoding="utf-8")
        config.reload()
        self.assertEqual(snapshot["mysql"]["host"], "development")
        self.assertEqual(config.get("mysql", "host"), "new")
        (self.root / "development.ini").write_text("[mysql]\nport=broken\n", encoding="utf-8")
        with self.assertRaises(ConfigurationError):
            config.reload()
        self.assertEqual(config.get("mysql", "host"), "new")

    def test_secret_file_and_redacted_snapshot(self) -> None:
        (self.root / "secret").write_text("private-value\n", encoding="utf-8")
        config = ConfigManager(self.root, environ={"GS_MYSQL_PASSWORD_FILE": "secret"})
        self.assertEqual(config.get_required("mysql", "password"), "private-value")
        self.assertEqual(config.snapshot()["mysql"]["password"], "[REDACTED]")
        config = ConfigManager(self.root, overrides={"mysql.password": "override"},
                               environ={"GS_MYSQL_PASSWORD_FILE": "secret"})
        self.assertEqual(config.get("mysql", "password"), "override")
        config = ConfigManager(self.root, overrides={"mysql.password": "override"},
                               environ={"GS_MYSQL_PASSWORD_FILE": "absent-secret"})
        self.assertEqual(config.get("mysql", "password"), "override")

    def test_production_security_validation(self) -> None:
        with self.assertRaises(ConfigurationError):
            ConfigManager(self.root, "production", {"security.cookie_secure": "false"}, {})
        with self.assertRaises(ConfigurationError):
            ConfigManager(self.root, "production", {"mysql.require_tls": "true", "kafka.security_protocol": "PLAINTEXT"}, {})
        secure = ConfigManager(self.root, "production", {
            "mysql.require_tls": "true", "kafka.security_protocol": "SSL",
            "rabbitmq.ssl": "true", "redis.ssl": "true"}, {})
        self.assertEqual(secure.environment, "production")


class RetryTests(unittest.TestCase):
    def test_bounded_jitter_and_exception_classification(self) -> None:
        operation = Mock(side_effect=[TimeoutError(), TimeoutError(), "ok"])
        sleeps = []
        result = RetryPolicy(3, 2, 3).run(operation, sleep=sleeps.append, uniform=lambda low, high: high)
        self.assertEqual(result, "ok")
        self.assertEqual(sleeps, [2, 3])
        operation = Mock(side_effect=ValidationError("bad input"))
        with self.assertRaises(ValidationError):
            RetryPolicy().run(operation)
        self.assertEqual(operation.call_count, 1)


class LoggingTests(unittest.TestCase):
    def test_recursive_redaction_and_exception_sanitization(self) -> None:
        self.assertEqual(sanitize({"password": "private", "nested": {"refresh_token": "private"}}),
                         {"password": "[REDACTED]", "nested": {"refresh_token": "[REDACTED]"}})
        formatter = JsonFormatter("test", "test", "local")
        record = logging.LogRecord("test", logging.ERROR, __file__, 1,
                                   "Authorization: Bearer private-token", (), None)
        payload = json.loads(formatter.format(record))
        self.assertNotIn("private-token", payload["message"])


class PoolTests(unittest.TestCase):
    def test_fetch_cursors_are_released_before_next_page(self) -> None:
        raw = Mock()
        raw.cursor.return_value.fetchall.return_value = [{"id": 1}]
        connection = Connection(raw)
        for _ in range(10):
            self.assertEqual(connection.fetch_all("SELECT id FROM users LIMIT 1"), [{"id": 1}])
        self.assertEqual(connection._cursors, [])
        self.assertEqual(raw.cursor.return_value.close.call_count, 10)

    def test_bounded_pool_releases_and_closes(self) -> None:
        raw = Mock()
        factory = Mock(return_value=raw)
        pool = ConnectionPool(factory, size=1, timeout=0.01)
        lease = pool.acquire()
        with self.assertRaises(PoolTimeoutError):
            pool.acquire()
        pool.release(lease)
        self.assertIs(pool.acquire(), raw)
        self.assertEqual(factory.call_count, 1)
        pool.close()
        pool.release(raw)
        raw.close.assert_called_once()
        self.assertEqual(pool.metrics()["total"], 0)
        self.assertEqual(pool.stats(), {"size": 1, "total": 0, "idle": 0, "in_use": 0})

    def test_factory_failure_does_not_leak_capacity(self) -> None:
        raw = Mock()
        pool = ConnectionPool(Mock(side_effect=[ConnectionError(), raw]), size=1)
        with self.assertRaises(ConnectionError):
            pool.acquire()
        self.assertIs(pool.acquire(), raw)
        pool.release(raw)
        pool.close()

    def test_transaction_commit_and_rollback(self) -> None:
        raw = Mock()
        config = Mock()
        config.get_int.return_value = 1
        config.get_float.return_value = 1.0
        database = Database(config, factory=lambda: raw)
        with database.transaction() as connection:
            connection.execute("SELECT %s", ("untrusted",))
        raw.commit.assert_called_once()
        with self.assertRaises(ValueError):
            with database.transaction():
                raise ValueError("failure")
        raw.rollback.assert_called_once()
        database.close()


if __name__ == "__main__":
    unittest.main()

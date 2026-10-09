"""Source poison recovery, checkpoint ordering and immutable export safeguards."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock
import unittest

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import ConfigurationError
from gridsentinel.domain.events import stamp
from gridsentinel.workers.connector import ConnectorRunner

ROOT = Path(__file__).resolve().parents[2]


def event() -> dict:
    """Create synthetic current-time canonical communication telemetry."""
    now = stamp(datetime.now(timezone.utc))
    return {"schema_version": "1.0", "event_id": "synthetic-event", "event_type": "METER_COMM_SUCCESS",
            "source_system": "synthetic-hes", "tenant_id": "demo", "meter_id": "synthetic-meter",
            "event_time_utc": now, "received_at_utc": now, "ingested_at_utc": now,
            "correlation_id": "synthetic-correlation", "trace_id": "synthetic-trace", "payload": {}}


class ConnectorRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "export.jsonl"
        self.config = ConfigManager(ROOT / "config", overrides={"connector:hes.enabled": "true", "connector:hes.mode": "file", "connector:hes.path": str(self.path), "connector:hes.source_shard": "batch-1"}, environ={})
        self.checkpoints, self.published, self.order = [], [], []
        connection, database = Mock(), Mock()
        connection.fetch_one.side_effect = lambda *args: self.checkpoints[-1] if self.checkpoints else None
        def execute(sql, params):
            self.order.append("checkpoint")
            self.checkpoints.append({"watermark": params[3], "source_event_id": params[4], "checksum": params[5]})
        connection.execute.side_effect = execute
        @contextmanager
        def scope():
            yield connection
        database.read, database.transaction = scope, scope
        self.publisher = Mock()
        def publish(topic, key, payload):
            self.order.append("confirmed")
            self.published.append((topic, key, payload))
        self.publisher.publish.side_effect = publish
        self.runner = ConnectorRunner(SimpleNamespace(config=self.config, database=database, stop=threading.Event()), "hes", self.publisher)

    def test_poison_is_confirmed_then_skipped_and_next_record_is_delivered(self) -> None:
        self.path.write_text("broken-json\n" + json.dumps(event()) + "\n", encoding="utf-8")
        self.assertEqual(self.runner.extract(), 2)
        self.assertEqual(self.order, ["confirmed", "checkpoint", "confirmed", "checkpoint"])
        self.assertEqual(self.published[0][0], "ami.errors.invalid")
        self.assertEqual(self.published[0][2]["source_position"], "1")
        self.assertNotIn("payload", self.published[0][2])
        self.assertEqual(self.checkpoints[-1]["watermark"], "2")
        self.assertEqual(self.runner.extract(), 0)

    def test_failed_dlq_confirmation_does_not_checkpoint_or_skip(self) -> None:
        self.path.write_text("broken-json\n", encoding="utf-8")
        self.publisher.publish.side_effect = ConnectionError("broker unavailable")
        with self.assertRaises(ConnectionError):
            self.runner.extract()
        self.assertEqual(self.checkpoints, [])

    def test_invalid_tenant_records_do_not_leak_raw_payload(self) -> None:
        raw = event()
        raw["tenant_id"] = "other-tenant"
        raw["payload"] = {"sensitive": "must not reach broker"}
        self.runner.handoff("1", raw)
        self.assertEqual(self.published[0][0], "ami.errors.invalid")
        self.assertEqual(self.published[0][2]["tenant_id"], "demo")
        self.assertNotIn("must not reach broker", json.dumps(self.published))

    def test_oversized_line_is_drained_without_corrupting_next_position(self) -> None:
        self.path.write_text("x" * 70000 + "\n" + json.dumps(event()) + "\n", encoding="utf-8")
        self.assertEqual(self.runner.extract(), 2)
        self.assertEqual(self.published[0][2]["source_position"], "1")
        self.assertEqual(self.checkpoints[-1]["watermark"], "2")

    def test_changed_file_refuses_unsafe_checkpoint_reuse(self) -> None:
        self.path.write_text(json.dumps(event()) + "\n", encoding="utf-8")
        self.runner.extract()
        self.path.write_text(json.dumps(event()) + "\nnew-line\n", encoding="utf-8")
        with self.assertRaises(ConfigurationError):
            self.runner.extract()

    def test_mysql_driver_values_require_explicit_timezone(self) -> None:
        with self.assertRaises(ConfigurationError):
            self.runner._source_values({"at": datetime(2026, 1, 1)})
        self.runner.config = ConfigManager(ROOT / "config", overrides={"connector:hes.source_timezone": "UTC"}, environ={})
        values = self.runner._source_values({"at": datetime(2026, 1, 1), "kwh": Decimal("12.34")})
        self.assertEqual(values, {"at": "2026-01-01T00:00:00.000000Z", "kwh": 12.34})

    def test_source_tie_breaker_is_not_replaced_by_canonical_event_id(self) -> None:
        self.runner.handoff("2026-01-01T00:00:00", event(), source_event_id="source-row-99")
        self.assertEqual(self.checkpoints[-1]["source_event_id"], "source-row-99")


if __name__ == "__main__":
    unittest.main()

"""Durability-order tests for Kafka and safe optional-cache behavior."""

from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import threading
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock
import unittest

from kafka import TopicPartition

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.infrastructure.kafka import KafkaPublisher, TelemetryWorker, topics
from gridsentinel.infrastructure.redis import RedisCache
from gridsentinel.workers.outbox import publish_kafka

ROOT = Path(__file__).resolve().parents[2]


class QuickStop(threading.Event):
    """Record backoff without waiting during deterministic failure tests."""

    def wait(self, timeout: float | None = None) -> bool:
        return self.is_set()


class KafkaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ConfigManager(ROOT / "config", environ={})

    def worker(self, value: bytes, operations: Mock, publisher: Mock) -> tuple[TelemetryWorker, Mock, list]:
        stop = QuickStop()
        partition = TopicPartition("synthetic-topic", 0)
        record = SimpleNamespace(topic=partition.topic, partition=0, offset=6, value=value)
        consumer = Mock()
        consumer.poll.return_value = {partition: [record]}
        consumer.assignment.return_value = []
        order = []
        consumer.commit.side_effect = lambda values: order.append("commit")
        operations.ingest.side_effect = lambda event: order.append("durable_effect")
        publisher.publish.side_effect = lambda *args: order.append("confirmed_dlq")
        worker = TelemetryWorker(self.config, operations, publisher, stop,
                                 heartbeat=lambda metrics: stop.set(), consumer_factory=lambda *args, **kwargs: consumer)
        return worker, consumer, order

    def test_effects_precede_manual_offset_commit(self) -> None:
        operations, publisher = Mock(), Mock()
        worker, consumer, order = self.worker(b'{"tenant_id":"demo"}', operations, publisher)
        worker.run()
        self.assertEqual(order, ["durable_effect", "commit"])
        self.assertEqual(next(iter(consumer.commit.call_args.args[0].values())).offset, 7)
        publisher.publish.assert_not_called()
        consumer.close.assert_called_once_with(autocommit=False)

    def test_duplicate_keys_nonfinite_and_deep_json_are_poison(self) -> None:
        for value in (b'{"tenant_id":"demo","tenant_id":"other"}',
                      b'{"tenant_id":"demo","value":NaN}', b"[" * 1200 + b"]" * 1200,
                      b'{"tenant_id":"other"}', b"broken-json"):
            operations, publisher = Mock(), Mock()
            worker, consumer, order = self.worker(value, operations, publisher)
            worker.run()
            self.assertEqual(order, ["confirmed_dlq", "commit"])
            operations.ingest.assert_not_called()
            deadletter = publisher.publish.call_args.args[2]
            self.assertEqual(deadletter["tenant_id"], "demo")
            self.assertEqual(deadletter["source_offset"], 6)
            self.assertIn("correlation_id", deadletter)
            self.assertNotIn("payload", deadletter)

    def test_no_offset_commit_when_effect_or_dlq_handoff_fails(self) -> None:
        for poison in (False, True):
            operations, publisher = Mock(), Mock()
            worker, consumer, order = self.worker(b"bad-json" if poison else b'{"tenant_id":"demo"}', operations, publisher)
            if poison:
                publisher.publish.side_effect = ConnectionError("broker unavailable")
            else:
                operations.ingest.side_effect = ConnectionError("database unavailable")
            worker.run()
            consumer.commit.assert_not_called()
            consumer.seek.assert_called_once_with(TopicPartition("synthetic-topic", 0), 6)

    def test_publisher_waits_for_confirmation_and_rejects_oversize(self) -> None:
        producer = Mock()
        publisher = KafkaPublisher(self.config, producer)
        publisher.publish("synthetic.topic", "meter-1", {"b": 2, "a": 1})
        producer.send.assert_called_once_with("synthetic.topic", key=b"meter-1", value=b'{"a":1,"b":2}')
        producer.send.return_value.get.assert_called_once_with(timeout=15)
        with self.assertRaises(ValidationError):
            publisher.publish("synthetic.topic", "meter-1", {"value": float("nan")})
        with self.assertRaises(ValidationError):
            publisher.publish("synthetic.topic", "meter-1", {"value": "x" * 262144})

    def test_topic_configuration_precedence(self) -> None:
        config = ConfigManager(ROOT / "config", overrides={"kafka.raw_hes_topic": "legacy.hes", "kafka_topics.communication": "custom.communication"}, environ={})
        self.assertEqual(topics(config)["communication"], "custom.communication")
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, "base.ini").write_text("[kafka]\nraw_hes_topic=legacy.hes\n", encoding="utf-8")
            legacy = ConfigManager(directory, environ={})
            self.assertEqual(topics(legacy)["raw_hes"], "legacy.hes")


class RedisTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ConfigManager(ROOT / "config", environ={})
        self.client = Mock()
        self.cache = RedisCache(self.config, self.client)

    def test_tenant_namespace_collision_resistance_and_ttl(self) -> None:
        first = self.cache.key("meter:state", "tenant:a", "b")
        second = self.cache.key("meter:state", "tenant", "a:b")
        self.assertNotEqual(first, second)
        self.assertNotEqual(self.cache.key("meter:state", "tenant", "%3A"), self.cache.key("meter:state", "tenant", ":"))
        self.cache.set("meter:state", "tenant", "meter", {"state": "HEALTHY"}, 15)
        self.assertEqual(self.client.set.call_args.kwargs["ex"], 15)
        for ttl in (0, 86401, True):
            with self.assertRaises(ValueError):
                self.cache.set("meter:state", "tenant", "meter", {}, ttl)
        with self.assertRaises(ValueError):
            self.cache.get("unapproved", "tenant", "meter")

    def test_corrupt_or_oversized_cache_values_are_misses(self) -> None:
        for value in ("not-json", '{"score":NaN}', "x" * 262145, b"\xff"):
            self.client.get.return_value = value
            self.assertIsNone(self.cache.get("meter:state", "tenant", "meter"))
        self.client.get.return_value = '{"state":"HEALTHY"}'
        self.assertEqual(self.cache.get("meter:state", "tenant", "meter"), {"state": "HEALTHY"})

    def test_redis_outage_provides_miss_and_local_rate_fallback(self) -> None:
        for method in (self.client.get, self.client.set, self.client.delete, self.client.eval, self.client.ping):
            method.side_effect = ConnectionError("unavailable")
        self.assertIsNone(self.cache.get("meter:state", "tenant", "meter"))
        self.assertFalse(self.cache.set("meter:state", "tenant", "meter", {}))
        self.assertFalse(self.cache.invalidate("meter:state", "tenant", "meter"))
        self.assertIsNone(self.cache.rate_limit("tenant", "ip", 5, 60))
        self.assertFalse(self.cache.health())

    def test_cache_bad_inputs_do_not_become_dependency_failures(self) -> None:
        with self.assertRaises(ValueError):
            self.cache.set("dashboard", "tenant", "view", {"value": float("nan")})
        with self.assertRaises(ValueError):
            self.cache.rate_limit("tenant", "ip", 0, 60)
        self.client.set.assert_not_called()


class OutboxTests(unittest.TestCase):
    def test_broker_failure_rolls_back_publication_markers(self) -> None:
        connection, database, publisher = Mock(), Mock(), Mock()
        connection.fetch_all.return_value = [{"outbox_id": "id-1", "topic": "t", "partition_key": "key", "payload": '{"value":1}'}]
        transaction_events = []
        @contextmanager
        def transaction():
            try:
                yield connection
                transaction_events.append("commit")
            except Exception:
                transaction_events.append("rollback")
                raise
        database.transaction = transaction
        publisher.publish.side_effect = ConnectionError("publisher confirmation failed")
        with self.assertRaises(ConnectionError):
            publish_kafka(database, publisher, "tenant")
        self.assertEqual(transaction_events, ["rollback"])
        connection.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()

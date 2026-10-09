"""Opt-in real Kafka/MySQL offset durability and Redis cache recovery evidence."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
from unittest.mock import Mock
import unittest
import uuid

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.domain.events import stamp
from gridsentinel.infrastructure.kafka import KafkaPublisher, TelemetryWorker, TOPIC_OPTIONS, connection_options, provision_topics, topics
from gridsentinel.infrastructure.mysql import Database
from gridsentinel.infrastructure.redis import RedisCache
from gridsentinel.processing.operations import OperationsService
from gridsentinel.services.audit import AuditService
from gridsentinel.workers.outbox import publish_kafka


@unittest.skipUnless(os.environ.get("GS_RUN_INFRASTRUCTURE_INTEGRATION") == "1",
                     "set GS_RUN_INFRASTRUCTURE_INTEGRATION=1 with local Kafka, Redis and MySQL")
class InfrastructureIntegrationTests(unittest.TestCase):
    """Use private topic names and a private synthetic tenant without broker resets."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[2]
        cls.tenant = "infra-test-" + uuid.uuid4().hex[:12]
        prefix = "gs.test." + uuid.uuid4().hex[:12] + "."
        cls.config = ConfigManager(cls.root / "config", overrides={
            "application.tenant_id": cls.tenant,
            "mysql.password_file": str(cls.root / ".local/secrets/mysql_password"),
            "redis.password_file": str(cls.root / ".local/secrets/redis_password"),
            "kafka.bootstrap_servers": os.environ.get("GS_TEST_KAFKA_SERVERS", "127.0.0.1:19092"),
            "kafka.partitions": 1,
            "kafka.consumer_group": prefix + "consumer",
            **{"kafka_topics." + key: prefix + key for key in TOPIC_OPTIONS},
        })
        cls.database = Database(cls.config)
        with tempfile.TemporaryDirectory() as directory:
            for name in ("001_foundation.sql", "002_operations.sql"):
                shutil.copyfile(cls.root / "database/migrations" / name, Path(directory) / name)
            cls.database.migrate(directory)
        with cls.database.transaction() as connection:
            connection.execute("INSERT INTO tenants(tenant_id,display_name,created_at) VALUES(%s,%s,UTC_TIMESTAMP(6))", (cls.tenant, "Synthetic infrastructure verification"))
        cls.operations = OperationsService(cls.database, cls.config, AuditService(cls.database))
        provision_topics(cls.config)
        cls.publisher = KafkaPublisher(cls.config)
        cls.cache = RedisCache(cls.config)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.publisher.close()
        cls.cache.close()
        admin = KafkaAdminClient(**connection_options(cls.config))
        try:
            admin.delete_topics(list(topics(cls.config).values()), timeout_ms=15000)
        finally:
            admin.close()
            cls.database.close()

    def event(self, meter: str) -> dict:
        now = stamp(datetime.now(timezone.utc))
        return {"schema_version": "1.0", "event_id": "synthetic-" + uuid.uuid4().hex,
                "event_type": "METER_COMM_SUCCESS", "source_system": "synthetic-infrastructure",
                "tenant_id": self.tenant, "meter_id": meter,
                "event_time_utc": now, "received_at_utc": now, "ingested_at_utc": now,
                "correlation_id": "test-" + uuid.uuid4().hex, "trace_id": "test-" + uuid.uuid4().hex,
                "payload": {}}

    def test_confirmed_outbox_has_idempotent_mysql_effects(self) -> None:
        event = self.event("synthetic-outbox-meter")
        self.assertFalse(self.operations.ingest(event)["duplicate"])
        self.assertTrue(self.operations.ingest(event)["duplicate"])
        failed = Mock()
        failed.publish.side_effect = ConnectionError("Injected broker confirmation failure")
        with self.assertRaises(ConnectionError):
            publish_kafka(self.database, failed, self.tenant)
        with self.database.read() as connection:
            row = connection.fetch_one("SELECT COUNT(*) AS count FROM outbox_events WHERE tenant_id=%s AND published_at IS NULL", (self.tenant,))
        self.assertEqual(row["count"], 2)
        self.assertEqual(publish_kafka(self.database, self.publisher, self.tenant), 2)
        self.assertEqual(publish_kafka(self.database, self.publisher, self.tenant), 0)
        consumer = KafkaConsumer(topics(self.config)["communication"], **connection_options(self.config),
                                 group_id=None, auto_offset_reset="earliest", enable_auto_commit=False)
        try:
            deadline, found = time.monotonic() + 15, []
            while time.monotonic() < deadline and not found:
                for batch in consumer.poll(timeout_ms=500).values():
                    found.extend(json.loads(record.value) for record in batch if json.loads(record.value).get("event_id") == event["event_id"])
            self.assertEqual(len(found), 1)
        finally:
            consumer.close(autocommit=False)

    def test_kafka_duplicates_and_poison_commit_only_after_durable_handoff(self) -> None:
        event = self.event("synthetic-kafka-meter")
        source_topic = topics(self.config)["raw_hes"]
        self.publisher.publish(source_topic, event["meter_id"], event)
        self.publisher.publish(source_topic, event["meter_id"], event)
        self.publisher.producer.send(source_topic, value=b'{"tenant_id":"ambiguous","tenant_id":"other"}', partition=0).get(timeout=15)
        stop, observations, errors = threading.Event(), [], []
        def heartbeat(metrics):
            observations.append(metrics)
            if metrics["processed"] >= 2 and metrics["invalid"] >= 1:
                stop.set()
        worker = TelemetryWorker(self.config, self.operations, self.publisher, stop, heartbeat)
        def run():
            try:
                worker.run()
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        thread.join(timeout=25)
        stop.set()
        thread.join(timeout=5)
        self.assertFalse(thread.is_alive(), "Consumer did not shut down")
        self.assertEqual(errors, [])
        self.assertTrue(observations and observations[-1]["processed"] == 2 and observations[-1]["invalid"] == 1)
        with self.database.read() as connection:
            row = connection.fetch_one("SELECT COUNT(*) AS count FROM communication_events WHERE tenant_id=%s AND meter_id=%s", (self.tenant, event["meter_id"]))
        self.assertEqual(row["count"], 1)
        consumer = KafkaConsumer(**connection_options(self.config), group_id=self.config.get("kafka", "consumer_group"), enable_auto_commit=False)
        try:
            self.assertEqual(consumer.committed(TopicPartition(source_topic, 0)), 3)
        finally:
            consumer.close(autocommit=False)
        invalid = KafkaConsumer(topics(self.config)["invalid"], **connection_options(self.config), group_id=None,
                                enable_auto_commit=False, auto_offset_reset="earliest")
        try:
            deadline, references = time.monotonic() + 15, []
            while time.monotonic() < deadline and not references:
                for batch in invalid.poll(timeout_ms=500).values():
                    references.extend(json.loads(record.value) for record in batch)
            self.assertEqual(references[0]["source_offset"], 2)
            self.assertEqual(references[0]["tenant_id"], self.tenant)
            self.assertNotIn("payload", references[0])
        finally:
            invalid.close(autocommit=False)

    def test_real_redis_namespace_ttl_corruption_and_outage_fallback(self) -> None:
        entity = "synthetic-" + uuid.uuid4().hex
        self.assertTrue(self.cache.health())
        self.assertTrue(self.cache.set("meter:state", self.tenant, entity, {"status": "HEALTHY"}, ttl=10))
        self.assertEqual(self.cache.get("meter:state", self.tenant, entity), {"status": "HEALTHY"})
        self.assertIsNone(self.cache.get("meter:state", "other-tenant", entity))
        self.assertTrue(0 < self.cache.client.ttl(self.cache.key("meter:state", self.tenant, entity)) <= 10)
        self.cache.client.set(self.cache.key("meter:state", self.tenant, entity), '{"status":NaN}', ex=10)
        self.assertIsNone(self.cache.get("meter:state", self.tenant, entity))
        self.assertTrue(self.cache.rate_limit(self.tenant, entity, 1, 10))
        self.assertFalse(self.cache.rate_limit(self.tenant, entity, 1, 10))
        offline_config = ConfigManager(self.root / "config", overrides={"redis.host": "127.0.0.1", "redis.port": 1})
        offline = RedisCache(offline_config)
        try:
            self.assertIsNone(offline.get("meter:state", self.tenant, entity))
            self.assertIsNone(offline.rate_limit(self.tenant, entity, 1, 10))
            self.assertFalse(offline.health())
        finally:
            offline.close()
            self.cache.invalidate("meter:state", self.tenant, entity)
            self.cache.invalidate("rate_limit", self.tenant, entity)


if __name__ == "__main__":
    unittest.main()

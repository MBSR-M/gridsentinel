"""Opt-in real MySQL/RabbitMQ durability, retry and lease recovery tests."""

from datetime import timedelta
import json
import os
from pathlib import Path
import secrets
import shutil
import tempfile
import threading
import time
import unittest
import uuid

import pika

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.infrastructure.mysql import Database
from gridsentinel.infrastructure.rabbitmq import RabbitWorker, connection_parameters
from gridsentinel.services.audit import AuditService
from gridsentinel.services.auth import AuthService
from gridsentinel.services.jobs.service import JOB_PERMISSIONS, JobService, utcnow


@unittest.skipUnless(os.environ.get("GS_RUN_RABBITMQ_INTEGRATION") == "1",
                     "set GS_RUN_RABBITMQ_INTEGRATION=1 with loopback MySQL and RabbitMQ")
class RabbitIntegrationTests(unittest.TestCase):
    """Use isolated durable queue names and a synthetic tenant for each suite."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.root = Path(__file__).resolve().parents[2]
        cls.tenant = "jobs-test-" + uuid.uuid4().hex[:12]
        queue_prefix = "test." + uuid.uuid4().hex[:12] + "."
        overrides = {
            "application.tenant_id": cls.tenant,
            "mysql.password_file": str(cls.root / ".local" / "secrets" / "mysql_password"),
            "rabbitmq.password_file": str(cls.root / ".local" / "secrets" / "rabbitmq_password"),
            "rabbitmq.host": os.environ.get("GS_TEST_RABBITMQ_HOST", "127.0.0.1"),
            "rabbitmq.port": os.environ.get("GS_TEST_RABBITMQ_PORT", "15672"),
            "rabbitmq.retry_base_delay_seconds": 1,
            "rabbitmq.retry_max_delay_seconds": 1,
            "rabbitmq.retry_attempts": 2,
            "rabbitmq.lease_seconds": 5,
            **{"rabbitmq_queues." + kind: queue_prefix + kind for kind in JOB_PERMISSIONS},
        }
        cls.config = ConfigManager(cls.root / "config", overrides=overrides)
        cls.database = Database(cls.config)
        with tempfile.TemporaryDirectory() as directory:
            for name in ("001_foundation.sql", "002_operations.sql", "004_platform.sql"):
                shutil.copyfile(cls.root / "database" / "migrations" / name, Path(directory) / name)
            cls.database.migrate(directory)
        cls.audit = AuditService(cls.database)
        auth = AuthService(cls.database, cls.config, cls.audit)
        password = secrets.token_urlsafe(32)
        auth.bootstrap_admin("job-verifier", password, cls.tenant)
        credentials = auth.login("job-verifier", password, cls.tenant, "127.0.0.1",
                                 "rabbitmq-integration", "jobs-suite-login")
        cls.principal = auth.authenticate(credentials["access_token"])
        cls.jobs = JobService(cls.database, cls.config, cls.audit)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            connection = pika.BlockingConnection(connection_parameters(cls.config))
            channel = connection.channel()
            for kind in JOB_PERMISSIONS:
                queue = cls.jobs.queue_for(kind)
                for name in (queue, queue + ".dlq", queue + ".retry.1"):
                    channel.queue_delete(queue=name)
            connection.close()
        finally:
            cls.database.close()

    def setUp(self) -> None:
        self.connection = pika.BlockingConnection(connection_parameters(self.config))
        self.worker = RabbitWorker(self.config, self.jobs, {}, threading.Event(), lambda state: None)
        self.worker.connection = self.connection
        self.worker.channel = self.connection.channel()
        self.worker.declare(self.worker.channel, [self.jobs.queue_for(kind) for kind in JOB_PERMISSIONS])
        self.addCleanup(self.close_connection)
        # Each test owns one synthetic tenant. Cancel leftover failure-injection
        # jobs, confirm their persisted intents, then clear only its private queues.
        with self.database.read() as connection:
            unfinished = connection.fetch_all("SELECT job_id FROM background_jobs WHERE tenant_id=%s AND status NOT IN ('SUCCEEDED','FAILED','CANCELED') LIMIT 100", (self.tenant,))
        for row in unfinished:
            self.jobs.job_action(self.principal, row["job_id"], "cancel", "test-cleanup")
        self.jobs.publish_outbox(self.worker.publish, self.tenant)
        for kind in JOB_PERMISSIONS:
            queue = self.jobs.queue_for(kind)
            for name in (queue, queue + ".dlq", queue + ".retry.1"):
                self.worker.channel.queue_purge(queue=name)

    def close_connection(self) -> None:
        if self.connection.is_open:
            self.connection.close()

    def create_job(self) -> dict:
        return self.jobs.create(self.principal, "report.generate", {"report_type": "overview"},
                                "jobs-test", str(uuid.uuid4()))

    def row(self, job_id: str) -> dict:
        with self.database.read() as connection:
            return connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND job_id=%s", (self.tenant, job_id))

    def delivery(self, queue: str, timeout: float = 5.0) -> tuple:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            method, properties, body = self.worker.channel.basic_get(queue=queue, auto_ack=False)
            if method is not None:
                return method, properties, body
            self.connection.process_data_events(time_limit=0.05)
            time.sleep(0.05)
        self.fail("Expected broker delivery did not arrive before timeout")

    def test_confirmed_persistent_job_executes_once_under_redelivery(self) -> None:
        job = self.create_job()
        calls = []
        self.worker.handlers["report.generate"] = lambda claimed: calls.append(claimed["job_id"]) or {"generated": True}
        self.jobs.publish_outbox(self.worker.publish, self.tenant)
        method, properties, body = self.delivery(self.jobs.queue_for("report.generate"))
        self.assertEqual(properties.delivery_mode, 2)
        self.worker.process_message(method, body)
        self.assertEqual(self.row(job["job_id"])["status"], "SUCCEEDED")
        # Re-publish a confirmed duplicate, exercising the durable terminal fence.
        self.worker.publish(self.jobs.queue_for("report.generate"), json.loads(body))
        method, _, body = self.delivery(self.jobs.queue_for("report.generate"))
        self.worker.process_message(method, body)
        self.assertEqual(calls, [job["job_id"]])

    def test_transient_failure_uses_ttl_retry_then_succeeds(self) -> None:
        job = self.create_job()
        attempts = []

        def handler(claimed):
            attempts.append(claimed["attempt_number"])
            if len(attempts) == 1:
                raise ConnectionError("Injected source outage")
            return {"completed": True}

        self.worker.handlers["report.generate"] = handler
        self.jobs.publish_outbox(self.worker.publish, self.tenant)
        method, _, body = self.delivery(self.jobs.queue_for("report.generate"))
        self.worker.process_message(method, body)
        self.assertEqual(self.row(job["job_id"])["status"], "RETRY")
        self.jobs.publish_outbox(self.worker.publish, self.tenant)
        method, _, body = self.delivery(self.jobs.queue_for("report.generate"))
        self.worker.process_message(method, body)
        self.assertEqual(attempts, [1, 2])
        self.assertEqual(self.row(job["job_id"])["status"], "SUCCEEDED")

    def test_exhaustion_is_visible_and_persisted_to_dlq(self) -> None:
        job = self.create_job()

        def unavailable(claimed):
            raise ConnectionError("Injected integration outage")

        self.worker.handlers["report.generate"] = unavailable
        for attempt in range(2):
            self.jobs.publish_outbox(self.worker.publish, self.tenant)
            method, _, body = self.delivery(self.jobs.queue_for("report.generate"))
            self.worker.process_message(method, body)
        row = self.row(job["job_id"])
        self.assertEqual(row["status"], "FAILED")
        self.assertEqual(row["attempt_number"], 2)
        self.jobs.publish_outbox(self.worker.publish, self.tenant)
        method, properties, body = self.delivery(self.jobs.queue_for("report.generate") + ".dlq")
        self.assertEqual(json.loads(body)["job_id"], job["job_id"])
        self.assertEqual(properties.delivery_mode, 2)
        self.worker.channel.basic_ack(method.delivery_tag)

    def test_broker_disconnect_leaves_durable_outbox_for_recovery(self) -> None:
        job = self.create_job()
        self.connection.close()
        with self.assertRaises(pika.exceptions.AMQPError):
            self.jobs.publish_outbox(self.worker.publish, self.tenant)
        with self.database.read() as connection:
            row = connection.fetch_one("SELECT published_at FROM outbox_events WHERE tenant_id=%s AND aggregate_id=%s AND event_type='JOB_QUEUED'", (self.tenant, job["job_id"]))
        self.assertIsNone(row["published_at"])
        self.assertEqual(self.row(job["job_id"])["status"], "PENDING")

    def test_crashed_worker_lease_recovers_and_stale_result_is_fenced(self) -> None:
        job = self.create_job()
        claimed = self.jobs.claim(self.tenant, job["job_id"])
        with self.database.transaction() as connection:
            connection.execute("UPDATE background_jobs SET locked_until=%s WHERE tenant_id=%s AND job_id=%s", (utcnow() - timedelta(seconds=1), self.tenant, job["job_id"]))
        self.assertEqual(self.jobs.recover_expired(self.tenant), 1)
        replacement = self.jobs.claim(self.tenant, job["job_id"])
        self.assertGreater(replacement["attempt_number"], claimed["attempt_number"])
        self.assertFalse(self.jobs.complete(claimed, {"stale": True}))
        self.assertTrue(self.jobs.complete(replacement, {"recovered": True}))

    def test_idempotency_and_tenant_scoping(self) -> None:
        key = str(uuid.uuid4())
        first = self.jobs.create(self.principal, "report.generate", {}, "idempotent", key)
        second = self.jobs.create(self.principal, "report.generate", {}, "idempotent", key)
        self.assertEqual(first["job_id"], second["job_id"])
        with self.database.read() as connection:
            count = connection.fetch_one("SELECT COUNT(*) AS count FROM outbox_events WHERE tenant_id=%s AND aggregate_id=%s", (self.tenant, first["job_id"]))["count"]
        self.assertEqual(count, 1)
        other = {**self.principal, "tenant_id": "another-tenant"}
        self.assertEqual(self.jobs.list_jobs(other, {})["items"], [])
        self.assertIsNone(self.jobs.claim("another-tenant", first["job_id"]))


if __name__ == "__main__":
    unittest.main()

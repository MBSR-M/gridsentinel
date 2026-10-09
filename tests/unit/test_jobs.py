"""Failure, fencing, broker confirmation and bounded retry contract tests."""

from datetime import datetime, timedelta
import json
import threading
import unittest
from unittest.mock import MagicMock, Mock
import uuid

from gridsentinel.core.exceptions import AuthorizationError, ConfigurationError, InfrastructureError
from gridsentinel.infrastructure.rabbitmq import RabbitWorker, connection_parameters
from gridsentinel.services.jobs.service import JobService


class TestConfig:
    """Small typed configuration double that preserves explicit fallback values."""

    def __init__(self, values: dict | None = None) -> None:
        self.values = {"application.tenant_id": "test-tenant", "rabbitmq.host": "localhost",
                       "rabbitmq.user": "test-user", "rabbitmq.password": "synthetic-only",
                       **(values or {})}

    def get(self, section: str, option: str, default=None):
        return self.values.get(section + "." + option, default)

    def get_required(self, section: str, option: str):
        return self.values[section + "." + option]

    def get_int(self, section: str, option: str, default=0):
        return int(self.get(section, option, default))

    def get_float(self, section: str, option: str, default=0):
        return float(self.get(section, option, default))

    def get_bool(self, section: str, option: str, default=False):
        return bool(self.get(section, option, default))


class JobServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 10, 1, 8)
        self.database = MagicMock()
        self.connection = self.database.transaction.return_value.__enter__.return_value
        self.audit = Mock()
        self.config = TestConfig()
        self.jobs = JobService(self.database, self.config, self.audit, lambda: self.now)
        self.job = {"job_id": str(uuid.uuid4()), "tenant_id": "test-tenant",
                    "job_type": "report.generate", "queue": "q.report.generate",
                    "status": "RUNNING", "payload": '{"type":"overview"}',
                    "correlation_id": "test-correlation", "idempotency_key": "request-1",
                    "created_at": self.now, "deadline": self.now + timedelta(hours=1),
                    "originating_user_id": 1, "attempt_number": 1, "max_attempts": 5,
                    "locked_until": self.now + timedelta(seconds=60), "next_retry_at": None}

    def test_unauthorized_creation_has_no_database_effect(self) -> None:
        with self.assertRaises(AuthorizationError):
            self.jobs.create({"id": 1, "tenant_id": "test-tenant", "permissions": []},
                             "report.generate", {}, "correlation", "idempotent")
        self.database.run_transaction.assert_not_called()

    def test_active_lease_and_terminal_redelivery_do_not_execute(self) -> None:
        for status in ("RUNNING", "SUCCEEDED", "FAILED", "CANCELED"):
            with self.subTest(status=status):
                self.connection.fetch_one.return_value = {**self.job, "status": status}
                self.assertIsNone(self.jobs.claim("test-tenant", self.job["job_id"]))
                self.connection.execute.assert_not_called()

    def test_expired_lease_claim_increases_fencing_generation(self) -> None:
        self.connection.fetch_one.return_value = {**self.job, "locked_until": self.now - timedelta(seconds=1)}
        claimed = self.jobs.claim("test-tenant", self.job["job_id"])
        self.assertEqual(claimed["attempt_number"], 2)
        self.assertEqual(claimed["payload"], {"type": "overview"})
        self.assertGreater(claimed["locked_until"], self.now)

    def test_stale_worker_cannot_commit_or_schedule_retry(self) -> None:
        self.connection.fetch_one.return_value = {**self.job, "attempt_number": 2}
        self.assertFalse(self.jobs.complete(self.job, {"done": True}))
        self.assertEqual(self.jobs.fail(self.job, "temporary"), "STALE")
        self.connection.execute.assert_not_called()
        self.audit.append.assert_not_called()

    def test_retry_writes_delayed_publish_intent_and_never_immediate_requeue(self) -> None:
        self.connection.fetch_one.return_value = dict(self.job)
        self.assertEqual(self.jobs.fail(self.job, "NetworkUnavailable"), "RETRY")
        inserts = [call for call in self.connection.execute.call_args_list
                   if "INSERT INTO outbox_events" in call.args[0]]
        self.assertEqual(len(inserts), 1)
        self.assertEqual(inserts[0].args[1][3], "q.report.generate.retry.2")
        self.assertEqual(json.loads(inserts[0].args[1][5])["job_id"], self.job["job_id"])
        self.assertEqual(self.audit.append.call_args.args[3], "JOB_RETRY_SCHEDULED")

    def test_exhaustion_creates_failed_record_and_dlq_intent(self) -> None:
        self.connection.fetch_one.return_value = {**self.job, "attempt_number": 5}
        exhausted = {**self.job, "attempt_number": 5}
        self.assertEqual(self.jobs.fail(exhausted, "NetworkUnavailable"), "FAILED")
        inserts = [call for call in self.connection.execute.call_args_list
                   if "INSERT INTO outbox_events" in call.args[0]]
        self.assertEqual(inserts[0].args[1][3], "q.report.generate.dlq")
        self.assertEqual(self.audit.append.call_args.args[3], "JOB_FAILED")

    def test_deadline_failure_is_terminal(self) -> None:
        self.connection.fetch_one.return_value = {**self.job, "deadline": self.now}
        self.assertFalse(self.jobs.complete(self.job, {"done": True}))
        self.assertEqual(self.audit.append.call_args.args[3], "JOB_FAILED")

    def test_failed_broker_publish_does_not_mark_outbox_published(self) -> None:
        row = {"topic": "q.report.generate", "payload": {"job_id": self.job["job_id"]},
               "outbox_id": "outbox-1"}
        self.connection.fetch_all.return_value = [row]
        publisher = Mock(side_effect=InfrastructureError("Broker unavailable"))
        with self.assertRaises(InfrastructureError):
            self.jobs.publish_outbox(publisher, "test-tenant")
        self.connection.execute.assert_not_called()
        self.assertEqual(self.database.transaction.return_value.__exit__.call_args.args[0], InfrastructureError)

    def test_publish_confirmation_precedes_published_timestamp(self) -> None:
        self.connection.fetch_all.return_value = [{"topic": "q.report.generate", "payload": {}, "outbox_id": "outbox-1"}]
        events = []
        self.connection.execute.side_effect = lambda *args: events.append("database-mark")
        self.assertEqual(self.jobs.publish_outbox(lambda *args: events.append("broker-confirmed"), "test-tenant"), 1)
        self.assertEqual(events, ["broker-confirmed", "database-mark"])

    def test_backoff_and_attempts_are_bounded(self) -> None:
        self.config.values.update({"rabbitmq.retry_attempts": 999,
                                   "rabbitmq.retry_base_delay_seconds": 2,
                                   "rabbitmq.retry_max_delay_seconds": 9})
        self.assertEqual(self.jobs.attempts(), 5)
        self.assertEqual([self.jobs.delay(n) for n in range(1, 6)], [2, 4, 8, 9, 9])
        self.assertEqual(self.jobs.delay(1000000), 9)


class RabbitWorkerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = TestConfig()
        self.jobs = Mock()
        self.jobs.queue_for.side_effect = lambda kind: "q." + kind
        self.jobs.delay.side_effect = lambda attempt: min(30, 2 ** attempt)
        self.jobs.claim.return_value = {"job_id": str(uuid.uuid4()), "job_type": "report.generate",
                                       "tenant_id": "test-tenant", "correlation_id": "test",
                                       "payload": {"type": "overview"}, "attempt_number": 1}
        self.stop = threading.Event()
        self.worker = RabbitWorker(self.config, self.jobs, {}, self.stop, Mock())
        self.worker.channel, self.worker.connection = Mock(), Mock()
        self.method = Mock(delivery_tag=10)
        self.envelope = json.dumps({"job_id": self.jobs.claim.return_value["job_id"],
                                    "tenant_id": "test-tenant"}).encode()

    def test_queue_topology_is_durable_with_fixed_ttl_and_dlq(self) -> None:
        channel = Mock()
        self.worker.declare(channel, ["q.report.generate"])
        declarations = channel.queue_declare.call_args_list
        self.assertTrue(all(call.kwargs["durable"] for call in declarations))
        retry = [call for call in declarations if ".retry." in call.kwargs["queue"]]
        self.assertTrue(retry)
        self.assertTrue(all(call.kwargs["arguments"]["x-message-ttl"] > 0 for call in retry))
        self.assertTrue(all(call.kwargs["arguments"]["x-dead-letter-routing-key"] == "q.report.generate" for call in retry))
        channel.confirm_delivery.assert_called_once()
        channel.basic_qos.assert_called_once_with(prefetch_count=10)

    def test_unconfirmed_publish_fails_and_messages_are_persistent(self) -> None:
        self.worker.channel.basic_publish.return_value = False
        with self.assertRaises(InfrastructureError):
            self.worker.publish("q.report.generate", self.jobs.claim.return_value)
        arguments = self.worker.channel.basic_publish.call_args.kwargs
        self.assertTrue(arguments["mandatory"])
        self.assertEqual(arguments["properties"].delivery_mode, 2)

    def test_missing_handler_fails_visibly_before_ack(self) -> None:
        events = []
        self.jobs.fail.side_effect = lambda *args, **kwargs: events.append("failed-durably")
        self.worker.channel.basic_ack.side_effect = lambda **kwargs: events.append("ack")
        self.worker.process_message(self.method, self.envelope)
        self.assertEqual(events, ["failed-durably", "ack"])
        self.jobs.fail.assert_called_once_with(self.jobs.claim.return_value, "HANDLER_NOT_CONFIGURED", retryable=False)

    def test_handler_failure_schedules_retry_before_ack(self) -> None:
        self.worker.handlers["report.generate"] = Mock(side_effect=ConnectionError("private credentials"))
        self.worker.process_message(self.method, self.envelope)
        self.jobs.fail.assert_called_once_with(self.jobs.claim.return_value, "ConnectionError", retryable=True)
        self.worker.channel.basic_ack.assert_called_once_with(delivery_tag=10)
        self.worker.channel.basic_nack.assert_not_called()

    def test_database_outage_leaves_delivery_unacknowledged(self) -> None:
        self.jobs.claim.side_effect = InfrastructureError("Database unavailable")
        with self.assertRaises(InfrastructureError):
            self.worker.process_message(self.method, self.envelope)
        self.worker.channel.basic_ack.assert_not_called()
        self.worker.channel.basic_reject.assert_not_called()

    def test_poison_and_cross_tenant_messages_are_dead_lettered(self) -> None:
        for body in (b"{", b"[]", b'{"tenant_id":"test-tenant","job_id":null}',
                     b'{"tenant_id":"another-tenant","job_id":"anything"}'):
            with self.subTest(body=body):
                self.worker.process_message(self.method, body)
        self.assertEqual(self.worker.channel.basic_reject.call_count, 4)
        self.assertTrue(all(call.kwargs["requeue"] is False
                            for call in self.worker.channel.basic_reject.call_args_list))
        self.jobs.claim.assert_not_called()

    def test_broker_outage_reconnects_and_respects_stop(self) -> None:
        attempts = []

        def unavailable(parameters):
            attempts.append(parameters)
            if len(attempts) == 2:
                self.stop.set()
            raise ConnectionError("Broker down")

        worker = RabbitWorker(self.config, self.jobs, {}, self.stop, Mock(), unavailable)
        # Interrupt the first backoff without sleeping one second in a unit test.
        self.stop.wait = Mock(return_value=False)
        worker.run(["q.report.generate"])
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.stop.wait.call_args_list[0].args, (1.0,))
        self.assertEqual(self.stop.wait.call_args_list[1].args, (2.0,))
        self.assertEqual(worker.heartbeat.call_count, 2)

    def test_ssl_flag_cannot_silently_use_plaintext(self) -> None:
        self.config.values["rabbitmq.ssl"] = True
        with self.assertRaises(ConfigurationError):
            connection_parameters(self.config)


if __name__ == "__main__":
    unittest.main()

"""Confirmed durable RabbitMQ publishing and bounded operational job workers."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import ssl
import threading
import time
from typing import Any, Callable, Mapping
import uuid

import pika

from gridsentinel.core.exceptions import ConfigurationError, GridSentinelError, InfrastructureError
from gridsentinel.services.jobs.service import JOB_PERMISSIONS, JobService, canonical

LOGGER = logging.getLogger(__name__)


def connection_parameters(config: Any) -> pika.ConnectionParameters:
    """Resolve injected secrets and optional CA-verified AMQP TLS configuration."""
    host = config.get_required("rabbitmq", "host")
    ca = config.get("rabbitmq", "ssl_ca", "")
    require_tls = (config.get_bool("rabbitmq", "require_tls", False)
                   or config.get_bool("rabbitmq", "ssl", False))
    if require_tls and not ca:
        raise ConfigurationError("RabbitMQ TLS requires a server CA file")
    tls = pika.SSLOptions(ssl.create_default_context(cafile=ca), host) if ca else None
    return pika.ConnectionParameters(
        host=host, port=config.get_int("rabbitmq", "port", 5671 if tls else 5672),
        virtual_host=config.get("rabbitmq", "virtual_host", "/"),
        credentials=pika.PlainCredentials(config.get_required("rabbitmq", "user"),
                                          config.get_required("rabbitmq", "password")),
        heartbeat=config.get_int("rabbitmq", "heartbeat_seconds", 30),
        blocked_connection_timeout=config.get_int("rabbitmq", "blocked_connection_timeout_seconds", 10),
        socket_timeout=config.get_float("rabbitmq", "socket_timeout_seconds", 5),
        stack_timeout=config.get_float("rabbitmq", "stack_timeout_seconds", 10),
        connection_attempts=1, ssl_options=tls)


class RabbitWorker:
    """One Pika I/O thread and one handler; scale using multiple worker processes.

    Handlers receive the authoritative claimed job, including payload and job_id.
    Handlers must make business effects idempotent using job_id. The database
    fences state transitions but cannot make arbitrary remote effects exactly-once.
    """

    def __init__(self, config: Any, job_service: JobService,
                 handlers: Mapping[str, Callable[[dict[str, Any]], Any]],
                 stop: threading.Event, heartbeat: Callable[[dict[str, Any]], None],
                 connection_factory: Callable[[Any], Any] = pika.BlockingConnection) -> None:
        self.config, self.jobs, self.handlers = config, job_service, dict(handlers)
        self.stop, self.heartbeat, self.connection_factory = stop, heartbeat, connection_factory
        self.tenant_id = config.get_required("application", "tenant_id")
        self.connection: Any = None
        self.channel: Any = None
        self.processed = 0

    def declare(self, channel: Any, queues: list[str]) -> None:
        """Declare persistent main, DLQ and fixed-TTL delayed retry tiers."""
        for queue in queues:
            channel.queue_declare(queue=queue + ".dlq", durable=True)
            channel.queue_declare(queue=queue, durable=True,
                                  arguments={"x-dead-letter-exchange": "",
                                             "x-dead-letter-routing-key": queue + ".dlq"})
            for delay in sorted({self.jobs.delay(attempt) for attempt in range(1, 6)}):
                channel.queue_declare(queue=queue + ".retry." + str(delay), durable=True,
                                      arguments={"x-message-ttl": delay * 1000,
                                                 "x-dead-letter-exchange": "",
                                                 "x-dead-letter-routing-key": queue})
        channel.basic_qos(prefetch_count=max(1, min(100, self.config.get_int("rabbitmq", "prefetch_count", 10))))
        channel.confirm_delivery()

    def publish(self, queue: str, envelope: dict[str, Any]) -> None:
        """Return only after a persistent mandatory publish receives confirmation."""
        result = self.channel.basic_publish(
            exchange="", routing_key=queue, body=canonical(envelope).encode(), mandatory=True,
            properties=pika.BasicProperties(content_type="application/json", content_encoding="utf-8",
                                            delivery_mode=2, message_id=envelope["job_id"],
                                            correlation_id=envelope["correlation_id"]))
        if result is False:
            raise InfrastructureError("RabbitMQ publish was not confirmed")

    def _execute(self, job: dict[str, Any]) -> None:
        """Maintain broker heartbeats and durable ownership while one handler runs."""
        handler = self.handlers.get(job["job_type"])
        if handler is None:
            self.jobs.fail(job, "HANDLER_NOT_CONFIGURED", retryable=False)
            return
        lease = max(3, self.config.get_int("rabbitmq", "lease_seconds", 60))
        renewed = time.monotonic()
        # A stopped process may be killed after its documented grace period;
        # durable leases recover the job, and injected effects must be idempotent.
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="job-handler") as executor:
            future = executor.submit(handler, job)
            owned = True
            while not future.done():
                self.connection.process_data_events(time_limit=0.1)
                if time.monotonic() - renewed >= lease / 3:
                    owned = self.jobs.renew(job)
                    renewed = time.monotonic()
                    self.heartbeat({"status": "RUNNING", "job_id": job["job_id"],
                                    "processed": self.processed})
                    if not owned:
                        # Python cannot safely terminate arbitrary handler code.
                        # Ignore its result; effect-level idempotency remains mandatory.
                        break
            try:
                result = future.result()
                if owned:
                    self.jobs.complete(job, result)
            except Exception as error:
                retryable = not isinstance(error, GridSentinelError) or error.retryable
                self.jobs.fail(job, type(error).__name__, retryable=retryable)

    def process_message(self, method: Any, body: bytes) -> None:
        """ACK only after durable effects; reject poison envelopes into the DLQ."""
        try:
            if len(body) > self.config.get_int("rabbitmq", "max_payload_bytes", 262144) + 2048:
                raise ValueError("Oversized job envelope")
            message = json.loads(body.decode("utf-8"))
            if not isinstance(message, dict) or message.get("tenant_id") != self.tenant_id:
                raise ValueError("Invalid job envelope scope")
            uuid.UUID(message["job_id"])
        except (ValueError, TypeError, KeyError, AttributeError, UnicodeDecodeError, RecursionError):
            LOGGER.warning("Invalid job envelope rejected", extra={"details": {"queue_delivery": method.delivery_tag}})
            self.channel.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return
        job = self.jobs.claim(self.tenant_id, message["job_id"])
        if job is not None:
            self._execute(job)
            self.processed += 1
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def run(self, queues: list[str] | None = None) -> None:
        """Reconnect with interruptible exponential delay; durable leases recover crashes."""
        allowed = {self.jobs.queue_for(kind) for kind in JOB_PERMISSIONS}
        selected = queues or sorted(allowed)
        if not selected or not set(selected).issubset(allowed):
            raise ConfigurationError("Unknown worker queue")
        delay = 1.0
        while not self.stop.is_set():
            try:
                self.connection = self.connection_factory(connection_parameters(self.config))
                self.channel = self.connection.channel()
                # All supported queues must exist because outbox rows may target
                # a different worker's queue or its retry tier.
                self.declare(self.channel, sorted(allowed))
                delay = 1.0
                while not self.stop.is_set():
                    self.jobs.recover_expired(self.tenant_id)
                    self.jobs.publish_outbox(self.publish, self.tenant_id)
                    had_message = False
                    for queue in selected:
                        if self.stop.is_set():
                            break
                        method, _, body = self.channel.basic_get(queue=queue, auto_ack=False)
                        if method is not None:
                            had_message = True
                            self.process_message(method, body)
                    self.connection.process_data_events(time_limit=0.1)
                    self.heartbeat({"status": "RUNNING", "processed": self.processed,
                                    "queues": selected})
                    if not had_message:
                        self.stop.wait(0.5)
            except ConfigurationError:
                raise
            except Exception as error:
                LOGGER.warning("Job worker dependency unavailable", extra={"details": {"exception_type": type(error).__name__}})
                try:
                    self.heartbeat({"status": "DEGRADED", "processed": self.processed,
                                    "error_code": type(error).__name__})
                except Exception:
                    pass
                self.stop.wait(delay)
                delay = min(30.0, delay * 2)
            finally:
                if self.connection is not None and self.connection.is_open:
                    try:
                        self.connection.close()
                    except Exception:
                        pass
                self.connection, self.channel = None, None


__all__ = ["RabbitWorker", "connection_parameters"]

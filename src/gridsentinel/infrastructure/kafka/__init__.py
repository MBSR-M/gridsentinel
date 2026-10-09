"""Confirmed Kafka publishing and manual partition-aware processing."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Any, Callable

from gridsentinel.core.exceptions import ValidationError

LOGGER = logging.getLogger(__name__)
TOPIC_OPTIONS = {
    "raw_hes": "ami.raw.hes", "raw_mdms": "ami.raw.mdms",
    "raw_network": "ami.raw.network", "raw_meter_events": "ami.raw.meter_events",
    "raw_gis_updates": "ami.raw.gis_updates", "communication": "ami.normalized.communication",
    "intervals": "ami.normalized.intervals", "network": "ami.normalized.network",
    "topology": "ami.normalized.topology", "meter_health": "ami.derived.meter_health",
    "gateway_health": "ami.derived.gateway_health", "incident_candidates": "ami.derived.incident_candidates",
    "incidents_created": "ami.incidents.created", "incidents_updated": "ami.incidents.updated",
    "incidents_resolved": "ami.incidents.resolved", "features": "ami.ml.features",
    "predictions": "ami.ml.predictions", "invalid": "ami.errors.invalid",
    "processing": "ami.errors.processing",
}
LEGACY_TOPIC_OPTIONS = {"raw_hes": "raw_hes_topic", "raw_mdms": "raw_mdms_topic",
                        "raw_network": "raw_network_topic", "raw_meter_events": "raw_meter_events_topic",
                        "raw_gis_updates": "raw_gis_topic", "communication": "normalized_communication_topic",
                        "intervals": "normalized_intervals_topic", "network": "normalized_network_topic",
                        "topology": "normalized_topology_topic", "invalid": "invalid_topic",
                        "processing": "processing_errors_topic"}


def strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys rather than accepting ambiguous telemetry."""
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON key")
        value[key] = item
    return value


def finite_constant(value: str) -> None:
    """Reject JSON extensions such as NaN and Infinity."""
    raise ValueError("Non-finite JSON value")


def topics(config: Any) -> dict[str, str]:
    """Resolve every topic from INI so names never leak into domain routing."""
    return {key: config.get("kafka_topics", key,
                           config.get("kafka", LEGACY_TOPIC_OPTIONS.get(key, key + "_topic"), default))
            for key, default in TOPIC_OPTIONS.items()}


def connection_options(config: Any) -> dict[str, Any]:
    """Use broker-supported TLS and SASL configuration, never custom crypto."""
    options: dict[str, Any] = {
        "bootstrap_servers": config.get_list("kafka", "bootstrap_servers", ["kafka:9092"]),
        "security_protocol": config.get("kafka", "security_protocol", "PLAINTEXT"),
        "request_timeout_ms": config.get_int("kafka", "request_timeout_ms", 30000),
    }
    if options["security_protocol"] in {"SSL", "SASL_SSL"}:
        options.update(ssl_cafile=config.get_required("kafka", "ssl_ca"), ssl_check_hostname=True)
    if options["security_protocol"].startswith("SASL"):
        options.update(sasl_mechanism=config.get("kafka", "sasl_mechanism", "PLAIN"),
                       sasl_plain_username=config.get_required("kafka", "username"),
                       sasl_plain_password=config.get_required("kafka", "password"))
    return options


class KafkaPublisher:
    """Await broker acknowledgments before declaring durable handoff."""

    def __init__(self, config: Any, producer: Any = None) -> None:
        if producer is None:
            from kafka import KafkaProducer
            producer = KafkaProducer(**connection_options(config), acks="all", retries=3,
                                     max_in_flight_requests_per_connection=1, max_block_ms=10000)
        self.producer = producer
        self.max_payload_bytes = config.get_int("kafka", "max_payload_bytes", 262144)

    def publish(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        """Serialize deterministically and wait for the broker to confirm."""
        if not isinstance(key, str) or not 1 <= len(key.encode()) <= 512:
            raise ValidationError("Invalid Kafka partition key")
        try:
            raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        except (ValueError, TypeError, RecursionError) as error:
            raise ValidationError("Kafka payload requires finite JSON") from error
        if len(raw) > self.max_payload_bytes:
            raise ValidationError("Kafka payload exceeds configured limit")
        self.producer.send(topic, key=key.encode(), value=raw).get(timeout=15)

    def close(self) -> None:
        """Flush bounded pending records before closing the producer."""
        self.producer.flush(timeout=15)
        self.producer.close(timeout=15)


def provision_topics(config: Any) -> None:
    """Create configured topics; never mutate existing partition layouts implicitly."""
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(**connection_options(config))
    try:
        existing = set(admin.list_topics())
        missing = [NewTopic(name, config.get_int("kafka", "partitions", 6),
                            config.get_int("kafka", "replication_factor", 1),
                            topic_configs={"retention.ms": str(config.get_int("kafka", "retention_hours", 168) * 3600000)})
                   for name in topics(config).values() if name not in existing]
        if missing:
            admin.create_topics(missing, timeout_ms=30000)
    finally:
        admin.close()


class TelemetryWorker:
    """Process synchronously per partition; committed effects precede offsets."""

    def __init__(self, config: Any, operations: Any, publisher: KafkaPublisher,
                 stop: threading.Event, heartbeat: Callable[[dict[str, Any]], None],
                 consumer_factory: Callable[..., Any] | None = None) -> None:
        self.config, self.operations, self.publisher = config, operations, publisher
        self.stop, self.heartbeat = stop, heartbeat
        self.consumer_factory = consumer_factory

    def _process(self, record: Any, names: dict[str, str]) -> bool:
        """Return true for a confirmed DLQ handoff, false for durable effects."""
        try:
            if record.value is None or len(record.value) > self.config.get_int("ingestion", "max_payload_bytes", 65536):
                raise ValidationError("Event exceeds payload limit or is a tombstone")
            try:
                event = json.loads(record.value.decode("utf-8"), object_pairs_hook=strict_object,
                                   parse_constant=finite_constant)
            except (ValueError, UnicodeDecodeError, RecursionError) as error:
                raise ValidationError("Invalid event JSON") from error
            tenant = self.config.get("application", "tenant_id", "demo")
            if not isinstance(event, dict) or event.get("tenant_id") != tenant:
                raise ValidationError("Event tenant does not match isolated deployment")
            self.operations.ingest(event)
            return False
        except ValidationError as error:
            identity = f"{record.topic}:{record.partition}:{record.offset}"
            correlation = "invalid-" + hashlib.sha256(identity.encode()).hexdigest()
            self.publisher.publish(names["invalid"], str(record.partition), {
                "tenant_id": self.config.get("application", "tenant_id", "demo"),
                "correlation_id": correlation, "trace_id": correlation,
                "source_topic": record.topic, "source_partition": record.partition,
                "source_offset": record.offset,
                "payload_sha256": hashlib.sha256(record.value or b"").hexdigest(),
                "error": error.code, "message": error.message,
            })
            return True

    def run(self) -> None:
        """Seek failed offsets and back off; rebalance redelivery is idempotent."""
        from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
        names = topics(self.config)
        source_topics = [names[name] for name in TOPIC_OPTIONS if name.startswith("raw_")]
        factory = self.consumer_factory or KafkaConsumer
        consumer = factory(*source_topics, **connection_options(self.config),
                                 group_id=self.config.get("kafka", "consumer_group", "gridsentinel-normalizer"),
                                 enable_auto_commit=False, auto_offset_reset=self.config.get("kafka", "auto_offset_reset", "earliest"),
                                 max_poll_records=self.config.get_int("kafka", "max_poll_records", 100),
                                 max_poll_interval_ms=300000)
        processed = invalid = 0
        try:
            while not self.stop.is_set():
                batches = consumer.poll(timeout_ms=1000)
                for partition, records in batches.items():
                    for record in records:
                        if self.stop.is_set():
                            break
                        try:
                            if self._process(record, names):
                                invalid += 1
                            else:
                                processed += 1
                        except Exception as error:
                            consumer.seek(partition, record.offset)
                            LOGGER.warning("Telemetry partition paused after processing failure",
                                           extra={"details": {"exception_type": type(error).__name__, "partition": record.partition}})
                            self.stop.wait(2)
                            break
                        consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "", -1)})
                lag = sum(max(0, end - consumer.position(p))
                          for p, end in consumer.end_offsets(consumer.assignment()).items()) if consumer.assignment() else 0
                self.heartbeat({"processed": processed, "invalid": invalid, "lag": lag})
        finally:
            consumer.close(autocommit=False)

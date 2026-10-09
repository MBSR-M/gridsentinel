"""Versioned canonical telemetry envelopes and deterministic serialization."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
import hashlib
import json
import re
from typing import Any, Mapping

from gridsentinel.core.exceptions import ValidationError

EVENT_TYPES = frozenset({
    "METER_COMM_SUCCESS", "METER_COMM_FAILURE", "METER_SILENT", "INTERVAL_AVAILABLE",
    "INTERVAL_MISSING", "POWER_FAILURE", "POWER_RESTORED", "METER_REBOOT",
    "CLOCK_SYNC_FAILURE", "CONFIGURATION_ERROR", "FIRMWARE_UPGRADE",
    "TAMPER_ALARM", "GATEWAY_DISCONNECTED", "GATEWAY_RESTORED",
    "DCU_DISCONNECTED", "NETWORK_REGISTRATION_FAILURE", "TOPOLOGY_UPDATE",
    "METER_INSTALLED", "METER_DECOMMISSIONED", "MDMS_VALIDATION_FAILURE",
})
IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")
ENVELOPE_FIELDS = frozenset({"schema_version", "event_id", "event_type",
    "source_system", "tenant_id", "meter_id", "event_time_utc",
    "received_at_utc", "ingested_at_utc", "correlation_id", "trace_id",
    "payload", "source_sequence", "raw_payload_reference"})


def utc(value: str | datetime) -> datetime:
    """Parse an explicitly UTC timestamp, rejecting naive or non-UTC input."""
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValidationError("Invalid UTC timestamp") from error
    if result.tzinfo is None or result.utcoffset() != timedelta(0):
        raise ValidationError("Timestamps require an explicit UTC offset")
    return result.astimezone(timezone.utc)


def stamp(value: datetime) -> str:
    """Serialize a database or aware timestamp in UTC."""
    return value.replace(tzinfo=timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> str:
    """Encode finite JSON values deterministically without executable objects."""
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("Payload must contain finite JSON values") from error


def identifier(value: Any, name: str) -> str:
    """Require a bounded identifier safe for source and tenant boundaries."""
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValidationError(f"Invalid {name}")
    return value


@dataclass(frozen=True)
class CanonicalEvent:
    """Immutable canonical event with source identity preserved."""
    schema_version: str
    event_id: str
    event_type: str
    source_system: str
    tenant_id: str
    meter_id: str | None
    event_time: datetime
    received_at: datetime
    ingested_at: datetime
    correlation_id: str
    trace_id: str
    payload_json: str
    source_sequence: int | None = None
    raw_payload_reference: str | None = None

    @property
    def payload(self) -> dict[str, Any]:
        """Return a copy so callers cannot mutate the validated event."""
        return json.loads(self.payload_json)

    @property
    def deduplication_key(self) -> str:
        """Bind source event identity to its tenant namespace."""
        return hashlib.sha256(canonical_json([self.tenant_id, self.source_system, self.event_id]).encode()).hexdigest()

    @property
    def partition_key(self) -> str:
        """Partition meter telemetry by meter and device telemetry by device."""
        return self.meter_id or self.payload.get("gateway_id") or self.payload.get("dcu_id")

    def to_dict(self) -> dict[str, Any]:
        """Return the public schema 1.0 JSON envelope."""
        result = {"schema_version": self.schema_version, "event_id": self.event_id,
            "event_type": self.event_type, "source_system": self.source_system,
            "tenant_id": self.tenant_id, "meter_id": self.meter_id,
            "event_time_utc": stamp(self.event_time), "received_at_utc": stamp(self.received_at),
            "ingested_at_utc": stamp(self.ingested_at), "correlation_id": self.correlation_id,
            "trace_id": self.trace_id, "payload": self.payload}
        if self.source_sequence is not None:
            result["source_sequence"] = self.source_sequence
        if self.raw_payload_reference:
            result["raw_payload_reference"] = self.raw_payload_reference
        return result

    def serialize(self) -> bytes:
        """Return deterministic UTF-8 bytes for transport and hashing."""
        return canonical_json(self.to_dict()).encode("utf-8")


def validate_event(data: Mapping[str, Any], max_bytes: int = 65536,
                   max_clock_skew_seconds: int = 300,
                   now: datetime | None = None) -> CanonicalEvent:
    """Validate envelope and event-specific fields before any durable effect."""
    if not isinstance(data, Mapping) or set(data) - ENVELOPE_FIELDS:
        raise ValidationError("Unknown event envelope fields")
    if len(canonical_json(dict(data)).encode()) > max_bytes:
        raise ValidationError("Event exceeds configured payload limit")
    if data.get("schema_version") != "1.0":
        raise ValidationError("Unsupported schema version")
    kind = data.get("event_type")
    if kind not in EVENT_TYPES:
        raise ValidationError("Unknown canonical event type; configure vendor mapping")
    fields = {name: identifier(data.get(name), name) for name in
              ("event_id", "source_system", "tenant_id", "correlation_id", "trace_id")}
    for name in ("source_system", "tenant_id"):
        if len(fields[name]) > 64:
            raise ValidationError(f"{name} exceeds 64 characters")
    meter = data.get("meter_id")
    if meter is not None:
        meter = identifier(meter, "meter_id")
        if len(meter) > 64:
            raise ValidationError("meter_id exceeds 64 characters")
    payload = data.get("payload")
    if not isinstance(payload, dict):
        raise ValidationError("Event payload must be an object")
    if meter is None and kind not in {"GATEWAY_DISCONNECTED", "GATEWAY_RESTORED",
                                     "DCU_DISCONNECTED", "NETWORK_REGISTRATION_FAILURE"}:
        raise ValidationError("This event requires meter_id")
    if meter is None and not (payload.get("gateway_id") or payload.get("dcu_id")):
        raise ValidationError("Device event requires gateway_id or dcu_id")
    for name in ("gateway_id", "dcu_id", "feeder_id", "transformer_id", "region_id", "sim_provider_id"):
        if payload.get(name) is not None:
            if len(identifier(payload[name], name)) > 64:
                raise ValidationError(f"{name} exceeds 64 characters")
    event_time = utc(data.get("event_time_utc"))
    received = utc(data.get("received_at_utc"))
    ingested = utc(data.get("ingested_at_utc"))
    reference = now or datetime.now(timezone.utc)
    if any(value.year<1000 or value>reference+timedelta(seconds=max_clock_skew_seconds) for value in (event_time,received,ingested)):
        raise ValidationError("Envelope exceeds timestamp range or future clock-skew limit")
    if received < event_time - timedelta(seconds=max_clock_skew_seconds) or ingested < received - timedelta(seconds=max_clock_skew_seconds):
        raise ValidationError("Envelope timestamp order exceeds clock-skew limit")
    sequence = data.get("source_sequence")
    if sequence is not None and (type(sequence) is not int or sequence < 0 or sequence > 9223372036854775807):
        raise ValidationError("source_sequence must be a nonnegative 64-bit integer")
    if kind.startswith("INTERVAL"):
        start, end = utc(payload.get("interval_start_utc")), utc(payload.get("interval_end_utc"))
        if end <= start or (end - start).total_seconds() > 86400:
            raise ValidationError("Interval bounds must span at most one day")
        if type(payload.get("estimation_flag", False)) is not bool:
            raise ValidationError("estimation_flag must be boolean")
        for name in ("hes_received_at", "mdms_received_at"):
            if payload.get(name) is not None:
                utc(payload[name])
        kwh = payload.get("kwh")
        if kwh is not None and (type(kwh) not in (int, float) or kwh < 0):
            raise ValidationError("kwh must be a nonnegative finite number")
    if kind == "TOPOLOGY_UPDATE":
        utc(payload.get("valid_from"))
        if payload.get("valid_to") and utc(payload["valid_to"]) <= utc(payload["valid_from"]):
            raise ValidationError("Invalid topology effective range")
    raw = data.get("raw_payload_reference")
    if raw is not None and (not isinstance(raw, str) or len(raw) > 255):
        raise ValidationError("Invalid raw payload reference")
    return CanonicalEvent("1.0", fields["event_id"], kind, fields["source_system"],
        fields["tenant_id"], meter, event_time, received, ingested,
        fields["correlation_id"], fields["trace_id"], canonical_json(payload), sequence, raw)

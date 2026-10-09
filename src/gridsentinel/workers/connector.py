"""INI-configured canonical-source ingestion with durable extraction checkpoints."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re
from pathlib import Path
from collections.abc import Iterator
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from gridsentinel.connectors.adapters import HttpConnector, MySQLIncrementalConnector, VendorNormalizer
from gridsentinel.core.exceptions import ConfigurationError, ValidationError
from gridsentinel.core.security import validate_identifier
from gridsentinel.domain.events import validate_event
from gridsentinel.infrastructure.kafka import finite_constant, strict_object, topics


class ConnectorRunner:
    """Incremental file/HTTP/MySQL extraction with broker-confirmed handoff."""

    def __init__(self, runtime: Any, source: str, publisher: Any) -> None:
        self.runtime, self.source, self.publisher = runtime, source, publisher
        self.config = runtime.config
        self.section = "connector:" + source
        self.tenant = self.config.get("application", "tenant_id", "demo")
        if source not in {"hes", "mdms", "network", "gis", "meter_events"}:
            raise ConfigurationError("Unknown connector source")
        self.shard = validate_identifier(self.config.get(self.section, "source_shard", "default"), 64)
        self.normalizer = None
        mapping = self.config.get_path(self.section, "vendor_mapping")
        if mapping:
            try:
                self.normalizer = VendorNormalizer.from_ini(mapping, self.tenant,
                    self.config.get_required(self.section, "source_system"),
                    self.config.get_required(self.section, "vendor"))
            except ValidationError as error:
                raise ConfigurationError("Connector vendor mapping is invalid") from error

    def checkpoint(self) -> dict[str, Any] | None:
        """Read the last durable handoff checkpoint."""
        with self.runtime.database.read() as connection:
            return connection.fetch_one("SELECT watermark,source_event_id,checksum FROM connector_checkpoints WHERE tenant_id=%s AND connector_id=%s AND source_shard=%s", (self.tenant, self.source, self.shard))

    def handoff(self, position: str, raw: dict[str, Any], checksum: str | None = None,
                source_event_id: str | None = None) -> None:
        """Confirm telemetry or its poison reference before advancing a checkpoint."""
        try:
            if self.normalizer:
                raw = self.normalizer.normalize(raw)
            event = validate_event(raw, self.config.get_int("ingestion", "max_payload_bytes", 65536))
            if event.tenant_id != self.tenant:
                raise ValidationError("Connector event belongs to another tenant")
        except ValidationError as error:
            encoded = json.dumps(raw, default=str, sort_keys=True, separators=(",", ":")).encode()
            self._invalid(position, hashlib.sha256(encoded).hexdigest(), error, checksum, source_event_id)
            return
        topic_key = {"hes": "raw_hes", "mdms": "raw_mdms", "network": "raw_network",
                     "gis": "raw_gis_updates", "meter_events": "raw_meter_events"}[self.source]
        self.publisher.publish(topics(self.config)[topic_key], event.partition_key, event.to_dict())
        self._checkpoint(position, source_event_id or event.event_id, checksum)

    def _checkpoint(self, position: str, source_event_id: str | None,
                    checksum: str | None) -> None:
        with self.runtime.database.transaction() as connection:
            connection.execute("INSERT INTO connector_checkpoints(tenant_id,connector_id,source_shard,watermark,source_event_id,last_successful_extract,checksum,updated_at) VALUES(%s,%s,%s,%s,%s,UTC_TIMESTAMP(6),%s,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE watermark=VALUES(watermark),source_event_id=VALUES(source_event_id),last_successful_extract=VALUES(last_successful_extract),checksum=VALUES(checksum),updated_at=VALUES(updated_at)", (self.tenant, self.source, self.shard, position, source_event_id, checksum))

    def _invalid(self, position: str, digest: str, error: ValidationError,
                 checksum: str | None = None, source_event_id: str | None = None) -> None:
        identity = f"{self.tenant}:{self.source}:{self.shard}:{position}:{digest}"
        correlation = "invalid-" + hashlib.sha256(identity.encode()).hexdigest()
        self.publisher.publish(topics(self.config)["invalid"], self.source + ":" + self.shard, {
            "tenant_id": self.tenant, "correlation_id": correlation, "trace_id": correlation,
            "connector_id": self.source, "source_shard": self.shard, "source_position": position,
            "payload_sha256": digest, "source_event_id": source_event_id,
            "error": error.code, "message": error.message})
        self._checkpoint(position, source_event_id, checksum)

    def _file_records(self, path: Path, after_line: int, format: str,
                      max_bytes: int, max_records: int) -> Iterator[tuple[int, dict[str, Any] | None, str, ValidationError | None]]:
        """Read bounded physical lines and drain oversized lines without retaining them.

        CSV exports require one record per physical line. A malformed header is a
        source configuration failure requiring operator repair, not a skippable row.
        """
        if format not in {"jsonl", "csv"} or max_bytes < 1 or not 1 <= max_records <= 10000:
            raise ConfigurationError("Invalid connector file limits or format")
        count = 0
        with path.open("rb") as stream:
            header = None
            if format == "csv":
                raw_header = stream.readline(max_bytes + 1)
                try:
                    if len(raw_header) > max_bytes:
                        raise ValueError("Oversized header")
                    header = next(csv.reader([raw_header.decode("utf-8-sig")], strict=True))
                    if not header or len(header) != len(set(header)):
                        raise ValueError("Invalid header")
                except (ValueError, UnicodeError, csv.Error, StopIteration) as error:
                    raise ConfigurationError("CSV header requires repair before extraction can resume") from error
            index = 0
            while not self.runtime.stop.is_set():
                raw = stream.readline(max_bytes + 1)
                if not raw:
                    break
                index += 1
                digest = hashlib.sha256(raw)
                oversized = len(raw) > max_bytes
                if oversized:
                    while not raw.endswith(b"\n"):
                        raw = stream.readline(max_bytes + 1)
                        if not raw:
                            break
                        digest.update(raw)
                if index <= after_line:
                    continue
                row, failure = None, None
                try:
                    if oversized:
                        raise ValueError("Oversized source record")
                    text = raw.decode("utf-8-sig")
                    if format == "jsonl":
                        row = json.loads(text, object_pairs_hook=strict_object, parse_constant=finite_constant)
                    else:
                        values = next(csv.reader([text], strict=True))
                        if len(values) != len(header):
                            raise ValueError("CSV column mismatch")
                        row = dict(zip(header, values))
                    if not isinstance(row, dict):
                        raise ValueError("Source row requires an object")
                except (ValueError, UnicodeError, csv.Error, RecursionError, StopIteration):
                    failure = ValidationError("Source record exceeds limit" if oversized else "Source record is malformed")
                yield index, row, digest.hexdigest(), failure
                count += 1
                if count >= max_records:
                    break

    def _source_values(self, value: Any) -> Any:
        """Convert driver values with an explicitly configured source timezone."""
        if isinstance(value, datetime):
            if value.tzinfo is None:
                name = self.config.get(self.section, "source_timezone")
                if not name:
                    raise ConfigurationError("Naive source timestamps require source_timezone")
                try:
                    if name == "UTC":
                        source_zone = timezone.utc
                    elif re.fullmatch(r"[+-]\d{2}:\d{2}", name):
                        source_zone = datetime.fromisoformat("2000-01-01T00:00:00" + name).tzinfo
                    else:
                        source_zone = ZoneInfo(name)
                    value = value.replace(tzinfo=source_zone)
                except (ZoneInfoNotFoundError, ValueError) as error:
                    raise ConfigurationError("Unknown connector source_timezone") from error
            return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ValidationError("Source decimal must be finite")
            return float(value)
        if isinstance(value, dict):
            return {key: self._source_values(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._source_values(item) for item in value]
        return value

    def extract(self) -> int:
        """Run one bounded extraction; unsupported modes fail visibly."""
        if not self.config.get_bool(self.section, "enabled", False):
            return 0
        checkpoint = self.checkpoint() or {}
        mode = self.config.get_required(self.section, "mode")
        count = 0
        if mode == "file":
            path = self.config.get_path(self.section, "path")
            if path is None or not path.is_file():
                raise ConfigurationError("Connector source file is missing")
            stat = path.stat()
            checksum = hashlib.sha256(f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}".encode()).hexdigest()
            if checkpoint.get("checksum") and checkpoint["checksum"] != checksum:
                raise ConfigurationError("Source file changed; register a new source shard/file before replay")
            for position, row, digest, failure in self._file_records(path, int(checkpoint.get("watermark", "0")),
                    self.config.get(self.section, "format", "jsonl"),
                    self.config.get_int("ingestion", "max_payload_bytes", 65536),
                    self.config.get_int(self.section, "batch_size", 1000)):
                if self.runtime.stop.is_set():
                    break
                if failure:
                    self._invalid(str(position), digest, failure, checksum)
                else:
                    self.handoff(str(position), row, checksum)
                count += 1
        elif mode == "http":
            adapter = HttpConnector(self.config.get_required(self.section, "url"),
                                    self.config.get_list(self.section, "allowed_hosts", []),
                                    self.config.get(self.section, "token", None))
            for row in adapter.records():
                self.handoff(str(row.get("event_time_utc", "")), row)
                count += 1
        elif mode == "mysql":
            from gridsentinel.core.configuration import ConfigManager
            from gridsentinel.infrastructure.mysql.database import Database
            source_config = ConfigManager(self.config.root, self.config.environment,
                overrides={"mysql": dict(self.config.snapshot(redact=False).get(self.section, {}))})
            source_database = Database(source_config)
            try:
                adapter = MySQLIncrementalConnector(source_database, self.config.get_required(self.section, "table"),
                    self.config.get_list(self.section, "columns"), self.config.get(self.section, "watermark_column", "updated_at"),
                    self.config.get(self.section, "id_column", "event_id"), self.config.get_int(self.section, "batch_size", 1000))
                watermark = datetime.fromisoformat(checkpoint["watermark"]) if checkpoint.get("watermark") else datetime(1970, 1, 1)
                for row in adapter.records(watermark, checkpoint.get("source_event_id", "")):
                    if self.runtime.stop.is_set():
                        break
                    at = row[self.config.get(self.section, "watermark_column", "updated_at")]
                    self.handoff(at.isoformat(), self._source_values(row), source_event_id=str(row[self.config.get(self.section, "id_column", "event_id")]))
                    count += 1
            finally:
                source_database.close()
        else:
            raise ConfigurationError("Connector mode requires file/http/mysql; external Kafka sources use the replay command")
        return count

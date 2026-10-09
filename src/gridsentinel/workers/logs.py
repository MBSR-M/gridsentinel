"""Bounded durable JSON file transport for the single-host pilot log collector."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any
import uuid

from gridsentinel.core.logging import JsonFormatter, sanitize


class SpoolLogHandler(logging.Handler):
    """Write one atomic sanitized file per record; drop at a configured quota.

    Critical security records use the transactional audit service instead.
    This shared-volume transport is bounded and durable on a single Docker host.
    """

    def __init__(self, root: Path, service: str, environment: str,
                 instance: str, tenant_id: str, max_files: int = 2048) -> None:
        super().__init__()
        self.directory = root / (service + "-" + instance)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.tenant_id, self.max_files, self.dropped = tenant_id, max_files, 0
        self.setFormatter(JsonFormatter(service, environment, instance))

    def emit(self, record: logging.LogRecord) -> None:
        """Never recursively log failures into the failing spool transport."""
        try:
            count = 0
            with os.scandir(self.directory) as entries:
                for _ in entries:
                    count += 1
                    if count >= self.max_files:
                        self.dropped += 1
                        return
            event = json.loads(self.format(record))
            event["tenant_id"] = self.tenant_id
            event["event_id"] = str(uuid.uuid4())
            raw = json.dumps(event, allow_nan=False).encode()
            if len(raw) > 65536:
                self.dropped += 1
                return
            destination = self.directory / (event["event_id"] + ".json")
            temporary = destination.with_suffix(".partial")
            with temporary.open("xb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        except Exception:
            self.dropped += 1


class LogCollector:
    """Persist bounded files idempotently and remove only after commit."""

    def __init__(self, database: Any, config: Any) -> None:
        self.database, self.config = database, config
        self.root = config.get_path("logging", "spool_directory")
        self.tenant_id = config.get("application", "tenant_id", "demo")

    def collect(self) -> dict[str, int]:
        """Recover a failed transaction by leaving the durable input file intact."""
        stored = invalid = 0
        if not self.root.exists():
            return {"stored": 0, "invalid": 0}
        for directory in self.root.iterdir():
            if not directory.is_dir():
                continue
            with os.scandir(directory) as entries:
                for entry in entries:
                    if stored + invalid >= self.config.get_int("logging", "batch_size", 100):
                        return {"stored": stored, "invalid": invalid}
                    if not entry.name.endswith(".json"):
                        continue
                    path = Path(entry.path)
                    try:
                        if path.stat().st_size > 65536:
                            raise ValueError("Oversized log")
                        event = sanitize(json.loads(path.read_text(encoding="utf-8")))
                        if event.get("tenant_id") != self.tenant_id or event.get("level") not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                            raise ValueError("Invalid log scope")
                        timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
                        uuid.UUID(event["event_id"])
                        with self.database.transaction() as connection:
                            connection.execute("INSERT IGNORE INTO application_logs(event_id,tenant_id,event_time,level,service_name,instance_id,logger_name,message,trace_id,correlation_id,user_id,incident_id,exception_type,sanitized_stack_trace,metadata_json) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                               (event["event_id"], self.tenant_id, timestamp, event["level"], str(event["service"])[:64], str(event["instance_id"])[:128], str(event["logger_name"])[:128], event["message"], event.get("trace_id"), event.get("correlation_id"), event.get("user_id"), event.get("incident_id"), event.get("exception_type"), json.dumps(event.get("sanitized_stack_trace")), json.dumps(event.get("details", {}))))
                        path.unlink()
                        stored += 1
                    except (ValueError, KeyError, TypeError):
                        # Drop malformed debug records; report via independent stderr.
                        path.unlink(missing_ok=True)
                        invalid += 1
                        sys.stderr.write("Log collector rejected an invalid record\n")
        return {"stored": stored, "invalid": invalid}

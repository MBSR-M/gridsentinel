"""JSON log records avoid arbitrary exception text and structured secrets."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import logging
import re
import traceback
from typing import Any

SECRET_KEYS = ("password", "token", "secret", "authorization", "cookie", "credential", "salt")
SECRET_PATTERN = re.compile(r"(?i)(password|token|secret|authorization|cookie|credential)([\s:=]+)([^\s,;}]+)")
BEARER_PATTERN = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]+")


def sanitize(value: Any) -> Any:
    """Redact nested secret fields and common credential formats."""
    if isinstance(value, dict):
        return {str(key): "[REDACTED]" if any(word in str(key).lower() for word in SECRET_KEYS)
                else sanitize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize(item) for item in value]
    if isinstance(value, str):
        value = BEARER_PATTERN.sub("Bearer [REDACTED]", value)
        return SECRET_PATTERN.sub(lambda match: match[1] + match[2] + "[REDACTED]", value)[:16384]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return "[UNSUPPORTED]"


class JsonFormatter(logging.Formatter):
    """Format bounded operational metadata and stack locations without locals."""

    def __init__(self, service: str, environment: str, instance_id: str) -> None:
        super().__init__()
        self.service = service
        self.environment = environment
        self.instance_id = instance_id

    def format(self, record: logging.LogRecord) -> str:
        """Serialize one safely bounded log record."""
        event = {"timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                 "level": record.levelname, "service": self.service,
                 "environment": self.environment, "instance_id": self.instance_id,
                 "logger_name": record.name, "message": sanitize(record.getMessage())}
        for name in ("correlation_id", "trace_id", "tenant_id", "user_id", "incident_id", "job_id"):
            event[name] = sanitize(getattr(record, name, None))
        event["exception_type"] = None
        event["sanitized_stack_trace"] = None
        if record.exc_info:
            event["exception_type"] = record.exc_info[0].__name__
            event["sanitized_stack_trace"] = [
                {"file": frame.filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1],
                 "line": frame.lineno, "function": frame.name}
                for frame in traceback.extract_tb(record.exc_info[2])[-20:]]
        if hasattr(record, "details"):
            event["details"] = sanitize(record.details)
        return json.dumps(event, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def configure_logging(service: str, environment: str = "development",
                      level: str = "INFO", instance_id: str = "local") -> None:
    """Install a structured stderr handler, suitable for Docker log collection."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter(service, environment, instance_id))
    logging.basicConfig(level=level.upper(), handlers=[handler], force=True)

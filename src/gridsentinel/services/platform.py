"""Bounded administrative reads, audit-backed exports and connector metadata."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any
import uuid

from gridsentinel.core.exceptions import NotFoundError, ValidationError
from gridsentinel.core.logging import sanitize
from gridsentinel.core.security import require_permission, validate_identifier
from gridsentinel.api.server import json_default


def page_limit(filters: dict[str, str], default: int = 50) -> int:
    """Reject malformed limits rather than silently accepting oversized queries."""
    try:
        limit = int(filters.get("limit", default))
    except (TypeError, ValueError) as error:
        raise ValidationError("limit must be an integer") from error
    if not 1 <= limit <= 500:
        raise ValidationError("limit must be between 1 and 500")
    return limit


class PlatformService:
    """Separate platform administration from the telemetry domain service."""

    def __init__(self, database: Any, config: Any, audit: Any) -> None:
        self.database, self.config, self.audit = database, config, audit

    def logs(self, principal: dict[str, Any], filters: dict[str, str],
             correlation_id: str, export: bool = False) -> dict[str, Any]:
        """Read a finite date range with keyset pagination and audited access."""
        require_permission(principal, "system.logs.export" if export else "system.logs.read")
        limit = page_limit(filters)
        try:
            cursor = int(filters.get("cursor", "0"))
            end = datetime.fromisoformat(filters["end"].replace("Z", "+00:00")) if filters.get("end") else datetime.now(timezone.utc)
            start = datetime.fromisoformat(filters["start"].replace("Z", "+00:00")) if filters.get("start") else end - timedelta(days=7)
            if start.tzinfo is None or end.tzinfo is None or start >= end or end - start > timedelta(days=31):
                raise ValueError()
        except ValueError as error:
            raise ValidationError("Log range must use UTC/offset timestamps, be ordered and at most 31 days") from error
        clauses = ["tenant_id=%s", "id>%s", "event_time>=%s", "event_time<%s"]
        params: list[Any] = [principal["tenant_id"], cursor,
                             start.astimezone(timezone.utc).replace(tzinfo=None), end.astimezone(timezone.utc).replace(tzinfo=None)]
        for key, column in {"level": "level", "service": "service_name", "instance": "instance_id",
                            "correlation_id": "correlation_id", "trace_id": "trace_id",
                            "user_id": "user_id", "incident_id": "incident_id"}.items():
            if filters.get(key):
                if len(filters[key]) > 128:
                    raise ValidationError("Log filter exceeds limit")
                clauses.append(f"{column}=%s")
                params.append(filters[key])
        if filters.get("q"):
            if len(filters["q"]) > 128:
                raise ValidationError("Log keyword exceeds limit")
            clauses.append("message LIKE %s")
            params.append("%" + filters["q"].replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        params.append(limit + 1)
        with self.database.transaction() as connection:
            rows = connection.fetch_all("SELECT id,event_time,level,service_name,instance_id,logger_name,message,trace_id,correlation_id,user_id,incident_id,exception_type,sanitized_stack_trace,metadata_json FROM application_logs WHERE " + " AND ".join(clauses) + " ORDER BY id LIMIT %s", params)
            self.audit.append(connection, principal["tenant_id"], principal["id"],
                              "LOG_EXPORT" if export else "LOG_READ", "logs", "filtered",
                              correlation_id, metadata={"filters": filters, "count": min(len(rows), limit)}, session_id=principal["session_id"])
        items = rows[:limit]
        for row in items:
            row["metadata"] = json.loads(row.pop("metadata_json"))
        return {"items": items, "next_cursor": str(items[-1]["id"]) if len(rows) > limit else None,
                "retention_days": self.config.get_int("retention", "raw_logs_days", 7)}

    def connectors(self, principal: dict[str, Any]) -> dict[str, Any]:
        """Expose configuration references with sensitive fields redacted."""
        require_permission(principal, "connectors.read")
        with self.database.read() as connection:
            rows = connection.fetch_all("SELECT connector_id,source_type,source_mode,enabled,configuration_json,created_at,updated_at FROM connector_definitions WHERE tenant_id=%s ORDER BY connector_id LIMIT 100", (principal["tenant_id"],))
        for row in rows:
            row["configuration"] = sanitize(json.loads(row.pop("configuration_json")))
        return {"items": rows, "next_cursor": None}

    def save_connector(self, principal: dict[str, Any], data: dict[str, Any],
                       correlation_id: str) -> dict[str, Any]:
        """Persist a disabled-by-default connector; execution uses INI source configuration."""
        require_permission(principal, "connectors.manage")
        identifier = validate_identifier(data.get("connector_id", ""))
        if data.get("source_type") not in {"hes", "mdms", "gis", "network", "meter_events"} or data.get("source_mode") not in {"file", "mysql", "http", "kafka"}:
            raise ValidationError("Unsupported connector type/mode")
        configuration = data.get("configuration", {})
        if not isinstance(configuration, dict) or len(json.dumps(configuration)) > 8192:
            raise ValidationError("Invalid connector configuration")
        if sanitize(configuration) != configuration:
            raise ValidationError("Use INI secret-file references; connector metadata may not contain credentials")
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO connector_definitions(tenant_id,connector_id,source_type,source_mode,enabled,configuration_json,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE source_type=VALUES(source_type),source_mode=VALUES(source_mode),enabled=VALUES(enabled),configuration_json=VALUES(configuration_json),updated_at=UTC_TIMESTAMP(6)", (principal["tenant_id"], identifier, data["source_type"], data["source_mode"], bool(data.get("enabled", False)), json.dumps(configuration)))
            self.audit.append(connection, principal["tenant_id"], principal["id"], "CONNECTOR_UPDATE", "connector", identifier, correlation_id, after=data, session_id=principal["session_id"])
        return {"connector_id": identifier}

    def metrics(self, principal: dict[str, Any]) -> dict[str, Any]:
        """Return recorded worker observations, never invented broker readings."""
        require_permission(principal, "system.health.read")
        with self.database.read() as connection:
            services = connection.fetch_all("SELECT service_name,instance_id,updated_at,status,metrics_json FROM service_heartbeats WHERE tenant_id=%s ORDER BY service_name LIMIT 100", (principal["tenant_id"],))
            jobs = connection.fetch_all("SELECT status,COUNT(*) AS job_count FROM background_jobs WHERE tenant_id=%s GROUP BY status", (principal["tenant_id"],))
            outbox = connection.fetch_one("SELECT COUNT(*) AS pending FROM outbox_events WHERE tenant_id=%s AND published_at IS NULL", (principal["tenant_id"],))
        for service in services:
            service["metrics"] = json.loads(service.pop("metrics_json"))
            service["stale"] = datetime.now(timezone.utc).replace(tzinfo=None) - service["updated_at"] > timedelta(seconds=120)
        return {"services": services, "jobs": jobs, "outbox": outbox, "mysql_ready": self.database.health(), "pool": self.database.pool.stats()}

    def report(self, principal: dict[str, Any], overview: dict[str, Any],
               correlation_id: str) -> dict[str, Any]:
        """Persist and audit a tenant-scoped snapshot export before returning it."""
        require_permission(principal, "reports.export")
        identifier = str(uuid.uuid4())
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO report_exports(export_id,tenant_id,export_type,actor_user_id,created_at,content_json) VALUES(%s,%s,'OVERVIEW',%s,UTC_TIMESTAMP(6),%s)", (identifier, principal["tenant_id"], principal["id"], json.dumps(overview, default=json_default)))
            self.audit.append(connection, principal["tenant_id"], principal["id"], "REPORT_EXPORT", "report", identifier, correlation_id, session_id=principal["session_id"])
        return {"export_id": identifier, "snapshot": overview}

    def exports(self, principal: dict[str, Any]) -> dict[str, Any]:
        """Return export metadata only to a permitted reader."""
        require_permission(principal, "reports.read")
        with self.database.read() as connection:
            items = connection.fetch_all("SELECT export_id,export_type,actor_user_id,created_at FROM report_exports WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 100", (principal["tenant_id"],))
        return {"items": items, "next_cursor": None}

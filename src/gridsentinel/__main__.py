"""Process entry points for the API, telemetry and operational services."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import signal
import threading
import time
from typing import Any, Callable
from urllib.request import urlopen

from gridsentinel.api.server import BoundedHTTPServer
from gridsentinel.core.exceptions import ConfigurationError, GridSentinelError, InfrastructureError
from gridsentinel.workers.runtime import Runtime

LOGGER = logging.getLogger(__name__)


def retention(runtime: Runtime) -> dict[str, int]:
    """Delete finite batches of operational data; never auto-delete security audits."""
    tenant = runtime.config.get("application", "tenant_id", "demo")
    policies = [("application_logs", "event_time", runtime.config.get_int("retention", "raw_logs_days", 7)),
                ("communication_events", "event_time", runtime.config.get_int("retention", "communication_events_days", 30)),
                ("interval_delivery_status", "interval_start", runtime.config.get_int("retention", "communication_events_days", 30)),
                ("meter_health_hourly", "hour_start", runtime.config.get_int("retention", "meter_hourly_health_days", 90)),
                ("meter_health_daily", "day_start", runtime.config.get_int("retention", "daily_health_months", 24) * 31)]
    counts: dict[str, int] = {}
    with runtime.database.transaction() as connection:
        for table, column, days in policies:
            if not 1 <= days <= 3650:
                raise ConfigurationError("Retention must be between 1 and 3650 days")
            cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=days)
            counts[table] = connection.execute(f"DELETE FROM {table} WHERE tenant_id=%s AND {column}<%s LIMIT 1000", (tenant, cutoff)).rowcount
        connection.execute("DELETE FROM service_heartbeats WHERE tenant_id=%s AND updated_at<UTC_TIMESTAMP()-INTERVAL 7 DAY LIMIT 1000", (tenant,))
    return counts


def job_handlers(runtime: Runtime, operations: Any, models: Any) -> dict[str, Callable[[dict[str, Any]], Any]]:
    """Provide implemented idempotent handlers; unknown integrations fail visibly."""
    def principal(job: dict[str, Any]) -> dict[str, Any]:
        with runtime.database.read() as connection:
            user = connection.fetch_one("SELECT id,username,tenant_id FROM users WHERE tenant_id=%s AND id=%s AND status='ACTIVE'", (job["tenant_id"], job["originating_user_id"]))
            if not user:
                raise ConfigurationError("Originating user is unavailable")
            return runtime.auth._principal(connection, user, None)

    def report(job: dict[str, Any]) -> dict[str, Any]:
        from gridsentinel.core.security import require_permission
        from gridsentinel.api.server import json_default
        actor = principal(job)
        require_permission(actor, "reports.export")
        with runtime.database.read() as connection:
            existing = connection.fetch_one("SELECT export_id FROM report_exports WHERE tenant_id=%s AND export_id=%s", (job["tenant_id"], job["job_id"]))
        if existing:
            return existing
        overview = operations.overview(job["tenant_id"])
        with runtime.database.transaction() as connection:
            connection.execute("INSERT IGNORE INTO report_exports(export_id,tenant_id,export_type,actor_user_id,created_at,content_json) VALUES(%s,%s,'OVERVIEW',%s,UTC_TIMESTAMP(6),%s)", (job["job_id"], job["tenant_id"], actor["id"], json.dumps(overview, default=json_default)))
            runtime.audit.append(connection, job["tenant_id"], actor["id"], "REPORT_EXPORT", "report", job["job_id"], job["correlation_id"])
        return {"export_id": job["job_id"]}

    def train(job: dict[str, Any]) -> dict[str, Any]:
        from gridsentinel.core.security import require_permission
        actor = principal(job)
        require_permission(actor, "models.train")
        return models.train(job["tenant_id"], {**job["payload"], "training_run_id": job["job_id"]}, actor, job["correlation_id"])

    def enrich(job: dict[str, Any]) -> dict[str, Any]:
        from gridsentinel.core.security import require_permission
        require_permission(principal(job), "incidents.assign")
        return operations.incident_detail(job["tenant_id"], job["payload"].get("incident_id", ""))

    from gridsentinel.services.alerts import NotificationService
    return {"report.generate": report, "export.generate": report, "model.train": train,
            "incident.enrich": enrich, "notification.send": NotificationService(runtime.config).send}


def periodic(runtime: Runtime, command: str) -> None:
    """Run bounded maintenance batches with visible degraded dependency status."""
    from gridsentinel.infrastructure.kafka import KafkaPublisher, provision_topics
    from gridsentinel.workers.outbox import publish_kafka
    from gridsentinel.workers.logs import LogCollector
    operations, models, jobs = runtime.services()
    publisher = None
    tenant = runtime.config.get("application", "tenant_id", "demo")
    collector = LogCollector(runtime.database, runtime.config)
    next_maintenance = 0.0
    maintenance_interval = max(1, runtime.config.get_int("scheduler", "interval_seconds", 30))
    try:
        while not runtime.stop.is_set():
            metrics: dict[str, Any] = {}
            try:
                maintenance_due = time.monotonic() >= next_maintenance
                if command in {"scheduler", "sweep", "correlate"} and maintenance_due:
                    metrics["health"] = operations.sweep(tenant, runtime.config.get_int("scheduler", "batch_size", 500))
                if command == "scheduler":
                    if maintenance_due:
                        metrics["recovered_jobs"] = jobs.recover_expired(tenant)
                        metrics["retention"] = retention(runtime)
                    if publisher is None:
                        provision_topics(runtime.config)
                        publisher = KafkaPublisher(runtime.config)
                    metrics["outbox_published"] = publish_kafka(runtime.database, publisher, tenant, runtime.config.get_int("scheduler", "batch_size", 500))
                if maintenance_due:
                    next_maintenance = time.monotonic() + maintenance_interval
                if command == "collect-logs":
                    metrics["logs"] = collector.collect()
                if command == "inference":
                    with runtime.database.read() as connection:
                        meters = connection.fetch_all("SELECT meter_id FROM meter_current_health WHERE tenant_id=%s AND current_status='OFFLINE' ORDER BY updated_at DESC LIMIT 100", (tenant,))
                    for meter in meters:
                        models.infer_current(tenant, meter["meter_id"])
                    metrics["scored"] = len(meters)
                runtime.heartbeat(metrics)
            except Exception as error:
                LOGGER.warning("Maintenance batch failed", extra={"details": {"exception_type": type(error).__name__, "command": command}})
                runtime.heartbeat({"error_type": type(error).__name__}, "DEGRADED")
            runtime.stop.wait(1 if command in {"scheduler", "collect-logs"} else maintenance_interval)
    finally:
        if publisher:
            publisher.close()


def main() -> None:
    """Dispatch one process role and handle SIGTERM without losing stored effects."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["api", "check-health", "telemetry", "jobs", "scheduler",
                        "connector", "sweep", "correlate", "inference", "collect-logs", "provision-topics"])
    parser.add_argument("--source", choices=["hes", "mdms", "gis", "network", "meter_events"], default="hes")
    parser.add_argument("--queue", action="append")
    args = parser.parse_args()
    if args.command == "check-health":
        port = int(os.environ.get("GS_API_PORT", "8000"))
        with urlopen(f"http://127.0.0.1:{port}/api/v1/system/readiness", timeout=3) as response:
            raise SystemExit(0 if response.status == 200 else 1)
    runtime = Runtime(args.command)
    server = None
    def terminate(signum: int, frame: Any) -> None:
        runtime.stop.set()
        if server:
            threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    try:
        if args.command == "api":
            from gridsentinel.api.application import ApiApplication
            operations, models, jobs = runtime.services()
            application = ApiApplication(runtime.config, runtime.database, runtime.auth, runtime.audit,
                                         operations, models, runtime.platform, jobs, runtime.cache)
            server = BoundedHTTPServer((runtime.config.get("api", "host", "127.0.0.1"), runtime.config.get_int("api", "port", 8000)),
                                     application, runtime.root / "web", runtime.config.get_int("api", "max_connections", 64),
                                     runtime.config.get_int("api", "max_body_bytes", 1048576), runtime.config.get_int("api", "request_timeout_seconds", 10))
            LOGGER.info("API started")
            runtime.heartbeat({"port": server.server_port})
            server.serve_forever(poll_interval=0.5)
        elif args.command in {"scheduler", "sweep", "correlate", "inference", "collect-logs"}:
            periodic(runtime, args.command)
        else:
            from gridsentinel.infrastructure.kafka import KafkaPublisher, TelemetryWorker, provision_topics
            if args.command == "provision-topics":
                provision_topics(runtime.config)
            elif args.command == "jobs":
                from gridsentinel.infrastructure.rabbitmq import RabbitWorker
                operations, models, jobs = runtime.services()
                RabbitWorker(runtime.config, jobs, job_handlers(runtime, operations, models), runtime.stop, runtime.heartbeat).run(args.queue)
            else:
                provision_topics(runtime.config)
                publisher = KafkaPublisher(runtime.config)
                try:
                    if args.command == "telemetry":
                        operations, _, _ = runtime.services()
                        TelemetryWorker(runtime.config, operations, publisher, runtime.stop, runtime.heartbeat).run()
                    elif args.command == "connector":
                        from gridsentinel.workers.connector import ConnectorRunner
                        runner = ConnectorRunner(runtime, args.source, publisher)
                        while not runtime.stop.is_set():
                            count = runner.extract()
                            runtime.heartbeat({"extracted": count}, "UP" if runtime.config.get_bool("connector:" + args.source, "enabled", False) else "DISABLED")
                            runtime.stop.wait(runtime.config.get_int("scheduler", "interval_seconds", 30))
                finally:
                    publisher.close()
    except GridSentinelError as error:
        LOGGER.error("Service stopped", extra={"details": {"code": error.code}})
        raise SystemExit(1)
    finally:
        if server:
            server.server_close()
        runtime.close()


if __name__ == "__main__":
    main()

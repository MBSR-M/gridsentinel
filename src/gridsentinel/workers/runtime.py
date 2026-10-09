"""Constructor injection and bounded lifetime for separately deployed processes."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
import socket
import threading
from typing import Any
import uuid

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.logging import configure_logging
from gridsentinel.infrastructure.mysql.database import Database
from gridsentinel.infrastructure.redis import RedisCache
from gridsentinel.services.auth import AuthService
from gridsentinel.services.audit import AuditService
from gridsentinel.services.platform import PlatformService
from gridsentinel.workers.logs import SpoolLogHandler


class Runtime:
    """Own resources per process; no module-global mutable clients."""

    def __init__(self, service: str, config: ConfigManager | None = None) -> None:
        root = Path(__file__).resolve().parents[3]
        self.root = root
        self.config = config or ConfigManager(root / "config", os.environ.get("GS_ENVIRONMENT", "development"))
        self.service, self.instance = service, socket.gethostname() + "-" + uuid.uuid4().hex[:8]
        self.stop = threading.Event()
        configure_logging(service, self.config.environment, self.config.get("logging", "level", "INFO"), self.instance)
        self.spool = SpoolLogHandler(self.config.get_path("logging", "spool_directory"), service,
                                    self.config.environment, self.instance,
                                    self.config.get("application", "tenant_id", "demo"),
                                    self.config.get_int("logging", "spool_max_files_per_instance", 2048))
        if service != "collect-logs":
            logging.getLogger().addHandler(self.spool)
        self.database = Database(self.config)
        self.audit = AuditService(self.database)
        self.auth = AuthService(self.database, self.config, self.audit)
        self.platform = PlatformService(self.database, self.config, self.audit)
        self.cache = RedisCache(self.config)

    def services(self) -> tuple[Any, Any, Any]:
        """Build domain services when their command actually requires them."""
        from gridsentinel.processing.operations import OperationsService
        from gridsentinel.ml.service import ModelService
        from gridsentinel.services.jobs.service import JobService
        return OperationsService(self.database, self.config, self.audit), ModelService(self.database, self.config, self.audit), JobService(self.database, self.config, self.audit)

    def heartbeat(self, metrics: dict[str, Any], status: str = "UP") -> None:
        """Store observations in an authoritative, tenant-scoped operations table."""
        metrics["dropped_debug_logs"] = self.spool.dropped
        with self.database.transaction() as connection:
            connection.execute("INSERT INTO service_heartbeats(tenant_id,service_name,instance_id,updated_at,status,metrics_json) VALUES(%s,%s,%s,UTC_TIMESTAMP(6),%s,%s) ON DUPLICATE KEY UPDATE updated_at=UTC_TIMESTAMP(6),status=VALUES(status),metrics_json=VALUES(metrics_json)", (self.config.get("application", "tenant_id", "demo"), self.service, self.instance, status, json.dumps(metrics)))

    def close(self) -> None:
        """Close resources after in-flight handlers have completed."""
        logging.getLogger().removeHandler(self.spool)
        self.spool.close()
        self.cache.close()
        self.database.close()

"""Bind approved reporting views to one deployment tenant and grant SELECT only."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from gridsentinel.core.configuration import ConfigManager
from gridsentinel.core.exceptions import ConfigurationError
from gridsentinel.core.security import validate_identifier

APPROVED_VIEWS = ("v_fleet_health", "v_region_health", "v_delivery_latency",
                  "v_ingestion_operations", "v_queue_health", "v_platform_health",
                  "v_error_trends", "v_prediction_monitoring", "v_incident_sla")


def reporting_sql(tenant_id: str) -> str:
    """Render only a validated tenant literal into the repository SQL template."""
    tenant = validate_identifier(tenant_id, 64)
    if tenant == "__TENANT__":
        raise ConfigurationError("Reporting requires a concrete deployment tenant")
    template = (ROOT / "database/migrations/005_reporting.sql").read_text(encoding="utf-8")
    declared = tuple(re.findall(r"VIEW\s+(v_[a-z_]+)\s+AS", template))
    if declared != APPROVED_VIEWS:
        raise ConfigurationError("Reporting template differs from the approved view catalog")
    return template.replace("__TENANT__", tenant)


def provision(tenant_id: str, compose_files: list[str] | None = None) -> None:
    """Use the mounted MySQL root secret inside Docker without exposing it."""
    statements = reporting_sql(tenant_id)
    statements += "\nREVOKE ALL PRIVILEGES, GRANT OPTION FROM 'gs_reporter'@'%';\n"
    for view in APPROVED_VIEWS:
        statements += f"GRANT SELECT ON gridsentinel.`{view}` TO 'gs_reporter'@'%';\n"
    command = ["docker", "compose"]
    for compose_file in compose_files or []:
        command.extend(["-f", str(Path(compose_file).resolve())])
    command.extend(["exec", "-T", "mysql", "sh", "-c",
                    "MYSQL_PWD=\"$(cat /run/secrets/mysql_root_password)\" exec mysql --batch --skip-column-names -uroot gridsentinel"])
    # SQL is delivered through stdin; no secret is interpolated into process args.
    result = subprocess.run(command, input=statements, text=True, cwd=ROOT,
                            capture_output=True, timeout=60, check=False)
    if result.returncode:
        raise ConfigurationError("Reporting provisioning failed; verify MySQL root access and completed migrations")


def main() -> None:
    """Provision tenant isolation after migrations/bootstrap and before Grafana."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant", help="Defaults to the configured deployment tenant")
    parser.add_argument("--compose-file", action="append", default=[])
    arguments = parser.parse_args()
    config = ConfigManager(ROOT / "config", os.environ.get("GS_ENVIRONMENT", "development"))
    tenant = arguments.tenant or config.get_required("application", "tenant_id")
    provision(tenant, arguments.compose_file)
    print(f"Provisioned {len(APPROVED_VIEWS)} read-only views for tenant {tenant}")


if __name__ == "__main__":
    main()

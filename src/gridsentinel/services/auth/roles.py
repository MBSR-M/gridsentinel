"""Explicit seed grants; role names never bypass permission checks."""

PERMISSIONS = (
    "users.read", "users.create", "users.update", "users.disable",
    "roles.read", "roles.manage", "sessions.read_own", "sessions.revoke_own",
    "sessions.read_all", "sessions.revoke_all", "connectors.read", "connectors.manage",
    "meters.read", "meters.export", "incidents.read", "incidents.acknowledge",
    "incidents.assign", "incidents.resolve", "incidents.feedback", "models.read",
    "models.train", "models.approve", "models.activate", "system.logs.read",
    "system.logs.export", "system.health.read", "audit.read", "audit.export",
    "config.read", "config.update", "jobs.read", "jobs.retry", "jobs.cancel",
    "analytics.read", "reports.read", "reports.export",
    "api_tokens.read_own", "api_tokens.create_own", "api_tokens.revoke_own",
    "api_tokens.read_all", "api_tokens.revoke_all",
)
OWN_SESSION = {"sessions.read_own", "sessions.revoke_own", "api_tokens.read_own", "api_tokens.create_own", "api_tokens.revoke_own"}
READ_OPERATIONS = {"meters.read", "incidents.read", "analytics.read", "models.read", "reports.read"}
ROLE_GRANTS = {
    "SUPER_ADMIN": set(PERMISSIONS),
    "ADMIN": OWN_SESSION | READ_OPERATIONS | {"users.read", "users.create", "users.update", "users.disable", "roles.read", "sessions.read_all", "sessions.revoke_all", "incidents.acknowledge", "incidents.assign", "incidents.resolve", "incidents.feedback", "system.health.read", "jobs.read"},
    "DEVELOPER": OWN_SESSION | {"system.logs.read", "system.logs.export", "system.health.read", "connectors.read", "jobs.read"},
    "DATA_ENGINEER": OWN_SESSION | READ_OPERATIONS | {"connectors.read", "connectors.manage", "jobs.read", "jobs.retry", "system.health.read"},
    "ML_ENGINEER": OWN_SESSION | READ_OPERATIONS | {"models.train"},
    "NOC_OPERATOR": OWN_SESSION | READ_OPERATIONS | {"incidents.acknowledge", "incidents.assign", "incidents.resolve", "incidents.feedback"},
    "FIELD_OPERATOR": OWN_SESSION | {"incidents.read", "incidents.feedback"},
    "AUDITOR": OWN_SESSION | {"audit.read", "audit.export"},
    "VIEWER": OWN_SESSION | READ_OPERATIONS,
}


def seed_roles(connection: object, tenant_id: str) -> None:
    """Provision tenant-specific system roles using idempotent inserts."""
    for code in PERMISSIONS:
        resource, action = code.rsplit(".", 1)
        connection.execute("INSERT IGNORE INTO permissions(tenant_id,permission_code,resource,action,description) VALUES(%s,%s,%s,%s,%s)", (tenant_id, code, resource, action, code))
    for code, grants in ROLE_GRANTS.items():
        connection.execute("INSERT IGNORE INTO roles(tenant_id,role_code,display_name,description,is_system_role) VALUES(%s,%s,%s,%s,TRUE)", (tenant_id, code, code.replace("_", " ").title(), f"System role {code}"))
        role = connection.fetch_one("SELECT id FROM roles WHERE tenant_id=%s AND role_code=%s", (tenant_id, code))
        for permission in sorted(grants):
            connection.execute("INSERT IGNORE INTO role_permissions(tenant_id,role_id,permission_id) SELECT %s,%s,id FROM permissions WHERE tenant_id=%s AND permission_code=%s", (tenant_id, role["id"], tenant_id, permission))

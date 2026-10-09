"""Generate route documentation from the same declarations used by the server."""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from gridsentinel.api.application import ApiApplication

INTRO = """# API reference

All routes use `/api/v1`. JSON success responses have `success`, `data` and `meta.correlation_id` / `request_time_utc`. Errors have `success:false`, `error.code`, `error.message`, `error.retryable` and the correlation ID. Request bodies are JSON objects, limited to 1 MiB; duplicate keys, non-finite values and ambiguous framing are rejected. `X-Correlation-ID` accepts a bounded identifier or the server generates one. Timestamps are UTC ISO 8601.

## Authentication

`POST /auth/login` takes `tenant_id`, `username`, `password`; the deployment tenant is fixed by configuration. Browser login sets HttpOnly access/refresh cookies and a readable CSRF cookie. `GET /auth/me` returns the current principal, CSRF token and authorized Grafana destination. Browser mutations require `X-CSRF-Token` and an accepted origin. `POST /auth/refresh` rotates the refresh family; reuse revokes it. An explicit login `client:"api"` returns opaque tokens to an API client. Bearer API tokens use `Authorization: Bearer TOKEN` and remain limited by live owner permissions.

Public GET routes: `/system/health` (process liveness), `/system/readiness` (database readiness). Application users, Grafana users and broker identities are separate.

## Requests and pagination

Use `limit` and the returned `next_cursor` for supported collections. Meter filters include `q`, `status`, `region_id`; incident filters include `q`, `status`, `severity`, `assigned_to`, `region_id`, `gateway_id`. Field operators are automatically restricted to their assigned incidents. Log queries support bounded UTC windows and correlation/trace/entity filters; reads and exports are audited. Some small catalogs and snapshots return bounded lists without further pagination.

Mutations accept an optional `Idempotency-Key` of 1–128 identifier characters. The same actor/tenant/key cannot be reused with a different method, path or body. Completed replay returns its recorded sanitized result; one-time token secrets are redacted. A pending or UNCERTAIN receipt returns conflict and requires investigation. Authentication rotation uses its own lifecycle and is not covered by generic receipts.

The proxy and application apply request limits. HTTP 429 means retry later; proxy 429 responses include `Retry-After`. Auth failures are 401, permissions 403, missing resources 404, validation 400 and conflicting lifecycle/idempotency actions 409. Dependency failures are visible 503 responses. Do not automatically retry uncertain mutations with a new key.

## Protected route catalog

Paths with `{kind}` expand to meter `health/events/predictions/topology`, analytics `overview/communication/data-quality/sla/regions/gateways`, or incident `evidence/history`. IDs are validated by the server's route patterns. Empty permission entries still require authentication.

| Method | Path | Required permission |
| --- | --- | --- |
"""

if __name__ == "__main__":
    application = ApiApplication(None, None, None, None, None, None, None, None)
    lines = [INTRO.rstrip()]
    for route in application.routes:
        path = re.sub(r"\(\?P<(\w+)>[^)]+\)", lambda match: "{" + match[1] + "}", route.pattern)
        lines.append(f"| {route.method} | `{path}` | `{route.permission or 'authenticated'}` |")
    lines.append("\nReports overview GET creates and audits an export snapshot before returning it. POST model training accepts a bounded `episodes` array, `horizon_hours` and `data_source`, and returns a durable job. See MODEL_CARD.md for the episode schema. Incident feedback takes `confirmed_cause` and supporting `note`; assignment takes `assigned_to`; resolution takes `resolution`. Model approval takes a required `reason`. Read-only configuration snapshots redact secrets.\n")
    (ROOT / "docs/API.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Documented {len(application.routes)} protected route declarations")

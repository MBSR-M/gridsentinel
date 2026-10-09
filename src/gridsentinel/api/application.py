"""Explicit versioned routes, same-origin protection and deny-default RBAC."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
import threading
import time
from typing import Any, Callable

from gridsentinel.core.exceptions import AuthenticationError, AuthorizationError, ConflictError, NotFoundError, RateLimitError, ValidationError
from gridsentinel.core.security import require_permission
from gridsentinel.core.logging import sanitize
from .server import Request, Response, json_default


@dataclass(frozen=True)
class Route:
    """One method/path has an explicit permission and application handler."""

    method: str
    pattern: str
    permission: str
    handler: Callable[[Request, dict[str, Any], re.Match[str]], Any]


def collection(items: list[dict[str, Any]], limit: int = 100,
               key: str = "id") -> dict[str, Any]:
    """Normalize legacy bounded list methods to the common collection envelope."""
    return {"items": items, "next_cursor": str(items[-1][key]) if len(items) == limit else None}


def integer(value: Any, default: int = 0) -> int:
    """Reject non-integer route/query identifiers with a safe error."""
    try:
        result = int(value if value is not None else default)
    except (TypeError, ValueError) as error:
        raise ValidationError("Expected an integer") from error
    if result < 0:
        raise ValidationError("Integer must be non-negative")
    return result


class ApiApplication:
    """Coordinate independent services without embedding their domain logic."""

    def __init__(self, config: Any, database: Any, auth: Any, audit: Any,
                 operations: Any, models: Any, platform: Any, jobs: Any,
                 cache: Any = None) -> None:
        self.config, self.database, self.auth, self.audit = config, database, auth, audit
        self.operations, self.models, self.platform, self.jobs = operations, models, platform, jobs
        self.cache = cache
        self._rates: OrderedDict[str, tuple[float, int]] = OrderedDict()
        self._rate_lock = threading.Lock()
        self.routes = self._routes()

    def _routes(self) -> list[Route]:
        """Declare exact methods and scopes; unmatched routes cannot be authorized."""
        routes = [
            Route("GET", r"/auth/me", "", lambda r, p, m: {"user": p, "csrf_token": r.cookies.get("gs_csrf"), "grafana_url": self.config.get("grafana", "url", "http://localhost:3000")}),
            Route("POST", r"/auth/logout", "sessions.revoke_own", self._logout),
            Route("POST", r"/auth/logout-all", "sessions.revoke_own", self._logout),
            Route("GET", r"/sessions", "sessions.read_own", lambda r, p, m: collection(self.auth.list_sessions(p), 500)),
            Route("DELETE", r"/sessions/(?P<id>[A-Za-z0-9-]{1,64})", "sessions.revoke_own", lambda r, p, m: self.auth.revoke_session(p, m["id"], r.correlation_id) or {"revoked": True}),
            Route("GET", r"/users", "users.read", lambda r, p, m: collection(self.auth.list_users(p, self._limit(r), integer(r.query.get("cursor"))), self._limit(r))),
            Route("POST", r"/users", "users.create", lambda r, p, m: self.auth.create_user(p, r.body, r.correlation_id)),
            Route("PATCH", r"/users/(?P<id>\d+)", "users.update", lambda r, p, m: self.auth.update_user(p, integer(m["id"]), r.body, r.correlation_id)),
            Route("GET", r"/roles", "roles.read", lambda r, p, m: collection(self.auth.roles(p))),
            Route("POST", r"/roles", "roles.manage", lambda r, p, m: self.auth.create_role(p, r.body, r.correlation_id)),
            Route("GET", r"/permissions", "roles.read", lambda r, p, m: collection(self.auth.permissions(p))),
            Route("GET", r"/api-tokens", "api_tokens.read_own", lambda r, p, m: collection(self.auth.list_api_tokens(p))),
            Route("POST", r"/api-tokens", "api_tokens.create_own", lambda r, p, m: self.auth.create_api_token(p, r.body, r.correlation_id)),
            Route("DELETE", r"/api-tokens/(?P<id>\d+)", "api_tokens.revoke_own", lambda r, p, m: self.auth.revoke_api_token(p, integer(m["id"]), r.correlation_id) or {"revoked": True}),
            Route("GET", r"/meters", "meters.read", lambda r, p, m: self.operations.list_meters(p["tenant_id"], r.query)),
            Route("GET", r"/meters/(?P<id>[A-Za-z0-9_.:-]{1,128})", "meters.read", lambda r, p, m: self.operations.meter_detail(p["tenant_id"], m["id"])),
            Route("GET", r"/meters/(?P<id>[A-Za-z0-9_.:-]{1,128})/(?P<kind>health|events|predictions|topology)", "meters.read", self._meter_component),
            Route("GET", r"/analytics/(?P<kind>overview|communication|data-quality|sla|regions|gateways)", "analytics.read", self._analytics),
            Route("GET", r"/incidents", "incidents.read", self._incidents),
            Route("GET", r"/incidents/(?P<id>[A-Za-z0-9_.:-]{1,128})", "incidents.read", self._incident),
            Route("GET", r"/incidents/(?P<id>[A-Za-z0-9_.:-]{1,128})/(?P<kind>evidence|history)", "incidents.read", self._incident),
            Route("GET", r"/ml/models", "models.read", lambda r, p, m: self.models.list_models(p["tenant_id"])),
            Route("GET", r"/ml/training-runs", "models.read", lambda r, p, m: self.models.training_runs(p["tenant_id"])),
            Route("POST", r"/ml/training-runs", "models.train", self._training_job),
            Route("GET", r"/ml/predictions", "models.read", lambda r, p, m: self.models.predictions(p["tenant_id"], r.query.get("meter_id", ""))),
            Route("GET", r"/ml/drift", "models.read", lambda r, p, m: self.models.drift(p["tenant_id"])),
            Route("GET", r"/developer/logs", "system.logs.read", lambda r, p, m: self.platform.logs(p, r.query, r.correlation_id)),
            Route("GET", r"/developer/logs/export", "system.logs.export", lambda r, p, m: self.platform.logs(p, r.query, r.correlation_id, export=True)),
            Route("GET", r"/developer/jobs", "jobs.read", lambda r, p, m: self.jobs.list_jobs(p, r.query)),
            Route("GET", r"/connectors", "connectors.read", lambda r, p, m: self.platform.connectors(p)),
            Route("POST", r"/connectors", "connectors.manage", lambda r, p, m: self.platform.save_connector(p, r.body, r.correlation_id)),
            Route("GET", r"/audit/events", "audit.read", lambda r, p, m: collection(self.audit.list_events(p, self._limit(r), integer(r.query.get("cursor"))), self._limit(r))),
            Route("GET", r"/audit/exports", "audit.read", self._audit_exports),
            Route("GET", r"/audit/verify", "audit.read", lambda r, p, m: self.audit.verify(p["tenant_id"])),
            Route("GET", r"/system/config", "config.read", lambda r, p, m: {"configuration": {s: dict(v) for s, v in self.config.snapshot().items()}, "retention": dict(self.config.snapshot().get("retention", {}))}),
            Route("GET", r"/system/metrics", "system.health.read", lambda r, p, m: self.platform.metrics(p)),
            Route("GET", r"/reports", "reports.read", lambda r, p, m: self.platform.exports(p)),
            Route("GET", r"/reports/overview", "reports.export", lambda r, p, m: self.platform.report(p, self.operations.overview(p["tenant_id"]), r.correlation_id)),
        ]
        for action in ("acknowledge", "assign", "resolve", "feedback"):
            routes.append(Route("POST", rf"/incidents/(?P<id>[A-Za-z0-9_.:-]{{1,128}})/{action}",
                                f"incidents.{action}", self._incident_action))
        for action in ("approve", "activate", "rollback"):
            routes.append(Route("POST", rf"/ml/models/(?P<id>[A-Za-z0-9_.:-]{{1,128}})/{action}",
                                "models.approve" if action == "approve" else "models.activate", self._model_action))
        for action in ("retry", "cancel"):
            routes.append(Route("POST", rf"/developer/jobs/(?P<id>[A-Za-z0-9-]{{1,64}})/{action}", f"jobs.{action}",
                                lambda r, p, m: self.jobs.job_action(p, m["id"], r.path.rsplit("/", 1)[-1], r.correlation_id)))
        return routes

    @staticmethod
    def _limit(request: Request) -> int:
        from gridsentinel.services.platform import page_limit
        return page_limit(request.query, 100)

    def dispatch(self, request: Request) -> Response:
        """Resolve public exceptions first, then enforce tenant, RBAC and CSRF."""
        if not request.path.startswith("/api/v1/"):
            raise NotFoundError("API route not found")
        path = request.path[len("/api/v1"):]
        if request.method == "GET" and path == "/system/health":
            return Response({"status": "UP", "service": "GridSentinel"})
        if request.method == "GET" and path == "/system/readiness":
            ready = self.database.health()
            return Response({"status": "READY" if ready else "NOT_READY"}, 200 if ready else 503)
        self._origin(request)
        self._rate(request)
        if request.method == "POST" and path == "/auth/login":
            tenant = request.body.get("tenant_id", self.config.get("application", "tenant_id", "demo"))
            if tenant != self.config.get("application", "tenant_id", "demo"):
                raise AuthenticationError("Invalid username or password")
            result = self.auth.login(request.body.get("username", ""), request.body.get("password", ""),
                                     tenant, self._client_ip(request), request.headers.get("user-agent", ""), request.correlation_id)
            return self._credential_response(result, request.body.get("client") == "api")
        if request.method == "POST" and path == "/auth/refresh":
            result = self.auth.refresh(request.cookies.get("gs_refresh", request.body.get("refresh_token", "")),
                                       request.headers.get("x-csrf-token", request.body.get("csrf_token", "")), request.correlation_id)
            return self._credential_response(result, request.body.get("client") == "api")
        principal, bearer = self._authenticate(request)
        for value in (request.query.get("tenant_id"), request.body.get("tenant_id")):
            if value is not None and value != principal["tenant_id"]:
                raise AuthorizationError("Tenant scope mismatch")
        for route in self.routes:
            match = re.fullmatch(route.pattern, path)
            if route.method == request.method and match:
                if route.permission:
                    require_permission(principal, route.permission)
                if request.method in {"POST", "PATCH", "DELETE"} and not bearer:
                    self.auth.validate_csrf(principal, request.headers.get("x-csrf-token", ""))
                # Durable receipts protect supplied mutation keys. A crash after
                # domain commit stays uncertain instead of replaying the effect.
                key = request.headers.get("idempotency-key")
                if key and request.method in {"POST", "PATCH", "DELETE"} and not path.startswith("/auth/"):
                    return self._idempotent(request, principal, match, route, key)
                result = route.handler(request, principal, match)
                return result if isinstance(result, Response) else Response(result)
        raise NotFoundError("API route not found")

    def _authenticate(self, request: Request) -> tuple[dict[str, Any], bool]:
        tenant = self.config.get("application", "tenant_id", "demo")
        authorization = request.headers.get("authorization")
        if authorization is not None:
            if not authorization.startswith("Bearer ") or len(authorization.split()) != 2:
                raise AuthenticationError("Invalid request credentials")
            token = authorization[7:]
            if token.startswith("gs_api_"):
                return self.auth.authenticate_api_token(token, tenant), True
            try:
                return self.auth.authenticate(token, tenant), True
            except AuthenticationError:
                return self.auth.authenticate_api_token(token, tenant), True
        token = request.cookies.get("gs_access", "")
        return self.auth.authenticate(token, tenant), False

    def _origin(self, request: Request) -> None:
        origin = request.headers.get("origin")
        allowed = self.config.get_list("api", "allowed_origins", ["http://localhost:8080", "http://127.0.0.1:8080"])
        if origin is not None and origin not in allowed:
            raise AuthorizationError("Origin is not allowed")
        if request.headers.get("sec-fetch-site") == "cross-site":
            raise AuthorizationError("Cross-site requests are not allowed")
        if self.config.get_bool("security", "cookie_secure", True):
            if not self.config.get_bool("api", "trusted_proxy", False) or request.headers.get("x-forwarded-proto") != "https":
                raise AuthorizationError("HTTPS through the configured trusted proxy is required")

    def _client_ip(self, request: Request) -> str:
        if self.config.get_bool("api", "trusted_proxy", False):
            return request.headers.get("x-real-ip", request.ip)
        return request.ip

    def _rate(self, request: Request) -> None:
        identity = self._client_ip(request)
        tenant = self.config.get("application", "tenant_id", "demo")
        result = self.cache.rate_limit(tenant, identity, 300, 60) if self.cache else None
        if result is False:
            raise RateLimitError("Request budget exceeded")
        if result:
            return
        with self._rate_lock:
            now = time.monotonic()
            start, count = self._rates.pop(identity, (now, 0))
            if now - start >= 60:
                start, count = now, 0
            self._rates[identity] = (start, count + 1)
            while len(self._rates) > 4096:
                self._rates.popitem(last=False)
            if count >= 300:
                raise RateLimitError("Request budget exceeded")

    def _credential_response(self, result: dict[str, Any], api_client: bool) -> Response:
        if api_client:
            return Response(result)
        secure = "; Secure" if self.config.get_bool("security", "cookie_secure", True) else ""
        max_age = max(0, int((result["absolute_expires_at"] - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()))
        headers = [
            ("Set-Cookie", f"gs_access={result['access_token']}; Path=/api/v1; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"),
            ("Set-Cookie", f"gs_refresh={result['refresh_token']}; Path=/api/v1/auth; HttpOnly; SameSite=Strict; Max-Age={max_age}{secure}"),
            ("Set-Cookie", f"gs_csrf={result['csrf_token']}; Path=/; SameSite=Strict; Max-Age={max_age}{secure}"),
        ]
        return Response({key: result[key] for key in ("user", "session_id", "csrf_token", "access_expires_at", "absolute_expires_at")}, headers=headers)

    def _logout(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Response:
        self.auth.logout(principal, request.path.endswith("logout-all"), request.correlation_id)
        secure = "; Secure" if self.config.get_bool("security", "cookie_secure", True) else ""
        return Response({"logged_out": True}, headers=[("Set-Cookie", f"{name}=; Path={path}; Max-Age=0; SameSite=Strict{secure}" + ("; HttpOnly" if name != "gs_csrf" else ""))
                         for name, path in (("gs_access", "/api/v1"), ("gs_refresh", "/api/v1/auth"), ("gs_csrf", "/"))])

    def _analytics(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        return self.operations.overview(principal["tenant_id"]) if match["kind"] == "overview" else self.operations.analytics(principal["tenant_id"], match["kind"], request.query)

    def _meter_component(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        if match["kind"] == "predictions":
            return self.models.predictions(principal["tenant_id"], match["id"])
        detail = self.operations.meter_detail(principal["tenant_id"], match["id"])
        value = detail.get(match["kind"], [] if match["kind"] != "health" else {})
        return collection(value) if isinstance(value, list) else value

    def _incidents(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        filters = dict(request.query)
        if principal.get("incident_scope") == "assigned":
            filters["assigned_to"] = str(principal["id"])
        return self.operations.list_incidents(principal["tenant_id"], filters)

    def _incident(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        detail = self.operations.incident_detail(principal["tenant_id"], match["id"])
        if principal.get("incident_scope") == "assigned" and detail.get("assigned_to") != principal["id"]:
            raise NotFoundError("Incident not found")
        return collection(detail.get(match["kind"], [])) if "kind" in match.groupdict() else detail

    def _incident_action(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        self._incident(request, principal, match)
        return self.operations.incident_action(principal, match["id"], request.path.rsplit("/", 1)[-1], request.body, request.correlation_id)

    def _model_action(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        return self.models.model_action(principal, match["id"], request.path.rsplit("/", 1)[-1], request.body, request.correlation_id)

    def _training_job(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        return self.jobs.create(principal, "model.train", request.body, request.correlation_id,
                                request.headers.get("idempotency-key", request.correlation_id))

    def _audit_exports(self, request: Request, principal: dict[str, Any], match: re.Match[str]) -> Any:
        items = self.audit.list_events(principal, 500, integer(request.query.get("cursor")))
        return collection([row for row in items if "EXPORT" in row["action"]], 500)

    def _idempotent(self, request: Request, principal: dict[str, Any], match: re.Match[str], route: Route, key: str) -> Response:
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", key):
            raise ValidationError("Invalid Idempotency-Key")
        digest = hashlib.sha256(json.dumps({"method": request.method, "path": request.path, "body": request.body}, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).digest()
        parameters = (principal["tenant_id"], principal["id"], key)
        with self.database.transaction() as connection:
            existing = connection.fetch_one("SELECT request_digest,status,response_json FROM api_requests WHERE tenant_id=%s AND actor_user_id=%s AND idempotency_key=%s FOR UPDATE", parameters)
            if existing:
                if existing["request_digest"] != digest:
                    raise ConflictError("Idempotency key was used for a different request")
                if existing["status"] == "COMPLETE":
                    return Response(json.loads(existing["response_json"]))
                raise ConflictError("Prior request outcome is pending or uncertain; inspect the resource before a new action")
            connection.execute("INSERT INTO api_requests(tenant_id,actor_user_id,idempotency_key,request_digest,status,created_at) VALUES(%s,%s,%s,%s,'PROCESSING',UTC_TIMESTAMP(6))", (*parameters, digest))
        try:
            result = route.handler(request, principal, match)
            response = result if isinstance(result, Response) else Response(result)
            with self.database.transaction() as connection:
                connection.execute("UPDATE api_requests SET status='COMPLETE',response_json=%s WHERE tenant_id=%s AND actor_user_id=%s AND idempotency_key=%s", (json.dumps(sanitize(response.data), default=json_default), *parameters))
            return response
        except BaseException:
            with self.database.transaction() as connection:
                connection.execute("UPDATE api_requests SET status='UNCERTAIN' WHERE tenant_id=%s AND actor_user_id=%s AND idempotency_key=%s", parameters)
            raise

"""Conservative bounded HTTP transport; deploy behind the supplied reverse proxy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
from pathlib import Path
import re
import socket
from socketserver import ThreadingMixIn
import threading
from typing import Any, Protocol
from urllib.parse import parse_qs, unquote, urlsplit
import uuid

from gridsentinel.core.exceptions import GridSentinelError, ValidationError

LOGGER = logging.getLogger(__name__)


def json_default(value: Any) -> Any:
    """Serialize SQL dates and decimals without exposing byte credentials."""
    if isinstance(value, datetime):
        utc_value = (value.astimezone(timezone.utc) if value.tzinfo is not None
                     else value.replace(tzinfo=timezone.utc))
        return utc_value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    raise TypeError("Unsupported public response value")


def strict_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous duplicate object keys at every JSON nesting level."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON object key")
        result[key] = value
    return result


@dataclass(frozen=True)
class Request:
    """Validated request context with bounded body and query strings."""

    method: str
    path: str
    query: dict[str, str]
    body: dict[str, Any]
    headers: dict[str, str]
    cookies: dict[str, str]
    correlation_id: str
    ip: str


@dataclass
class Response:
    """Application response including separate Set-Cookie fields."""

    data: Any
    status: int = 200
    headers: list[tuple[str, str]] = field(default_factory=list)


class Application(Protocol):
    """Transport-independent application boundary."""

    def dispatch(self, request: Request) -> Response:
        """Authorize and perform one application request."""


class BoundedHTTPServer(ThreadingMixIn, HTTPServer):
    """Reject excess connections before spawning threads; time out slow peers."""

    daemon_threads = False
    block_on_close = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int], application: Application,
                 web_root: Path, max_connections: int = 64,
                 max_body_bytes: int = 1048576, timeout_seconds: int = 10) -> None:
        if max_connections < 1 or max_body_bytes < 1 or timeout_seconds <= 0:
            raise ValueError("HTTP resource limits must be positive")
        self.application = application
        self.web_root = web_root.resolve()
        self.max_body_bytes = max_body_bytes
        self.peer_timeout = timeout_seconds
        self._capacity = threading.BoundedSemaphore(max_connections)
        super().__init__(address, Handler)

    def get_request(self) -> tuple[socket.socket, Any]:
        """Set the deadline before reading any bytes from an accepted socket."""
        connection, address = super().get_request()
        connection.settimeout(self.peer_timeout)
        return connection, address

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        """Apply backpressure without accumulating a work queue."""
        if not self._capacity.acquire(blocking=False):
            try:
                request.sendall(b"HTTP/1.0 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._capacity.release()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        """Always release capacity, including malformed request failures."""
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._capacity.release()


class Handler(BaseHTTPRequestHandler):
    """HTTP/1.0 JSON API with strict framing, same-origin transport and no CORS."""

    server: BoundedHTTPServer
    protocol_version = "HTTP/1.0"
    server_version = "GridSentinel"
    sys_version = ""

    def log_message(self, format: str, *args: Any) -> None:
        """Avoid the default handler logging arbitrary URLs or query secrets."""

    def do_GET(self) -> None:
        """Handle a bounded GET request."""
        self._handle()

    def do_POST(self) -> None:
        """Handle a JSON mutation."""
        self._handle()

    def do_PATCH(self) -> None:
        """Handle a JSON partial update."""
        self._handle()

    def do_DELETE(self) -> None:
        """Handle an authorized deletion or revocation."""
        self._handle()

    def do_OPTIONS(self) -> None:
        """Reject cross-origin preflights; CORS is not enabled."""
        self.send_error(405)

    def _request(self, correlation_id: str) -> Request:
        if len(self.path) > 4096:
            raise ValidationError("Request URI exceeds limit")
        if sum(len(k) + len(v) for k, v in self.headers.items()) > 16384:
            raise ValidationError("Request headers exceed limit")
        for key in ("Content-Length", "Host", "Authorization", "Cookie", "Origin",
                    "X-CSRF-Token", "Idempotency-Key", "X-Correlation-ID",
                    "X-Forwarded-Proto", "X-Forwarded-For", "X-Real-IP"):
            if len(self.headers.get_all(key, [])) > 1:
                raise ValidationError("Duplicate security-sensitive header")
        if self.headers.get("Transfer-Encoding") or self.headers.get("Expect"):
            raise ValidationError("Unsupported request framing")
        raw_length = self.headers.get("Content-Length", "0")
        if not raw_length.isascii() or not raw_length.isdigit():
            raise ValidationError("Invalid content length")
        length = int(raw_length)
        if length > self.server.max_body_bytes:
            raise ValidationError("Request body exceeds limit")
        body: dict[str, Any] = {}
        if self.command in {"POST", "PATCH"} and length == 0:
            body = {}
        elif length:
            if self.command == "GET":
                raise ValidationError("GET request bodies are not accepted")
            if self.headers.get_content_type() != "application/json":
                raise ValidationError("Content-Type must be application/json")
            raw = self.rfile.read(length)
            if len(raw) != length:
                raise ValidationError("Incomplete request body")
            try:
                body = json.loads(raw.decode("utf-8"),
                                  object_pairs_hook=strict_json_object,
                                  parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
            except (ValueError, UnicodeDecodeError, RecursionError) as error:
                raise ValidationError("Malformed JSON body") from error
            if not isinstance(body, dict):
                raise ValidationError("JSON body must be an object")
        try:
            parsed = urlsplit(self.path)
        except ValueError as error:
            raise ValidationError("Malformed request URI") from error
        if parsed.scheme or parsed.netloc:
            raise ValidationError("Absolute request URLs are not supported")
        if parsed.fragment:
            raise ValidationError("Request URI fragments are not supported")
        try:
            query = parse_qs(parsed.query, keep_blank_values=True,
                             max_num_fields=30, strict_parsing=False,
                             encoding="utf-8", errors="strict")
            decoded_path = unquote(parsed.path, encoding="utf-8", errors="strict")
        except (ValueError, UnicodeDecodeError) as error:
            raise ValidationError("Malformed or excessive query parameters") from error
        if any(ord(character) < 32 or ord(character) == 127 for character in decoded_path):
            raise ValidationError("Control characters in request URI")
        if any(len(values) != 1 for values in query.values()):
            raise ValidationError("Duplicate query parameter")
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie", ""))
        except Exception as error:
            raise ValidationError("Malformed cookie header") from error
        return Request(self.command, decoded_path,
                       {k: v[0] for k, v in query.items()}, body,
                       {k.lower(): v for k, v in self.headers.items()},
                       {k: morsel.value for k, morsel in cookie.items()},
                       correlation_id, self.client_address[0])

    def _handle(self) -> None:
        started = datetime.now(timezone.utc)
        candidate = self.headers.get("X-Correlation-ID", "")
        correlation = candidate if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", candidate) else str(uuid.uuid4())
        try:
            request = self._request(correlation)
            if not request.path.startswith("/api/"):
                if request.method != "GET":
                    self.send_error(405)
                    return
                self._static(request.path)
                return
            response = self.server.application.dispatch(request)
            payload = {"success": True, "data": response.data,
                       "meta": {"correlation_id": correlation,
                                "request_time_utc": started.isoformat().replace("+00:00", "Z")}}
            self._send(response.status, payload, response.headers)
            LOGGER.info("API request completed", extra={"correlation_id": correlation,
                        "details": {"method": request.method, "status": response.status,
                                    "duration_ms": (datetime.now(timezone.utc)-started).total_seconds()*1000}})
        except GridSentinelError as error:
            self._send(error.status, {"success": False,
                       "error": {"code": error.code, "message": error.message,
                                 "retryable": error.retryable},
                       "meta": {"correlation_id": correlation}})
        except (TimeoutError, socket.timeout):
            self.close_connection = True
        except Exception as error:
            LOGGER.error("API request failed", extra={"correlation_id": correlation,
                         "details": {"exception_type": type(error).__name__}})
            self._send(500, {"success": False,
                       "error": {"code": "INTERNAL_ERROR", "message": "Request could not be completed",
                                 "retryable": False}, "meta": {"correlation_id": correlation}})

    def _send(self, status: int, payload: dict[str, Any],
              headers: list[tuple[str, str]] | None = None) -> None:
        raw = json.dumps(payload, default=json_default, allow_nan=False,
                         ensure_ascii=True, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        for name, value in headers or []:
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(raw)
        self.close_connection = True

    def _static(self, path: str) -> None:
        root = self.server.web_root
        target = (root / path.lstrip("/")).resolve()
        if path == "/":
            target = (root / "index.html").resolve()
        if not target.is_relative_to(root) or not target.is_file():
            self.send_error(404)
            return
        if target.stat().st_size > 2 * 1024 * 1024:
            self.send_error(413)
            return
        content_types = {".html": "text/html", ".js": "text/javascript",
                         ".css": "text/css", ".svg": "image/svg+xml",
                         ".png": "image/png", ".ico": "image/x-icon"}
        raw = target.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_types.get(target.suffix, "application/octet-stream"))
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; object-src 'none'; frame-ancestors 'none'; base-uri 'self'")
        self.end_headers()
        self.wfile.write(raw)

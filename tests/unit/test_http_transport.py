"""Socket-level regression tests for the bounded standard-library transport."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from gridsentinel.api.server import BoundedHTTPServer, Request, Response, json_default


class RecordingApplication:
    """Expose validated request content without relying on database or identity."""

    def __init__(self) -> None:
        self.requests: list[Request] = []
        self.fail = False

    def dispatch(self, request: Request) -> Response:
        """Record dispatch and return only bounded test values."""
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("private database failure")
        return Response({"body": request.body, "query": request.query,
                         "path": request.path})


class HTTPTransportTests(unittest.TestCase):
    """Exercise actual HTTP framing and file boundaries over loopback sockets."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.web = self.root / "web"
        self.web.mkdir()
        (self.web / "index.html").write_text("public dashboard", encoding="utf-8")
        (self.root / "private.txt").write_text("private sentinel", encoding="utf-8")
        self.application = RecordingApplication()
        self.server = BoundedHTTPServer(("127.0.0.1", 0), self.application,
                                       self.web, max_connections=1,
                                       max_body_bytes=128, timeout_seconds=1)
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={"poll_interval": 0.01})
        self.thread.start()
        self.addCleanup(self.cleanup_server)

    def cleanup_server(self) -> None:
        """Wait for in-flight bounded request threads before removing files."""
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
        self.directory.cleanup()

    def raw_request(self, request: bytes, *, end_body: bool = False) -> tuple[int, dict[str, str], bytes]:
        """Exchange one raw HTTP request so duplicate headers remain visible."""
        with socket.create_connection(self.server.server_address, timeout=3) as peer:
            peer.sendall(request)
            if end_body:
                peer.shutdown(socket.SHUT_WR)
            chunks = []
            while True:
                try:
                    part = peer.recv(65536)
                except (ConnectionAbortedError, ConnectionResetError):
                    # Windows may reset an overload socket after the complete
                    # zero-body 503 when the server closes unread request bytes.
                    received = b"".join(chunks)
                    if not received.startswith(b"HTTP/1.0 503") or b"\r\n\r\n" not in received:
                        raise
                    break
                if not part:
                    break
                chunks.append(part)
        raw = b"".join(chunks)
        head, body = raw.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        status = int(lines[0].split()[1])
        headers = dict(line.split(": ", 1) for line in lines[1:])
        if status != 503:
            self.assertTrue(self.server._capacity.acquire(timeout=3),
                            "Request thread did not release capacity")
            self.server._capacity.release()
        return status, headers, body

    def post(self, body: bytes, extra_headers: bytes = b"") -> tuple[int, dict[str, str], bytes]:
        """Send a JSON object with explicit byte length."""
        request = (b"POST /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                   b"Content-Type: application/json\r\nContent-Length: "
                   + str(len(body)).encode() + b"\r\n" + extra_headers + b"\r\n" + body)
        return self.raw_request(request)

    def assert_rejected(self, result: tuple[int, dict[str, str], bytes]) -> None:
        """Invalid framing must fail before application dispatch."""
        status, _, body = result
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(body)["error"]["code"], "VALIDATION_ERROR")
        self.assertEqual(self.application.requests, [])

    def test_valid_utf8_body_and_response_security_headers(self) -> None:
        status, headers, body = self.post('{"name":"meter \u03b1"}'.encode("utf-8"))
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["data"]["body"]["name"], "meter \u03b1")
        self.assertEqual(int(headers["Content-Length"]), len(body))
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["X-Frame-Options"], "DENY")

    def test_invalid_json_and_unsupported_encoding_are_rejected(self) -> None:
        for body in (b'{"field":}', b'{"field":NaN}', b'{"field":Infinity}',
                     b'{"field":"\xff"}', b'[]', ' {"field":1}'.encode("utf-16")):
            with self.subTest(body=body):
                self.assert_rejected(self.post(body))

    def test_duplicate_json_keys_are_rejected_at_every_level(self) -> None:
        for body in (b'{"role":"viewer","role":"admin"}',
                     b'{"nested":{"role":"viewer","role":"admin"}}'):
            with self.subTest(body=body):
                self.assert_rejected(self.post(body))

    def test_oversized_body_is_rejected_before_reading(self) -> None:
        request = (b"POST /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                   b"Content-Type: application/json\r\nContent-Length: 129\r\n\r\n")
        self.assert_rejected(self.raw_request(request))

    def test_invalid_content_lengths_are_rejected(self) -> None:
        for value in (b"-1", b"+2", b"two", b"2, 2"):
            with self.subTest(value=value):
                request = (b"POST /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                           b"Content-Length: " + value + b"\r\n\r\n")
                self.assert_rejected(self.raw_request(request))

    def test_duplicate_security_headers_are_rejected(self) -> None:
        for name in (b"Host", b"Content-Length", b"Authorization", b"Cookie", b"Origin",
                     b"X-CSRF-Token", b"Idempotency-Key", b"X-Correlation-ID",
                     b"X-Forwarded-Proto", b"X-Forwarded-For", b"X-Real-IP"):
            with self.subTest(name=name):
                value = b"0" if name == b"Content-Length" else b"value"
                request = (b"GET /api/v1/check HTTP/1.1\r\n" + name + b": " + value
                           + b"\r\n" + name.lower() + b": " + value + b"\r\n\r\n")
                self.assert_rejected(self.raw_request(request))

    def test_transfer_encoding_and_expect_are_rejected(self) -> None:
        for header in (b"Transfer-Encoding: chunked", b"Expect: 100-continue"):
            with self.subTest(header=header):
                request = (b"POST /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                           + header + b"\r\n\r\n")
                self.assert_rejected(self.raw_request(request))

    def test_get_body_and_non_json_mutation_are_rejected(self) -> None:
        for method, content_type in ((b"GET", b"application/json"),
                                     (b"POST", b"text/plain")):
            with self.subTest(method=method):
                request = (method + b" /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                           b"Content-Length: 2\r\nContent-Type: " + content_type + b"\r\n\r\n{}")
                self.assert_rejected(self.raw_request(request))

    def test_incomplete_body_is_rejected(self) -> None:
        request = (b"POST /api/v1/check HTTP/1.1\r\nHost: localhost\r\n"
                   b"Content-Type: application/json\r\nContent-Length: 8\r\n\r\n{}")
        self.assert_rejected(self.raw_request(request, end_body=True))

    def test_excessive_query_fields_and_duplicates_are_validation_errors(self) -> None:
        for query in ("&".join(f"key{index}=1" for index in range(31)),
                      "role=viewer&role=admin", "role=viewer&role=", "role=&role="):
            with self.subTest(query=query):
                request = (f"GET /api/v1/check?{query} HTTP/1.1\r\nHost: localhost\r\n\r\n").encode()
                self.assert_rejected(self.raw_request(request))

    def test_request_uri_limits_and_absolute_urls(self) -> None:
        for uri in ("/api/v1/check?value=" + "x" * 4096, "http://localhost/api/v1/check",
                    "http://[broken/api", "/api/v1/check#fragment", "/%00private.txt",
                    "/api/v1/check?name=%FF"):
            with self.subTest(uri=uri[:50]):
                self.assert_rejected(self.raw_request(
                    f"GET {uri} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode()))

    def test_header_aggregate_limit_is_enforced(self) -> None:
        request = (b"GET /api/v1/check HTTP/1.1\r\nHost: localhost\r\nX-Large: "
                   + b"x" * 17000 + b"\r\n\r\n")
        self.assert_rejected(self.raw_request(request))

    def test_static_traversal_cannot_read_files_outside_web_root(self) -> None:
        for uri in ("/../private.txt", "/%2e%2e/private.txt", "/%2e%2e%5cprivate.txt"):
            with self.subTest(uri=uri):
                status, _, body = self.raw_request(
                    f"GET {uri} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
                self.assertEqual(status, 404)
                self.assertNotIn(b"private sentinel", body)

    def test_index_symlink_cannot_escape_web_root(self) -> None:
        index = self.web / "index.html"
        index.unlink()
        try:
            index.symlink_to(self.root / "private.txt")
        except OSError:
            self.skipTest("Creating symlinks requires OS permission")
        status, _, body = self.raw_request(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 404)
        self.assertNotIn(b"private sentinel", body)

    def test_index_resolution_is_checked_even_without_symlink_permission(self) -> None:
        path_type = type(self.web)
        original_resolve = path_type.resolve
        private_target = (self.root / "private.txt").resolve()
        public_index = self.server.web_root / "index.html"

        def resolve_index(path: Path, strict: bool = False) -> Path:
            if path == public_index:
                return private_target
            return original_resolve(path, strict=strict)

        with patch.object(path_type, "resolve", resolve_index):
            status, _, body = self.raw_request(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 404)
        self.assertNotIn(b"private sentinel", body)

    def test_static_files_include_restrictive_csp(self) -> None:
        status, headers, body = self.raw_request(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"public dashboard")
        self.assertIn("script-src 'self'", headers["Content-Security-Policy"])
        self.assertIn("frame-ancestors 'none'", headers["Content-Security-Policy"])

    def test_semaphore_capacity_rejects_and_recovers(self) -> None:
        self.assertTrue(self.server._capacity.acquire(blocking=False))
        try:
            status, _, _ = self.raw_request(b"GET /api/v1/check HTTP/1.1\r\nHost: localhost\r\n\r\n")
            self.assertEqual(status, 503)
            self.assertEqual(self.application.requests, [])
        finally:
            self.server._capacity.release()
        status, _, _ = self.raw_request(b"GET /api/v1/check HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 200)

    def test_application_exception_is_sanitized_and_capacity_released(self) -> None:
        self.application.fail = True
        status, _, body = self.raw_request(b"GET /api/v1/check HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 500)
        self.assertNotIn(b"private database failure", body)
        self.assertEqual(json.loads(body)["error"]["code"], "INTERNAL_ERROR")
        self.application.fail = False
        status, _, _ = self.raw_request(b"GET /api/v1/check HTTP/1.1\r\nHost: localhost\r\n\r\n")
        self.assertEqual(status, 200)

    def test_correlation_identifier_is_bounded(self) -> None:
        status, _, body = self.post(b"{}", b"X-Correlation-ID: known-correlation\r\n")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["meta"]["correlation_id"], "known-correlation")
        _, _, body = self.post(b"{}", b"X-Correlation-ID: " + b"x" * 129 + b"\r\n")
        self.assertNotEqual(json.loads(body)["meta"]["correlation_id"], "x" * 129)

    def test_aware_datetimes_are_converted_to_utc(self) -> None:
        value = datetime(2026, 10, 1, 13, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))
        self.assertEqual(json_default(value), "2026-10-01T08:00:00Z")

    def test_invalid_resource_limits_fail_before_binding(self) -> None:
        for keyword in ({"max_connections": 0}, {"max_body_bytes": 0}, {"timeout_seconds": 0}):
            with self.subTest(keyword=keyword), self.assertRaises(ValueError):
                BoundedHTTPServer(("127.0.0.1", 0), self.application, self.web, **keyword)


if __name__ == "__main__":
    unittest.main()

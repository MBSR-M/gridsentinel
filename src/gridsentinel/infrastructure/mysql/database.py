"""Bounded MySQL pool, explicit transactions, and checksummed migrations."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
import hashlib
import logging
from pathlib import Path
import threading
import time
from typing import Any

from gridsentinel.core.exceptions import ConfigurationError, InfrastructureError, PoolTimeoutError
from gridsentinel.core.retry import RetryPolicy

LOGGER = logging.getLogger(__name__)


class Connection:
    """Query wrapper closing all owned cursors at the end of its scope."""

    def __init__(self, raw: Any, slow_query_seconds: float = 1.0) -> None:
        self.raw = raw
        self._cursors: list[Any] = []
        self._slow_query_seconds = slow_query_seconds

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        """Execute parameterized SQL without logging parameters or SQL literals."""
        cursor = self.raw.cursor()
        self._cursors.append(cursor)
        started = time.monotonic()
        try:
            cursor.execute(sql, params)
        finally:
            elapsed = time.monotonic() - started
            if elapsed >= self._slow_query_seconds:
                LOGGER.warning("Slow database query", extra={"details": {
                    "duration_seconds": elapsed,
                    "statement_digest": hashlib.sha256(sql.encode()).hexdigest()}})
        return cursor

    def fetch_one(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        """Return one dictionary row or None."""
        cursor = self.execute(sql, params)
        try:
            return cursor.fetchone()
        finally:
            cursor.close()
            self._cursors.remove(cursor)

    def fetch_all(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        """Return rows; callers must supply an explicit bounded query."""
        cursor = self.execute(sql, params)
        try:
            return list(cursor.fetchall())
        finally:
            cursor.close()
            self._cursors.remove(cursor)

    def close(self) -> None:
        """Release every cursor allocated in this transaction."""
        for cursor in self._cursors:
            cursor.close()
        self._cursors.clear()


class ConnectionPool:
    """Thread-safe finite pool with bounded acquisition and no reconnect replay."""

    def __init__(self, factory: Callable[[], Any], size: int = 8,
                 timeout: float = 5.0) -> None:
        if size < 1 or timeout <= 0:
            raise ConfigurationError("Invalid database connection pool limits")
        self.factory = factory
        self.size = size
        self.timeout = timeout
        self._condition = threading.Condition()
        self._idle: list[Any] = []
        self._total = 0
        self._closed = False

    def acquire(self) -> Any:
        """Lease an available connection or raise a bounded timeout."""
        deadline = time.monotonic() + self.timeout
        while True:
            create = False
            raw = None
            with self._condition:
                if self._closed:
                    raise InfrastructureError("Database pool is closed")
                if self._idle:
                    raw = self._idle.pop()
                elif self._total < self.size:
                    self._total += 1
                    create = True
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeoutError("Database connection pool exhausted")
                    self._condition.wait(remaining)
                    continue
            if create:
                try:
                    raw = self.factory()
                except Exception:
                    with self._condition:
                        self._total -= 1
                        self._condition.notify()
                    raise
            try:
                raw.ping(reconnect=False)
                return raw
            except Exception:
                self.release(raw, broken=True)
                if time.monotonic() >= deadline:
                    raise InfrastructureError("Database connection unavailable")

    def release(self, raw: Any, broken: bool = False) -> None:
        """Return a clean connection or discard a broken lease."""
        with self._condition:
            if broken or self._closed:
                try:
                    raw.close()
                finally:
                    self._total -= 1
            else:
                self._idle.append(raw)
            self._condition.notify()

    def close(self) -> None:
        """Close idle connections; active leases close when returned."""
        with self._condition:
            self._closed = True
            for raw in self._idle:
                raw.close()
            self._total -= len(self._idle)
            self._idle.clear()
            self._condition.notify_all()

    def metrics(self) -> dict[str, int]:
        """Return current pool utilization for operational health."""
        with self._condition:
            return {"size": self.size, "total": self._total,
                    "idle": len(self._idle), "active": self._total - len(self._idle)}

    def stats(self) -> dict[str, int]:
        """Return the stable platform metrics contract under the pool lock."""
        with self._condition:
            return {"size": self.size, "total": self._total,
                    "idle": len(self._idle), "in_use": self._total - len(self._idle)}


class Database:
    """MySQL connection abstraction injecting its configuration and factory."""

    def __init__(self, config: Any, factory: Callable[[], Any] | None = None) -> None:
        self.config = config
        self.pool = ConnectionPool(factory or self._connect,
                                   config.get_int("mysql", "pool_size", 8),
                                   config.get_float("mysql", "pool_timeout_seconds", 5.0))

    def _connect(self) -> Any:
        import pymysql

        ca = self.config.get("mysql", "ssl_ca", "")
        if self.config.get_bool("mysql", "require_tls", False) and not ca:
            raise ConfigurationError("MySQL TLS requires a server CA file")
        options = dict(host=self.config.get_required("mysql", "host"),
                       port=self.config.get_int("mysql", "port", 3306),
                       user=self.config.get_required("mysql", "user"),
                       password=self.config.get_required("mysql", "password"),
                       database=self.config.get_required("mysql", "database"),
                       charset="utf8mb4", autocommit=False,
                       cursorclass=pymysql.cursors.DictCursor,
                       connect_timeout=self.config.get_int("mysql", "connect_timeout_seconds", 5),
                       read_timeout=self.config.get_int("mysql", "read_timeout_seconds", 15),
                       write_timeout=self.config.get_int("mysql", "write_timeout_seconds", 15),
                       init_command="SET time_zone = '+00:00'")
        if ca:
            options.update(ssl_ca=str(ca), ssl_verify_cert=True, ssl_verify_identity=True)
        return pymysql.connect(**options)

    @contextmanager
    def transaction(self) -> Iterator[Connection]:
        """Commit a complete scope or roll back its entire mutation set."""
        raw = self.pool.acquire()
        wrapped = Connection(raw, self.config.get_float("mysql", "slow_query_seconds", 1.0))
        broken = False
        try:
            raw.begin()
            yield wrapped
            raw.commit()
        except BaseException:
            try:
                raw.rollback()
            except Exception:
                broken = True
            raise
        finally:
            try:
                wrapped.close()
            except Exception:
                broken = True
            self.pool.release(raw, broken)

    @contextmanager
    def read(self) -> Iterator[Connection]:
        """Provide a read scope that rolls back instead of committing effects."""
        raw = self.pool.acquire()
        wrapped = Connection(raw, self.config.get_float("mysql", "slow_query_seconds", 1.0))
        broken = False
        try:
            yield wrapped
        finally:
            try:
                raw.rollback()
                wrapped.close()
            except Exception:
                broken = True
            self.pool.release(raw, broken)

    def run_transaction(self, operation: Callable[[Connection], Any]) -> Any:
        """Retry entire transactions only for deadlocks/lock wait timeouts."""
        def run() -> Any:
            with self.transaction() as connection:
                return operation(connection)
        return RetryPolicy(self.config.get_int("mysql", "transaction_retry_attempts", 3)).run(
            run, retryable=lambda error: bool(error.args and error.args[0] in (1205, 1213)))

    def health(self) -> bool:
        """Check connectivity without revealing external failure details."""
        try:
            with self.read() as connection:
                return connection.fetch_one("SELECT 1 AS healthy")["healthy"] == 1
        except Exception:
            return False

    def close(self) -> None:
        """Close the pool for graceful process shutdown."""
        self.pool.close()

    def migrate(self, directory: str | Path) -> list[str]:
        """Serialize forward idempotent SQL migrations and verify checksums.

        MySQL DDL commits implicitly: failed DDL requires restore/repair, and
        migrations use CREATE IF NOT EXISTS to safely resume interrupted setup.
        """
        applied: list[str] = []
        with self.transaction() as connection:
            acquired = connection.fetch_one("SELECT GET_LOCK('gridsentinel:migrations', 30) AS acquired")
            if acquired["acquired"] != 1:
                raise InfrastructureError("Migration lock could not be acquired")
            try:
                connection.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(255) PRIMARY KEY, checksum CHAR(64) NOT NULL, applied_at DATETIME(6) NOT NULL) ENGINE=InnoDB")
                for path in sorted(Path(directory).glob("*.sql")):
                    sql = path.read_text(encoding="utf-8")
                    digest = hashlib.sha256(sql.encode()).hexdigest()
                    row = connection.fetch_one("SELECT checksum FROM schema_migrations WHERE version=%s", (path.name,))
                    if row:
                        if row["checksum"] != digest:
                            raise ConfigurationError(f"Applied migration checksum changed: {path.name}")
                        continue
                    # Repository migration grammar forbids routines and SQL comments containing semicolons.
                    content = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
                    for statement in content.split(";"):
                        if statement.strip():
                            connection.execute(statement)
                    connection.execute("INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(%s,%s,UTC_TIMESTAMP(6))", (path.name, digest))
                    connection.raw.commit()
                    applied.append(path.name)
            finally:
                connection.execute("SELECT RELEASE_LOCK('gridsentinel:migrations')")
        return applied

"""Audited, tenant-scoped jobs with transactional messaging and fenced leases."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any, Callable
import uuid

from gridsentinel.core.exceptions import ConflictError, NotFoundError, ValidationError
from gridsentinel.core.security import require_permission

JOB_PERMISSIONS = {
    "notification.send": "incidents.assign", "report.generate": "reports.export",
    "incident.enrich": "incidents.assign", "model.train": "models.train",
    "model.evaluate": "models.train", "integration.retry": "connectors.manage",
    "export.generate": "reports.export", "audit.anchor": "audit.export",
}
TERMINAL = frozenset({"SUCCEEDED", "FAILED", "CANCELED"})
STATES = TERMINAL | {"PENDING", "RUNNING", "RETRY"}


def utcnow() -> datetime:
    """Return a naive UTC timestamp compatible with the MySQL UTC session."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def as_json(value: Any) -> Any:
    """Decode a MySQL JSON column once."""
    return json.loads(value) if isinstance(value, (str, bytes)) else value


def canonical(value: Any) -> str:
    """Require JSON data without non-finite values or arbitrary Python objects."""
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError, RecursionError) as error:
        raise ValidationError("Job payload must be valid JSON") from error


class JobService:
    """Persist jobs, audits and publish intents atomically; never broker-only jobs."""

    def __init__(self, database: Any, config: Any, audit: Any,
                 clock: Callable[[], datetime] = utcnow) -> None:
        self.database, self.config, self.audit, self.clock = database, config, audit, clock

    def queue_for(self, job_type: str) -> str:
        """Resolve an explicit supported job type to its configured queue."""
        if job_type not in JOB_PERMISSIONS:
            raise ValidationError("Unsupported job type")
        queue = self.config.get("rabbitmq_queues", job_type, "q." + job_type)
        if not isinstance(queue, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,100}", queue):
            raise ValidationError("Invalid configured job queue")
        return queue

    def attempts(self) -> int:
        """Return a bounded retry budget for one requested execution cycle."""
        return max(1, min(5, self.config.get_int("rabbitmq", "retry_attempts", 5)))

    def delay(self, attempt: int) -> int:
        """Select a fixed retry tier; no immediate requeue retry loop is used."""
        base = max(1, self.config.get_int("rabbitmq", "retry_base_delay_seconds", 2))
        maximum = max(base, self.config.get_int("rabbitmq", "retry_max_delay_seconds", 300))
        return min(maximum, base * 2 ** max(0, min(attempt - 1, 4)))

    def envelope(self, row: dict[str, Any]) -> dict[str, Any]:
        """Build the persistent job contract from authoritative database data."""
        return {"job_id": row["job_id"], "job_type": row["job_type"],
                "created_at": row["created_at"].isoformat() + "Z",
                "correlation_id": row["correlation_id"], "tenant_id": row["tenant_id"],
                "attempt_number": row["attempt_number"], "payload": as_json(row["payload"]),
                "idempotency_key": row["idempotency_key"],
                "deadline": row["deadline"].isoformat() + "Z",
                "originating_user_id": row["originating_user_id"]}

    def _outbox(self, connection: Any, row: dict[str, Any], queue: str) -> None:
        connection.execute("INSERT INTO outbox_events(outbox_id,tenant_id,aggregate_id,event_type,topic,partition_key,payload,created_at) VALUES(%s,%s,%s,'JOB_QUEUED',%s,%s,%s,%s)",
                           (str(uuid.uuid4()), row["tenant_id"], row["job_id"], queue,
                            row["job_id"], canonical(self.envelope(row)), self.clock()))

    def create(self, principal: dict[str, Any], job_type: str, payload: dict[str, Any],
               correlation_id: str, idempotency_key: str) -> dict[str, Any]:
        """Create one authorized job; identical retries return the existing job."""
        queue = self.queue_for(job_type)
        require_permission(principal, JOB_PERMISSIONS[job_type])
        if (not isinstance(payload, dict) or not isinstance(idempotency_key, str)
                or not 1 <= len(idempotency_key) <= 128
                or any(ord(character) < 32 for character in idempotency_key)):
            raise ValidationError("Invalid job request")
        encoded = canonical(payload)
        if len(encoded.encode()) > self.config.get_int("rabbitmq", "max_payload_bytes", 262144):
            raise ValidationError("Job payload exceeds limit")
        if not isinstance(correlation_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", correlation_id):
            raise ValidationError("Invalid correlation identifier")
        now, job_id = self.clock(), str(uuid.uuid4())
        deadline = now + timedelta(seconds=self.config.get_int("rabbitmq", "job_deadline_seconds", 3600))

        def operation(connection: Any) -> dict[str, Any]:
            connection.execute("INSERT INTO background_jobs(job_id,tenant_id,job_type,queue,status,payload,correlation_id,idempotency_key,max_attempts,deadline,originating_user_id,created_at,updated_at) VALUES(%s,%s,%s,%s,'PENDING',%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE job_id=job_id",
                               (job_id, principal["tenant_id"], job_type, queue, encoded,
                                correlation_id, idempotency_key, self.attempts(), deadline,
                                principal["id"], now, now))
            row = connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND idempotency_key=%s FOR UPDATE", (principal["tenant_id"], idempotency_key))
            if row["job_type"] != job_type or canonical(as_json(row["payload"])) != encoded or row["originating_user_id"] != principal["id"]:
                raise ConflictError("Idempotency key was already used for another job")
            if row["job_id"] == job_id:
                self._outbox(connection, row, queue)
                self.audit.append(connection, principal["tenant_id"], principal["id"],
                                  "JOB_CREATE", "job", job_id, correlation_id,
                                  after={"job_type": job_type, "status": "PENDING"})
            return self.public(row)
        return self.database.run_transaction(operation)

    @staticmethod
    def public(row: dict[str, Any]) -> dict[str, Any]:
        """Return operational metadata without the potentially sensitive payload."""
        return {key: row.get(key) for key in (
            "job_id", "job_type", "queue", "status", "attempt_number", "max_attempts",
            "correlation_id", "created_at", "updated_at", "completed_at", "next_retry_at",
            "deadline", "originating_user_id", "last_error")}

    def list_jobs(self, principal: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
        """Return a bounded tenant page using an opaque-to-clients stable UUID cursor."""
        require_permission(principal, "jobs.read")
        try:
            limit = int(filters.get("limit", 25))
        except (ValueError, TypeError) as error:
            raise ValidationError("Invalid page size") from error
        if not 1 <= limit <= 100:
            raise ValidationError("Invalid page size")
        sql, values = "SELECT * FROM background_jobs WHERE tenant_id=%s", [principal["tenant_id"]]
        for field in ("status", "job_type"):
            value = filters.get(field)
            if value:
                if value not in (STATES if field == "status" else JOB_PERMISSIONS):
                    raise ValidationError("Invalid job filter")
                sql += f" AND {field}=%s"
                values.append(value)
        cursor = filters.get("cursor")
        if cursor:
            try:
                uuid.UUID(cursor)
            except (ValueError, TypeError, AttributeError) as error:
                raise ValidationError("Invalid page cursor") from error
            sql += " AND job_id>%s"
            values.append(cursor)
        values.append(limit + 1)
        with self.database.read() as connection:
            rows = connection.fetch_all(sql + " ORDER BY job_id LIMIT %s", values)
        return {"items": [self.public(row) for row in rows[:limit]],
                "next_cursor": rows[limit - 1]["job_id"] if len(rows) > limit else None}

    def job_action(self, principal: dict[str, Any], job_id: str, action: str,
                   correlation_id: str) -> dict[str, Any]:
        """Audit retry/cancel transitions; an active handler must respect fencing."""
        if action not in {"retry", "cancel"}:
            raise ValidationError("Unsupported job action")
        require_permission(principal, "jobs." + action)
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND job_id=%s FOR UPDATE", (principal["tenant_id"], job_id))
            if not row:
                raise NotFoundError("Job not found")
            if action == "retry":
                if row["status"] != "FAILED":
                    raise ConflictError("Only failed jobs may be retried")
                row.update(status="PENDING", max_attempts=row["attempt_number"] + self.attempts(),
                           deadline=self.clock() + timedelta(seconds=self.config.get_int("rabbitmq", "job_deadline_seconds", 3600)))
                connection.execute("UPDATE background_jobs SET status='PENDING',max_attempts=%s,deadline=%s,next_retry_at=NULL,locked_until=NULL,last_error=NULL,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (row["max_attempts"], row["deadline"], self.clock(), row["tenant_id"], job_id))
                self._outbox(connection, row, row["queue"])
            else:
                if row["status"] in TERMINAL:
                    raise ConflictError("A terminal job cannot be canceled")
                row["status"] = "CANCELED"
                connection.execute("UPDATE background_jobs SET status='CANCELED',locked_until=NULL,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (self.clock(), row["tenant_id"], job_id))
            self.audit.append(connection, row["tenant_id"], principal["id"], "JOB_" + action.upper(),
                              "job", job_id, correlation_id, after={"status": row["status"]})
            return self.public(row)

    def claim(self, tenant_id: str, job_id: str) -> dict[str, Any] | None:
        """Take one fenced lease or ignore a duplicate/terminal delivery."""
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND job_id=%s FOR UPDATE", (tenant_id, job_id))
            if not row or row["status"] in TERMINAL:
                return None
            now = self.clock()
            if row["status"] == "RUNNING" and row["locked_until"] and row["locked_until"] > now:
                return None
            if row["next_retry_at"] and row["next_retry_at"] > now:
                return None
            if row["deadline"] <= now or row["attempt_number"] >= row["max_attempts"]:
                self._failed(connection, row, "DEADLINE_OR_ATTEMPTS_EXHAUSTED")
                return None
            row.update(status="RUNNING", attempt_number=row["attempt_number"] + 1)
            row["locked_until"] = now + timedelta(seconds=max(3, self.config.get_int("rabbitmq", "lease_seconds", 60)))
            connection.execute("UPDATE background_jobs SET status='RUNNING',attempt_number=%s,locked_until=%s,next_retry_at=NULL,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (row["attempt_number"], row["locked_until"], now, tenant_id, job_id))
            row["payload"] = as_json(row["payload"])
            return row

    def renew(self, job: dict[str, Any]) -> bool:
        """Renew only the currently owned generation; cancellation loses ownership."""
        with self.database.transaction() as connection:
            cursor = connection.execute("UPDATE background_jobs SET locked_until=%s,updated_at=%s WHERE tenant_id=%s AND job_id=%s AND status='RUNNING' AND attempt_number=%s", (self.clock() + timedelta(seconds=max(3, self.config.get_int("rabbitmq", "lease_seconds", 60))), self.clock(), job["tenant_id"], job["job_id"], job["attempt_number"]))
            return cursor.rowcount == 1

    def complete(self, job: dict[str, Any], result: Any) -> bool:
        """Commit a result only while holding the same claim generation."""
        encoded = canonical(result)
        if len(encoded.encode()) > self.config.get_int("rabbitmq", "max_payload_bytes", 262144):
            raise ValidationError("Job result exceeds limit")
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND job_id=%s FOR UPDATE", (job["tenant_id"], job["job_id"]))
            if not row or row["status"] != "RUNNING" or row["attempt_number"] != job["attempt_number"]:
                return False
            if row["deadline"] <= self.clock():
                self._failed(connection, row, "JOB_DEADLINE_EXCEEDED")
                return False
            connection.execute("UPDATE background_jobs SET status='SUCCEEDED',result_json=%s,locked_until=NULL,completed_at=%s,updated_at=%s,last_error=NULL WHERE tenant_id=%s AND job_id=%s", (encoded, self.clock(), self.clock(), job["tenant_id"], job["job_id"]))
            self.audit.append(connection, job["tenant_id"], None, "JOB_SUCCEEDED", "job", job["job_id"], job["correlation_id"], metadata={"attempt": job["attempt_number"]})
            return True

    def _failed(self, connection: Any, row: dict[str, Any], code: str) -> None:
        row["status"] = "FAILED"
        connection.execute("UPDATE background_jobs SET status='FAILED',last_error=%s,locked_until=NULL,completed_at=%s,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (code, self.clock(), self.clock(), row["tenant_id"], row["job_id"]))
        self._outbox(connection, row, row["queue"] + ".dlq")
        self.audit.append(connection, row["tenant_id"], None, "JOB_FAILED", "job", row["job_id"], row["correlation_id"], outcome="FAILURE", metadata={"error_code": code, "attempt": row["attempt_number"]})

    def fail(self, job: dict[str, Any], code: str, retryable: bool = True) -> str:
        """Atomically record failure and the next fixed-TTL retry/DLQ publish intent."""
        code = re.sub(r"[^A-Za-z0-9_.:-]", "_", code)[:128]
        with self.database.transaction() as connection:
            row = connection.fetch_one("SELECT * FROM background_jobs WHERE tenant_id=%s AND job_id=%s FOR UPDATE", (job["tenant_id"], job["job_id"]))
            if not row or row["status"] != "RUNNING" or row["attempt_number"] != job["attempt_number"]:
                return "STALE"
            delay = self.delay(row["attempt_number"])
            if not retryable or row["attempt_number"] >= row["max_attempts"] or self.clock() + timedelta(seconds=delay) >= row["deadline"]:
                self._failed(connection, row, code)
                return "FAILED"
            row["status"] = "RETRY"
            next_retry = self.clock() + timedelta(seconds=delay)
            connection.execute("UPDATE background_jobs SET status='RETRY',next_retry_at=%s,locked_until=NULL,last_error=%s,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (next_retry, code, self.clock(), row["tenant_id"], row["job_id"]))
            self._outbox(connection, row, row["queue"] + ".retry." + str(delay))
            self.audit.append(connection, row["tenant_id"], None, "JOB_RETRY_SCHEDULED", "job", row["job_id"], row["correlation_id"], outcome="FAILURE", metadata={"error_code": code, "retry_after_seconds": delay})
            return "RETRY"

    def recover_expired(self, tenant_id: str) -> int:
        """Recover crashed worker leases and overdue persisted retries in bounded batches."""
        now = self.clock()
        with self.database.transaction() as connection:
            rows = connection.fetch_all("SELECT * FROM background_jobs WHERE tenant_id=%s AND ((status='RUNNING' AND locked_until<=%s) OR (status='RETRY' AND next_retry_at<=%s)) ORDER BY updated_at LIMIT 100 FOR UPDATE SKIP LOCKED", (tenant_id, now, now - timedelta(seconds=30)))
            for row in rows:
                if row["attempt_number"] >= row["max_attempts"] or row["deadline"] <= now:
                    self._failed(connection, row, "WORKER_LEASE_EXPIRED")
                else:
                    connection.execute("UPDATE background_jobs SET status='PENDING',locked_until=NULL,next_retry_at=NULL,updated_at=%s WHERE tenant_id=%s AND job_id=%s", (now, tenant_id, row["job_id"]))
                    self._outbox(connection, row, row["queue"])
            return len(rows)

    def publish_outbox(self, publisher: Callable[[str, dict[str, Any]], None],
                       tenant_id: str, batch_size: int = 50) -> int:
        """Mark publish only after broker confirmation; redelivery remains idempotent."""
        count = 0
        with self.database.transaction() as connection:
            rows = connection.fetch_all("SELECT * FROM outbox_events WHERE tenant_id=%s AND event_type='JOB_QUEUED' AND published_at IS NULL ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED", (tenant_id, max(1, min(batch_size, 100))))
            for row in rows:
                publisher(row["topic"], as_json(row["payload"]))
                connection.execute("UPDATE outbox_events SET published_at=%s,attempt_count=attempt_count+1,last_error=NULL WHERE tenant_id=%s AND outbox_id=%s", (self.clock(), tenant_id, row["outbox_id"]))
                count += 1
        return count

"""Concurrency-safe audit sequencing using one locked head row per tenant."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
from typing import Any

from gridsentinel.core.logging import sanitize
from gridsentinel.core.security import require_permission


def audit_digest(previous: bytes, payload: str) -> bytes:
    """Link deterministic canonical JSON to its preceding SHA-256 digest."""
    return hashlib.sha256(previous + payload.encode("utf-8")).digest()


class AuditService:
    """Write audits inside the caller's transaction, failing closed on failure."""

    def __init__(self, database: Any) -> None:
        self.database = database

    def append(self, connection: Any, tenant_id: str, actor_user_id: int | None,
               action: str, resource_type: str, resource_id: str | int,
               correlation_id: str, outcome: str = "SUCCESS", before: Any = None,
               after: Any = None, metadata: Any = None,
               session_id: str | None = None) -> bytes:
        """Lock, advance and append the tenant chain within one transaction."""
        # An upsert takes an exclusive row lock immediately. INSERT IGNORE can
        # leave shared duplicate-key locks that deadlock during lock upgrades.
        connection.execute("INSERT INTO audit_heads(tenant_id,sequence_number,last_hash) VALUES(%s,0,%s) ON DUPLICATE KEY UPDATE sequence_number=sequence_number", (tenant_id, bytes(32)))
        head = connection.fetch_one("SELECT sequence_number,last_hash FROM audit_heads WHERE tenant_id=%s FOR UPDATE", (tenant_id,))
        sequence = head["sequence_number"] + 1
        timestamp = datetime.now(timezone.utc).replace(tzinfo=None)
        before, after, metadata = sanitize(before), sanitize(after), sanitize(metadata)
        event = {"tenant_id": tenant_id, "sequence_number": sequence,
                 "occurred_at": timestamp.isoformat(timespec="microseconds") + "Z",
                 "actor_user_id": actor_user_id, "session_id": session_id,
                 "action": action, "resource_type": resource_type, "resource_id": str(resource_id),
                 "correlation_id": correlation_id, "outcome": outcome,
                 "before": before, "after": after, "metadata": metadata}
        payload = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
        digest = audit_digest(head["last_hash"], payload)
        connection.execute("INSERT INTO audit_events(tenant_id,sequence_number,occurred_at,actor_user_id,session_id,action,resource_type,resource_id,correlation_id,outcome,before_state,after_state,metadata,canonical_payload,previous_hash,event_hash) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                           (tenant_id, sequence, timestamp, actor_user_id, session_id, action, resource_type,
                            str(resource_id), correlation_id, outcome, json.dumps(before), json.dumps(after),
                            json.dumps(metadata), payload, head["last_hash"], digest))
        connection.execute("UPDATE audit_heads SET sequence_number=%s,last_hash=%s WHERE tenant_id=%s", (sequence, digest, tenant_id))
        return digest

    def list_events(self, principal: dict[str, Any], limit: int = 100,
                    after_id: int = 0) -> list[dict[str, Any]]:
        """Read bounded, tenant-scoped audit events without exposing raw tokens."""
        require_permission(principal, "audit.read")
        with self.database.read() as connection:
            rows = connection.fetch_all("SELECT id,sequence_number,occurred_at,actor_user_id,session_id,action,resource_type,resource_id,correlation_id,outcome,before_state,after_state,metadata,HEX(event_hash) AS event_hash FROM audit_events WHERE tenant_id=%s AND id>%s ORDER BY id LIMIT %s", (principal["tenant_id"], max(0, after_id), max(1, min(limit, 500))))
            return rows

    def verify(self, tenant_id: str) -> dict[str, Any]:
        """Verify the complete chain in bounded pages and compare its head.

        This detects accidental edits; an external retained digest is necessary
        to detect a privileged database administrator rewriting the full chain.
        """
        previous = bytes(32)
        sequence = 0
        with self.database.read() as connection:
            # One repeatable-read transaction gives a consistent head and chain.
            head = connection.fetch_one("SELECT sequence_number,last_hash FROM audit_heads WHERE tenant_id=%s", (tenant_id,))
            while True:
                rows = connection.fetch_all("SELECT tenant_id,sequence_number,occurred_at,actor_user_id,session_id,action,resource_type,resource_id,correlation_id,outcome,before_state,after_state,metadata,canonical_payload,previous_hash,event_hash FROM audit_events WHERE tenant_id=%s AND sequence_number>%s ORDER BY sequence_number LIMIT 500", (tenant_id, sequence))
                if not rows:
                    break
                for row in rows:
                    sequence += 1
                    if row["sequence_number"] != sequence or not hmac.compare_digest(row["previous_hash"], previous):
                        return {"valid": False, "checked": sequence, "reason": "CHAIN_ORDER"}
                    expected = audit_digest(previous, row["canonical_payload"])
                    if not hmac.compare_digest(expected, row["event_hash"]):
                        return {"valid": False, "checked": sequence, "reason": "HASH_MISMATCH"}
                    try:
                        stored = {"tenant_id": row["tenant_id"], "sequence_number": row["sequence_number"],
                                  "occurred_at": row["occurred_at"].isoformat(timespec="microseconds") + "Z",
                                  "actor_user_id": row["actor_user_id"], "session_id": row["session_id"],
                                  "action": row["action"], "resource_type": row["resource_type"],
                                  "resource_id": row["resource_id"], "correlation_id": row["correlation_id"],
                                  "outcome": row["outcome"], "before": json.loads(row["before_state"]),
                                  "after": json.loads(row["after_state"]), "metadata": json.loads(row["metadata"])}
                        canonical = json.loads(row["canonical_payload"])
                    except (TypeError, ValueError):
                        return {"valid": False, "checked": sequence, "reason": "MALFORMED_PAYLOAD"}
                    if stored != canonical:
                        return {"valid": False, "checked": sequence, "reason": "COLUMN_MISMATCH"}
                    previous = expected
            valid = (head is None and sequence == 0) or (head is not None and head["sequence_number"] == sequence and hmac.compare_digest(head["last_hash"], previous))
        return {"valid": valid, "checked": sequence, "digest": previous.hex()}

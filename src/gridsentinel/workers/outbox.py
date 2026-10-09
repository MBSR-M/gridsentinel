"""Durable confirmed outbox dispatcher; publishing may repeat after a crash."""

from __future__ import annotations

import json
from typing import Any


def publish_kafka(database: Any, publisher: Any, tenant_id: str,
                  limit: int = 100) -> int:
    """Lock a bounded batch and mark records only after broker acknowledgment."""
    with database.transaction() as connection:
        rows = connection.fetch_all("SELECT outbox_id,topic,partition_key,payload FROM outbox_events WHERE tenant_id=%s AND event_type<>'JOB_QUEUED' AND published_at IS NULL ORDER BY created_at LIMIT %s FOR UPDATE SKIP LOCKED", (tenant_id, max(1, min(limit, 500))))
        for row in rows:
            payload = json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
            publisher.publish(row["topic"], row["partition_key"], payload)
            connection.execute("UPDATE outbox_events SET published_at=UTC_TIMESTAMP(6),attempt_count=attempt_count+1,last_error=NULL WHERE tenant_id=%s AND outbox_id=%s", (tenant_id, row["outbox_id"]))
    return len(rows)

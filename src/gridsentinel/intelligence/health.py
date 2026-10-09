"""Communication and data-delivery health remain independent."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from gridsentinel.domain.events import utc

COMMUNICATION = frozenset({"METER_COMM_SUCCESS", "METER_COMM_FAILURE", "POWER_FAILURE", "POWER_RESTORED"})


@dataclass(frozen=True)
class MeterHealth:
    """Deterministic state with observed communication and delivery indicators."""
    current_status: str = "UNKNOWN"
    previous_status: str = "UNKNOWN"
    state_entered_at: datetime | None = None
    last_seen_at: datetime | None = None
    consecutive_failures: int = 0
    missing_interval_count: int = 0
    late_interval_count: int = 0
    communication_score: float | None = None
    state_confidence: float = 0.0
    delivery_status: str = "UNKNOWN"
    last_event_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return an independent dataclass mapping."""
        return asdict(self)


def derive_health(events: Iterable[dict[str, Any]], as_of: datetime,
                  expected_minutes: int = 60, grace_multiplier: float = 3,
                  offline_failures: int = 3, recovery_successes: int = 2,
                  installation_status: str = "ACTIVE",
                  previous_status: str = "UNKNOWN",
                  last_seen_at: datetime | None = None,
                  missing_intervals: int = 0, late_intervals: int = 0) -> MeterHealth:
    """Fold source-time facts, never infer offline solely from absent MDMS data."""
    ordered = sorted((row for row in events if _time(row) <= as_of),
                     key=lambda row: (_time(row), row.get("source_sequence") or 0,
                                      row.get("source_event_id", row.get("event_id", ""))))
    status, failures, successes = "UNKNOWN", 0, 0
    entered, latest_id = None, None
    communication_total = communication_successes = 0
    for row in ordered:
        kind, at = row["event_type"], _time(row)
        latest_id = row.get("event_id", row.get("source_event_id"))
        old = status
        if kind in {"METER_COMM_SUCCESS", "POWER_RESTORED"}:
            last_seen_at = max(last_seen_at, at) if last_seen_at else at
            successes += 1
            failures = 0
            status = "RECOVERING" if old in {"OFFLINE", "DEGRADED", "RECOVERING"} and successes < recovery_successes else "HEALTHY"
            communication_total += 1
            communication_successes += 1
        elif kind == "METER_COMM_FAILURE":
            failures += 1
            successes = 0
            status = "OFFLINE" if failures >= offline_failures else "DEGRADED"
            communication_total += 1
        elif kind == "POWER_FAILURE":
            failures += 1
            successes = 0
            status = "OFFLINE"
        if old != status:
            entered = at
    if status == "UNKNOWN" and last_seen_at:
        status = previous_status if previous_status in {"HEALTHY", "DEGRADED", "RECOVERING", "OFFLINE"} else "HEALTHY"
    if last_seen_at and as_of - last_seen_at > timedelta(minutes=expected_minutes * grace_multiplier):
        status = "OFFLINE"
        entered = last_seen_at + timedelta(minutes=expected_minutes * grace_multiplier)
    if installation_status in {"DECOMMISSIONED", "REMOVED"}:
        status = "DECOMMISSIONED"
    delivery = "MISSING" if missing_intervals else "LATE" if late_intervals else "UNKNOWN"
    if status == "HEALTHY" and missing_intervals:
        status = "DELAYED"
    score = communication_successes / communication_total if communication_total else None
    return MeterHealth(status, previous_status, entered, last_seen_at, failures,
        missing_intervals, late_intervals, score, 1.0 if status != "UNKNOWN" else 0.0,
        delivery, latest_id)


def _time(row: dict[str, Any]) -> datetime:
    value = row.get("event_time", row.get("event_time_utc"))
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return utc(value)

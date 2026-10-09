"""Point-in-time features, observed recovery labels and temporal evaluation."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta
import math
from typing import Any
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import utc
FEATURES = ('outage_duration_hours', 'failures_last_24h', 'recoveries_last_30d', 'gateway_failure_ratio', 'shared_outage_size', 'power_alarm_present', 'hour_of_day')

@dataclass(frozen=True)
class RecoveryEpisode:
    """Feature snapshot and follow-up for observed communication recovery."""
    episode_id: str
    meter_id: str
    as_of: datetime
    observed_until: datetime
    recovered_at: datetime | None
    features: tuple[float, ...]
    stratum: str
    intervention_occurred: bool | None = None

    @classmethod
    def from_dict(cls, row: dict[str, Any]) -> RecoveryEpisode:
        """Validate bounded fixed-schema features and reject future leakage."""
        required={'episode_id','meter_id','as_of','observed_until','feature_observed_at','features'}
        if not isinstance(row,dict) or not required.issubset(row):
            raise ValidationError('Recovery episode is missing required schema fields')
        as_of, observed = (utc(row['as_of']), utc(row['observed_until']))
        recovered = utc(row['recovered_at']) if row.get('recovered_at') else None
        if observed < as_of or (recovered and (recovered < as_of or recovered > observed)):
            raise ValidationError('Recovery follow-up is temporally inconsistent')
        if not row.get('feature_observed_at') or utc(row['feature_observed_at']) > as_of:
            raise ValidationError('Features must be observed at or before prediction time')
        source = row.get('features')
        if not isinstance(source, dict) or set(source) != set(FEATURES):
            raise ValidationError('Feature snapshot must exactly match recovery-1.0')
        values = []
        for name in FEATURES:
            value = source[name]
            if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (value > 1000000000.0):
                raise ValidationError('Recovery features must be bounded finite numbers')
            values.append(float(value))
        if not 0 <= source['gateway_failure_ratio'] <= 1 or source['power_alarm_present'] not in (0, 1) or (not 0 <= source['hour_of_day'] <= 23):
            raise ValidationError('Recovery feature range violated')
        intervention = row.get('intervention_occurred')
        if intervention is not None and type(intervention) is not bool:
            raise ValidationError('Intervention status must be boolean or unknown')
        for name in ('episode_id', 'meter_id'):
            if not isinstance(row.get(name), str) or not 1 <= len(row[name]) <= 128:
                raise ValidationError('Episode and meter identifiers are required')
        stratum = row.get('communication_type', 'UNKNOWN')
        if not isinstance(stratum, str) or len(stratum) > 64:
            raise ValidationError('Invalid communication stratum')
        return cls(row['episode_id'], row['meter_id'], as_of, observed, recovered, tuple(values), stratum, intervention)

def build_labels(episodes: list[RecoveryEpisode], horizon_hours: int) -> tuple[list[tuple[RecoveryEpisode, int]], int]:
    """Exclude right-censored negatives; target observed recovery, not spontaneous."""
    if horizon_hours not in (6, 12, 24):
        raise ValidationError('Recovery horizon must be6,12 or24 hours')
    labelled = []
    censored = 0
    seen = set()
    for row in episodes:
        if row.episode_id in seen:
            raise ValidationError('Repeated outage episodes would leak across splits')
        seen.add(row.episode_id)
        end = row.as_of + timedelta(hours=horizon_hours)
        if row.recovered_at and row.recovered_at <= end:
            labelled.append((row, 1))
        elif row.observed_until >= end:
            labelled.append((row, 0))
        else:
            censored += 1
    return (sorted(labelled, key=lambda pair: (pair[0].as_of, pair[0].episode_id)), censored)

def temporal_split(rows: list[tuple[RecoveryEpisode, int]]) -> tuple[list[Any], list[Any], list[Any]]:
    """Split whole timestamp cohorts chronologically into train/validation/test."""
    times = sorted(set((row.as_of for row, _ in rows)))
    if len(times) < 5:
        raise ValidationError('Temporal evaluation requires at least5 distinct prediction times')
    validation_at = times[max(1, int(len(times) * 0.6))]
    test_at = times[max(2, int(len(times) * 0.8))]
    train = [pair for pair in rows if pair[0].as_of < validation_at]
    validation = [pair for pair in rows if validation_at <= pair[0].as_of < test_at]
    test = [pair for pair in rows if pair[0].as_of >= test_at]
    if not all((train, validation, test)):
        raise ValidationError('Temporal split produced an empty evaluation cohort')
    return (train, validation, test)

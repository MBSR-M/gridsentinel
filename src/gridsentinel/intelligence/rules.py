"""Explainable correlation, anomaly detection, RCA and approved SLA contracts."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
import math
import statistics
from typing import Any, Iterable
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import utc

def correlate_gateway(failed_meters: Iterable[str], active_count: int, minimum_count: int, minimum_ratio: float) -> dict[str, Any]:
    """Group distinct failed meters only against a known eligible population."""
    affected = sorted(set(failed_meters))
    ratio = len(affected) / active_count if active_count else None
    return {'affected_meters': affected, 'affected_meter_count': len(affected), 'failure_ratio': ratio, 'is_shared_failure': bool(ratio is not None and len(affected) >= minimum_count and (ratio >= minimum_ratio))}

def rank_causes(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    """Rank suspected causes by explicit rule support, never proven diagnosis."""
    rules = {'GATEWAY_FAILURE': [('gateway_alarm', 4), ('shared_gateway_failure', 2), ('neighbor_gateways_healthy', 1)], 'DCU_FAILURE': [('dcu_alarm', 4), ('shared_dcu_failure', 2)], 'NETWORK_PROVIDER_ISSUE': [('provider_alarm', 4), ('multiple_gateway_failure', 2)], 'LOW_SIGNAL_QUALITY': [('low_signal', 2)], 'POWER_INTERRUPTION': [('power_alarm', 4)], 'HES_PROCESSING_DELAY': [('hes_delay', 3)], 'HES_MDMS_INTEGRATION_DELAY': [('hes_received_mdms_absent', 3)], 'MDMS_VALIDATION_FAILURE': [('validation_failure', 4)], 'METER_FIRMWARE_ISSUE': [('firmware_alarm', 3)], 'TOPOLOGY_MAPPING_ERROR': [('topology_conflict', 4)], 'METER_SPECIFIC_FAILURE': [('isolated_failure', 2)]}
    candidates = []
    for cause, factors in rules.items():
        supporting = [name for name, _ in factors if evidence.get(name) is True]
        score = sum((weight for name, weight in factors if evidence.get(name) is True))
        contrary = []
        if cause in {'GATEWAY_FAILURE', 'DCU_FAILURE'} and evidence.get('device_heartbeat_ok') is True:
            score = max(0, score - 3)
            contrary.append('device_heartbeat_ok')
        if score:
            candidates.append({'cause': cause, 'evidence_score': score, 'support': supporting, 'contradictions': contrary, 'status': 'SUSPECTED', 'confidence_kind': 'RULE_SUPPORT', 'confidence': min(score / 7, 1.0)})
    return sorted(candidates, key=lambda row: (-row['evidence_score'], row['cause'])) or [{'cause': 'UNKNOWN', 'evidence_score': 0, 'support': [], 'contradictions': [], 'status': 'SUSPECTED', 'confidence_kind': 'RULE_SUPPORT', 'confidence': 0.0}]

def anomaly_summary(values: list[float], current: float, alpha: float=0.2) -> dict[str, Any]:
    """Return robust and EWMA indicators from bounded historical observations."""
    if not values or not 0 < alpha <= 1 or (not all((math.isfinite(v) for v in values + [current]))):
        raise ValidationError('Anomaly inputs require finite historical observations')
    median = statistics.median(values)
    mad = statistics.median((abs(v - median) for v in values))
    ewma = values[0]
    for value in values[1:]:
        ewma = alpha * value + (1 - alpha) * ewma
    severity = abs(current - median) / (1.4826 * mad) if mad else 0 if current == median else None
    return {'median': median, 'mad': mad, 'ewma': ewma, 'robust_score': severity, 'change': current - ewma, 'anomaly': current != median if mad == 0 else severity > 3}

def evaluate_sla(contract: dict[str, Any] | None, deliveries: list[dict[str, Any]]) -> dict[str, Any]:
    """Evaluate an explicitly approved delivery contract; never invent a KPI."""
    if not contract or contract.get('approval_status') != 'APPROVED':
        return {'status': 'UNCONFIGURED', 'kpi': None, 'risk': None, 'reason': 'An approved customer SLA contract is required'}
    if contract.get('formula') != 'INTERVAL_DELIVERY_BY_DEADLINE':
        raise ValidationError('Unsupported approved SLA formula')
    deadline = utc(contract['deadline_utc'])
    excluded = set(contract.get('excluded_meter_ids', []))
    allowed_quality = set(contract.get('accepted_quality', ['VALID']))
    eligible = [row for row in deliveries if row['meter_id'] not in excluded]
    physical = estimated = on_time = 0
    for row in eligible:
        at = row.get('mdms_received_at')
        if row.get('estimated_flag'):
            estimated += 1
            continue
        if at and row.get('quality_status') in allowed_quality:
            physical += 1
            at = at.replace(tzinfo=deadline.tzinfo) if isinstance(at, datetime) and at.tzinfo is None else utc(at)
            on_time += at <= deadline
    population = contract.get('expected_eligible_intervals')
    if type(population) is not int or population < 0:
        raise ValidationError('Approved SLA must supply expected eligible intervals after exclusions')
    if len(eligible) > population:
        raise ValidationError('Observed SLA rows exceed the approved eligible population')
    kpi = on_time / population if population else None
    target = contract.get('target_ratio')
    if type(target) not in (int, float) or not 0 <= target <= 1:
        raise ValidationError('Approved SLA target must be between zero and one')
    return {'status': 'EVALUATED', 'contract_id': contract.get('contract_id'), 'eligible_intervals': population, 'physical_deliveries': physical, 'estimated_intervals': estimated, 'on_time_deliveries': on_time, 'outstanding_intervals': max(0, population - physical), 'unobserved_intervals': max(0, population - len(eligible)), 'kpi': kpi, 'target': target, 'risk': None if kpi is None else max(0, target - kpi), 'possible_final_range': [kpi, 1.0] if population else [None, None]}

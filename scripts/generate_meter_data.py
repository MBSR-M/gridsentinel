"""Stream seeded synthetic fleets, known outages and recovery training episodes."""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
import sys
from typing import Any, Iterator
import uuid
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from gridsentinel.domain.events import canonical_json, stamp

def iter_events(meter_count: int, events_per_meter: int=4, seed: int=42, start: datetime | None=None, scenario: str='mixed', tenant_id: str='demo') -> Iterator[dict[str, Any]]:
    """Generate a logical fleet lazily with explicit scenario ground truth."""
    if not 1 <= meter_count <= 100000000 or not 1 <= events_per_meter <= 100 or scenario not in {'mixed', 'healthy', 'gateway', 'power', 'mdms', 'network'}:
        raise ValueError('Invalid synthetic generation bounds')
    rng = random.Random(seed)
    initial = start or datetime.now(timezone.utc) - timedelta(minutes=max(120, 40 + (events_per_meter - 1) * 10 + 61))
    for index in range(meter_count):
        meter = f'SYN-MTR-{index:010d}'
        gateway = f'SYN-GW-{index // 50:07d}'
        region = f'SYN-REG-{index // 1000:05d}'
        selected = scenario if scenario != 'mixed' else ['healthy', 'gateway', 'power', 'mdms', 'network'][index // 50 % 5]

        def envelope(kind: str, at: datetime, payload: dict[str, Any], sequence: int, source: str='synthetic_hes') -> dict[str, Any]:
            event_id = f'seed{seed}.{meter}.{sequence}.{kind}'
            correlation = str(uuid.uuid5(uuid.NAMESPACE_URL, event_id))
            delay = 3600 if selected == 'mdms' and kind.startswith('INTERVAL') else rng.randrange(1, 30)
            received = at + timedelta(seconds=delay)
            return {'schema_version': '1.0', 'event_id': event_id, 'event_type': kind, 'source_system': source, 'tenant_id': tenant_id, 'meter_id': meter, 'event_time_utc': stamp(at), 'received_at_utc': stamp(received), 'ingested_at_utc': stamp(received + timedelta(seconds=1)), 'correlation_id': correlation, 'trace_id': correlation, 'source_sequence': sequence, 'payload': {'synthetic': True, 'ground_truth_scenario': selected, **payload}}
        yield envelope('METER_INSTALLED', initial, {'manufacturer_id': 'SYN-VENDOR-1', 'model': 'SYNTHETIC-A', 'communication_type': 'CELLULAR' if index % 2 else 'RF', 'firmware_version': 'SYN-1.0', 'region_id': region, 'expected_communication_minutes': 60}, 0, 'synthetic_gis')
        yield envelope('TOPOLOGY_UPDATE', initial, {'gateway_id': gateway, 'dcu_id': f'SYN-DCU-{index // 100:07d}', 'feeder_id': f'SYN-FEED-{index // 1000:05d}', 'transformer_id': f'SYN-TX-{index // 100:07d}', 'sim_provider_id': 'SYN-CARRIER-' + str(index // 50 % 3), 'region_id': region, 'region_name': 'Synthetic Region ' + str(index // 1000), 'valid_from': stamp(initial), 'mapping_quality': 'SYNTHETIC_GROUND_TRUTH'}, 1, 'synthetic_gis')
        yield envelope('METER_COMM_SUCCESS', initial + timedelta(minutes=10), {'gateway_id': gateway, 'signal_strength': -70 + rng.randrange(-8, 8), 'status': 'SUCCESS'}, 2)
        for event_index in range(events_per_meter):
            at = initial + timedelta(minutes=40 + event_index * 10)
            if selected in {'gateway', 'network'}:
                kind = 'METER_COMM_FAILURE'
                payload = {'gateway_id': gateway, 'status': 'FAILED', 'error_code': 'SYN_TIMEOUT', 'ground_truth_cause': 'GATEWAY_FAILURE' if selected == 'gateway' else 'NETWORK_PROVIDER_ISSUE'}
            elif selected == 'power':
                kind = 'POWER_FAILURE' if event_index < events_per_meter - 1 else 'POWER_RESTORED'
                payload = {'gateway_id': gateway, 'ground_truth_cause': 'POWER_INTERRUPTION', 'operator_confirmed_outcome': 'POWER_RESTORED' if kind == 'POWER_RESTORED' else None}
            elif selected == 'mdms':
                kind = 'INTERVAL_MISSING'
                payload = {'interval_start_utc': stamp(at - timedelta(minutes=30)), 'interval_end_utc': stamp(at), 'hes_received_at': stamp(at + timedelta(seconds=15)), 'mdms_received_at': None, 'estimation_flag': False, 'quality_flag': 'UNKNOWN', 'ground_truth_cause': 'HES_MDMS_INTEGRATION_DELAY'}
            else:
                kind = 'METER_COMM_SUCCESS'
                payload = {'gateway_id': gateway, 'status': 'SUCCESS', 'signal_strength': -68 + rng.randrange(-10, 10)}
            yield envelope(kind, at, payload, event_index + 3, 'synthetic_mdms' if kind.startswith('INTERVAL') else 'synthetic_hes')
        if selected == 'gateway' and index % 50 == 0:
            at = initial + timedelta(minutes=39)
            row = envelope('GATEWAY_DISCONNECTED', at, {'gateway_id': gateway, 'ground_truth_cause': 'GATEWAY_FAILURE'}, 100)
            row['meter_id'] = None
            yield row

def iter_episodes(count: int=200, seed: int=42) -> Iterator[dict[str, Any]]:
    """Produce known synthetic observed/censored episodes for algorithm checks."""
    rng = random.Random(seed)
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    for index in range(count):
        as_of = start + timedelta(days=index)
        shared = rng.random()
        recover = shared < 0.5
        duration = rng.uniform(0.2, 6)
        censored = index % 17 == 0
        recovered = as_of + timedelta(hours=rng.uniform(1, 8)) if recover and (not censored) else None
        yield {'episode_id': f'SYN-EP-{seed}-{index}', 'meter_id': f'SYN-MTR-{index:010d}', 'as_of': stamp(as_of), 'feature_observed_at': stamp(as_of), 'observed_until': stamp(as_of + timedelta(hours=3 if censored else 25)), 'recovered_at': stamp(recovered) if recovered else None, 'communication_type': 'RF' if index % 2 else 'CELLULAR', 'intervention_occurred': index % 13 == 0, 'features': {'outage_duration_hours': duration, 'failures_last_24h': rng.randrange(1, 15), 'recoveries_last_30d': rng.randrange(0, 8), 'gateway_failure_ratio': shared, 'shared_outage_size': int(shared * 50), 'power_alarm_present': index % 2, 'hour_of_day': rng.randrange(0, 24)}}

def main() -> None:
    """Write finite JSONL events or a bounded recovery training JSON file."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--meters', type=int, default=1000)
    parser.add_argument('--events-per-meter', type=int, default=4)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--max-events', type=int, default=10000)
    parser.add_argument('--tenant', default='demo')
    parser.add_argument('--scenario', default='mixed')
    parser.add_argument('--output', type=Path, default=Path('artifacts/synthetic_events.jsonl'))
    parser.add_argument('--episodes', type=int, default=0)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.episodes:
        if not 5 <= args.episodes <= 20000:
            parser.error('Episode count must be between5 and20000')
        args.output.write_text(canonical_json({'data_source': 'synthetic', 'horizon_hours': 12, 'episodes': list(iter_episodes(args.episodes, args.seed))}), encoding='utf-8')
        print(json.dumps({'synthetic': True, 'episodes': args.episodes, 'output': str(args.output)}))
        return
    count = 0
    with args.output.open('w', encoding='utf-8', newline='\n') as stream:
        for row in iter_events(args.meters, args.events_per_meter, args.seed, scenario=args.scenario, tenant_id=args.tenant):
            if count >= args.max_events:
                break
            stream.write(canonical_json(row) + '\n')
            count += 1
    print(json.dumps({'synthetic': True, 'logical_meters': args.meters, 'emitted_events': count, 'seed': args.seed, 'output': str(args.output)}))
if __name__ == '__main__':
    main()

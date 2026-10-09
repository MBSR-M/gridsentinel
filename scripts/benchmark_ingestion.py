"""Measure local canonical validation or MySQL transaction ingestion honestly."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics
import sys
import time
import tracemalloc
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from generate_meter_data import iter_events
from gridsentinel.domain.events import validate_event

def main() -> None:
    """Collect measured throughput, latency and memory with explicit scope."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--events', type=int, default=10000)
    parser.add_argument('--mode', choices=['validation', 'mysql'], default='validation')
    parser.add_argument('--tenant', default='demo')
    parser.add_argument('--environment', default='development')
    parser.add_argument('--output', type=Path, default=Path('artifacts/benchmark_ingestion.json'))
    args = parser.parse_args()
    if not 1 <= args.events <= 1000000:
        parser.error('Event bound is1 to1000000')
    operation = validate_event
    database = None
    if args.mode == 'mysql':
        from gridsentinel.core.configuration import ConfigManager
        from gridsentinel.infrastructure.mysql import Database
        from gridsentinel.services.audit import AuditService
        from gridsentinel.processing.operations import OperationsService
        config = ConfigManager(Path(__file__).resolve().parents[1] / 'config', args.environment)
        database = Database(config)
        operation = OperationsService(database, config, AuditService(database)).ingest
    tracemalloc.start()
    started = time.perf_counter()
    latencies = []
    count = duplicates = 0
    try:
        for event in iter_events(max(1, args.events // 7 + 1), tenant_id=args.tenant):
            mark = time.perf_counter()
            outcome = operation(event)
            latencies.append(time.perf_counter() - mark)
            count += 1
            if isinstance(outcome, dict) and outcome.get('duplicate'):
                duplicates += 1
            if count >= args.events:
                break
    finally:
        if database:
            database.close()
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    latencies.sort()
    percentile = lambda fraction: latencies[min(len(latencies) - 1, int(len(latencies) * fraction))] * 1000
    result = {'scope': args.mode, 'synthetic': True, 'events': count, 'duplicate_events': duplicates, 'unique_events': count - duplicates, 'elapsed_seconds': elapsed, 'events_per_second': count / elapsed, 'latency_ms': {'p50': percentile(0.5), 'p95': percentile(0.95), 'p99': percentile(0.99)}, 'peak_traced_memory_bytes': peak, 'measured_at': datetime.now(timezone.utc).isoformat(), 'limitations': 'Single local process; validation mode excludes broker/database. No fleet-scale capacity claim.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2), encoding='utf-8')
    print(json.dumps(result, indent=2))
if __name__ == '__main__':
    main()

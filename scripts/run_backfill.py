"""Resume bounded canonical JSONL historical backfill with durable checkpoints."""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from gridsentinel.connectors.adapters import FileConnector
from gridsentinel.core.configuration import ConfigManager
from gridsentinel.infrastructure.mysql import Database
from gridsentinel.services.audit import AuditService
from gridsentinel.processing.operations import OperationsService, sql_time

def main() -> None:
    """Advance line checkpoints only after committed idempotent domain effects."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--tenant', required=True)
    parser.add_argument('--connector', default='backfill')
    parser.add_argument('--batch-size', type=int, default=500)
    parser.add_argument('--environment', default='development')
    args = parser.parse_args()
    config = ConfigManager(Path(__file__).resolve().parents[1] / 'config', args.environment)
    database = Database(config)
    service = OperationsService(database, config, AuditService(database))
    shard = hashlib.sha256(str(args.file.resolve()).encode()).hexdigest()[:64]
    try:
        with database.read() as connection:
            checkpoint = connection.fetch_one('SELECT watermark FROM connector_checkpoints WHERE tenant_id=%s AND connector_id=%s AND source_shard=%s', (args.tenant, args.connector, shard))
        start = int(checkpoint['watermark']) if checkpoint else 0
        processed = 0
        for line, row in FileConnector(args.file, max_records=args.batch_size).records(start):
            if row.get('tenant_id') != args.tenant:
                raise ValueError('Backfill cannot cross its authorized tenant')
            service.ingest(row)
            with database.transaction() as connection:
                now = sql_time(datetime.now(timezone.utc))
                connection.execute('INSERT INTO connector_checkpoints(tenant_id,connector_id,source_shard,watermark,source_event_id,last_successful_extract,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE watermark=VALUES(watermark),source_event_id=VALUES(source_event_id),last_successful_extract=VALUES(last_successful_extract),updated_at=VALUES(updated_at)', (args.tenant, args.connector, shard, str(line), row['event_id'], now, now))
            processed += 1
        print(f'Committed {processed} events; repeat the same command to resume')
    finally:
        database.close()
if __name__ == '__main__':
    main()

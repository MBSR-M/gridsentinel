"""Read-only configured file, HTTP and incremental MySQL source adapters."""
from __future__ import annotations
from collections.abc import Callable, Iterator, Mapping
import configparser
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler
import uuid
from gridsentinel.core.exceptions import ValidationError
from gridsentinel.domain.events import canonical_json, stamp, validate_event

class VendorNormalizer:
    """Normalize only explicitly mapped vendor fields and event codes."""

    def __init__(self, tenant_id: str, source_system: str, field_mapping: Mapping[str, str], code_mapping: Mapping[str, str]) -> None:
        self.tenant_id, self.source_system = (tenant_id, source_system)
        self.field_mapping, self.code_mapping = (dict(field_mapping), dict(code_mapping))

    @classmethod
    def from_ini(cls, path: Path, tenant_id: str, source_system: str, vendor: str) -> VendorNormalizer:
        """Read vendor-specific mappings from an external INI file."""
        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str
        if not parser.read(path, encoding='utf-8') or not parser.has_section(vendor + '.fields') or (not parser.has_section(vendor + '.events')):
            raise ValidationError('Vendor field and event mappings are required')
        return cls(tenant_id, source_system, dict(parser.items(vendor + '.fields')), dict(parser.items(vendor + '.events')))

    def normalize(self, raw: Mapping[str, Any], received_at: datetime | None=None) -> dict[str, Any]:
        """Preserve source ID and reject unmapped vendor event semantics."""
        fields = {canonical: raw.get(source) for canonical, source in self.field_mapping.items()}
        code = fields.pop('event_code', None)
        kind = self.code_mapping.get(str(code))
        if kind is None:
            raise ValidationError('Unmapped vendor event code')
        now = received_at or datetime.now(timezone.utc)
        event_id = fields.pop('event_id', None)
        meter_id = fields.pop('meter_id', None)
        at = fields.pop('event_time_utc', None)
        sequence = fields.pop('source_sequence', None)
        stable = str(uuid.uuid5(uuid.NAMESPACE_URL, f'{self.tenant_id}:{self.source_system}:{event_id}'))
        envelope = {'schema_version': '1.0', 'tenant_id': self.tenant_id, 'source_system': self.source_system, 'event_id': event_id, 'meter_id': meter_id, 'event_type': kind, 'event_time_utc': at, 'received_at_utc': stamp(now), 'ingested_at_utc': stamp(now), 'correlation_id': stable, 'trace_id': stable, 'payload': {name: value for name, value in fields.items() if value is not None}, 'raw_payload_reference': 'sha256:' + hashlib.sha256(canonical_json(dict(raw)).encode()).hexdigest()}
        if sequence is not None:
            if isinstance(sequence, str) and sequence.isdigit():
                sequence = int(sequence)
            envelope['source_sequence'] = sequence
        return validate_event(envelope).to_dict()

class FileConnector:
    """Stream bounded JSONL or CSV records without loading a whole export."""

    def __init__(self, path: Path, format: str='jsonl', max_record_bytes: int=65536, max_records: int=10000) -> None:
        self.path, self.format, self.max_record_bytes, self.max_records = (path, format, max_record_bytes, max_records)
        if format not in {'jsonl', 'csv'} or max_records < 1 or max_record_bytes < 1:
            raise ValidationError('Invalid file adapter limits')

    def records(self, after_line: int=0) -> Iterator[tuple[int, dict[str, Any]]]:
        """Yield source line checkpoints only when the caller durably hands off."""
        count = 0
        with self.path.open('rb') as stream:
            if self.format == 'csv':
                header_bytes = stream.readline(self.max_record_bytes + 1)
                if len(header_bytes) > self.max_record_bytes:
                    raise ValidationError('CSV header exceeds record limit')
                header = next(csv.reader([header_bytes.decode('utf-8-sig')]))
                if not header or len(header) != len(set(header)):
                    raise ValidationError('CSV requires unique column names')
            for index in range(1, 2 ** 63):
                line = stream.readline(self.max_record_bytes + 1)
                if not line:
                    break
                if len(line) > self.max_record_bytes:
                    raise ValidationError('Source record exceeds configured limit')
                if index <= after_line:
                    continue
                try:
                    text = line.decode('utf-8-sig')
                    if self.format == 'jsonl':
                        row = json.loads(text, parse_constant=lambda _: (_ for _ in ()).throw(ValueError('non-finite')))
                    else:
                        values = next(csv.reader([text], strict=True))
                        if len(values) != len(header):
                            raise ValueError('column mismatch')
                        row = dict(zip(header, values))
                except (ValueError, UnicodeError, csv.Error) as error:
                    raise ValidationError('Source record is malformed') from error
                if not isinstance(row, dict):
                    raise ValidationError('Source record must be an object')
                yield (index, row)
                count += 1
                if count >= self.max_records:
                    break

class NoRedirect(HTTPRedirectHandler):
    """Disallow redirects that could bypass the approved host boundary."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        raise ValidationError('Source API redirects are not authorized')

class HttpConnector:
    """Fetch bounded JSON from an explicitly approved HTTPS source endpoint."""

    def __init__(self, url: str, allowed_hosts: list[str], token: str | None=None, timeout_seconds: float=10, max_response_bytes: int=1048576, max_records: int=1000, opener: Any=None) -> None:
        target = urlsplit(url)
        if target.scheme != 'https' or target.hostname not in allowed_hosts or target.username or target.password or target.fragment:
            raise ValidationError('Source API requires an authorized HTTPS host')
        self.url, self.token, self.timeout = (url, token, timeout_seconds)
        self.max_response_bytes, self.max_records = (max_response_bytes, max_records)
        self.opener = opener or build_opener(NoRedirect())
        if timeout_seconds <= 0 or max_response_bytes < 1 or max_records < 1:
            raise ValidationError('Invalid source API limits')

    def records(self) -> list[dict[str, Any]]:
        """Fetch one finite source page; retries belong to the durable connector job."""
        headers = {'Accept': 'application/json'}
        if self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        with self.opener.open(Request(self.url, headers=headers), timeout=self.timeout) as response:
            if response.headers.get_content_type() != 'application/json':
                raise ValidationError('Source API returned an unsupported content type')
            body = response.read(self.max_response_bytes + 1)
            if len(body) > self.max_response_bytes:
                raise ValidationError('Source API response exceeds configured limit')
        try:
            data = json.loads(body)
            canonical_json(data)
        except (ValueError, UnicodeError) as error:
            raise ValidationError('Source API returned malformed JSON') from error
        rows = data.get('items') if isinstance(data, dict) else data
        if not isinstance(rows, list) or len(rows) > self.max_records or any((not isinstance(row, dict) for row in rows)):
            raise ValidationError('Source API page violates record bounds')
        return rows

class MySQLIncrementalConnector:
    """Read approved source tables with an update-time plus source-ID watermark."""

    def __init__(self, database: Any, table: str, columns: list[str], updated_column: str, id_column: str, batch_size: int=500) -> None:
        identifiers = [table, updated_column, id_column, *columns]
        if not all((re.fullmatch('[A-Za-z_][A-Za-z0-9_]{0,63}', name) for name in identifiers)) or not 1 <= batch_size <= 10000:
            raise ValidationError('Invalid approved source query configuration')
        if updated_column not in columns or id_column not in columns:
            raise ValidationError('Watermark fields must be selected')
        self.database, self.table, self.columns = (database, table, columns)
        self.updated_column, self.id_column, self.batch_size = (updated_column, id_column, batch_size)

    def records(self, watermark: datetime, source_id: str='') -> list[dict[str, Any]]:
        """Read a stable bounded increment using read-only database credentials."""
        update, key = (f'`{self.updated_column}`', f'`{self.id_column}`')
        query = 'SELECT ' + ','.join((f'`{name}`' for name in self.columns)) + f' FROM `{self.table}` WHERE ({update}>%s OR ({update}=%s AND {key}>%s)) ORDER BY {update},{key} LIMIT %s'
        with self.database.read() as connection:
            return connection.fetch_all(query, (watermark, watermark, source_id, self.batch_size))

def handoff_batch(records: Iterator[tuple[Any, dict[str, Any]]], publish_durably: Callable[[dict[str, Any]], Any], save_checkpoint: Callable[[Any], None]) -> int:
    """Advance checkpoints after durable delivery, preserving at-least-once replay."""
    count = 0
    for watermark, row in records:
        publish_durably(row)
        save_checkpoint(watermark)
        count += 1
    return count

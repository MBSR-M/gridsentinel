"""Tenant-scoped telemetry effects committed atomically with a durable outbox."""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import math
from typing import Any
import uuid
from gridsentinel.core.exceptions import AuthorizationError, ConflictError, NotFoundError, ValidationError
from gridsentinel.domain.events import CanonicalEvent, canonical_json, identifier, stamp, utc, validate_event
from gridsentinel.intelligence.health import derive_health
from gridsentinel.intelligence.rules import correlate_gateway, evaluate_sla, rank_causes

def sql_time(value: datetime | None) -> datetime | None:
    """Convert UTC domain timestamps to MySQL's timezone-free UTC convention."""
    return value.astimezone(timezone.utc).replace(tzinfo=None) if value else None

def public(value: Any) -> Any:
    """Convert repository values into JSON-safe public response values."""
    if isinstance(value, datetime):
        return stamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {key: public(item) for key, item in value.items()}
    if isinstance(value, list):
        return [public(item) for item in value]
    return value

def decoded(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Decode known JSON columns without interpreting arbitrary strings."""
    if row is None:
        return None
    for name in ('evidence_json', 'result_json', 'before_state', 'after_state', 'definition_json', 'metrics_json', 'explanation_json', 'feature_snapshot'):
        if isinstance(row.get(name), str):
            row[name] = json.loads(row[name])
    return row

class OperationsService:
    """Implement validated telemetry persistence, intelligence and workflows."""

    def __init__(self, database: Any, config: Any, audit: Any) -> None:
        self.database, self.config, self.audit = (database, config, audit)

    def ingest(self, event: dict[str, Any]) -> dict[str, Any]:
        """Commit idempotency, facts, derived state and outgoing events together."""
        value = validate_event(event, self.config.get_int('ingestion', 'max_payload_bytes', 65536), self.config.get_int('ingestion', 'max_clock_skew_seconds', 300))
        now = datetime.now(timezone.utc)
        with self.database.transaction() as connection:
            cursor = connection.execute("INSERT IGNORE INTO event_deduplication(tenant_id,namespace,idempotency_key,first_observed_at,event_time,processing_status) VALUES(%s,%s,%s,%s,%s,'COMPLETED')", (value.tenant_id, value.source_system, value.deduplication_key, sql_time(now), sql_time(value.event_time)))
            if cursor.rowcount == 0:
                return {'duplicate': True, 'event_id': value.event_id, 'deduplication_key': value.deduplication_key}
            payload = value.payload
            meter = None
            if value.meter_id:
                cadence = payload.get('expected_communication_minutes', self.config.get_int('health', 'expected_communication_minutes', 60))
                if type(cadence) is not int or not 1 <= cadence <= 10080:
                    raise ValidationError('Invalid communication cadence')
                connection.execute("INSERT IGNORE INTO meters(tenant_id,meter_id,installation_status,expected_communication_minutes,created_at,updated_at) VALUES(%s,%s,'ACTIVE',%s,%s,%s)", (value.tenant_id, value.meter_id, cadence, sql_time(now), sql_time(now)))
                meter = connection.fetch_one('SELECT meter_id,installation_status,expected_communication_minutes FROM meters WHERE tenant_id=%s AND meter_id=%s FOR UPDATE', (value.tenant_id, value.meter_id))
            if value.event_type == 'TOPOLOGY_UPDATE':
                self._topology(connection, value)
            elif value.event_type in {'METER_INSTALLED', 'METER_DECOMMISSIONED'}:
                status = 'ACTIVE' if value.event_type == 'METER_INSTALLED' else 'DECOMMISSIONED'
                connection.execute('UPDATE meters SET installation_status=%s,manufacturer_id=%s,model=%s,firmware_version=%s,communication_type=%s,region_id=%s,installed_at=COALESCE(installed_at,%s),deactivated_at=%s,updated_at=%s WHERE tenant_id=%s AND meter_id=%s', (status, self._bounded(payload.get('manufacturer_id'), 64), self._bounded(payload.get('model'), 64), self._bounded(payload.get('firmware_version'), 64), self._bounded(payload.get('communication_type'), 24), payload.get('region_id'), sql_time(value.event_time), sql_time(value.event_time) if status == 'DECOMMISSIONED' else None, sql_time(now), value.tenant_id, value.meter_id))
                meter['installation_status'] = status
            elif value.event_type.startswith('INTERVAL'):
                self._interval(connection, value)
            mapping = self._mapping(connection, value.tenant_id, value.meter_id, value.event_time) if value.meter_id else None
            gateway = mapping.get('gateway_id') if mapping else payload.get('gateway_id')
            if not value.event_type.startswith('INTERVAL') and value.event_type != 'TOPOLOGY_UPDATE':
                self._event(connection, value, gateway)
            if not value.meter_id:
                device = payload.get('gateway_id') or payload.get('dcu_id')
                status = 'HEALTHY' if value.event_type == 'GATEWAY_RESTORED' else 'OFFLINE'
                connection.execute("INSERT INTO network_devices(tenant_id,device_id,device_type,status,last_event_at,created_at,updated_at) VALUES(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE status=IF(VALUES(last_event_at)>=COALESCE(last_event_at,'1000-01-01'),VALUES(status),status),last_event_at=GREATEST(COALESCE(last_event_at,'1000-01-01'),VALUES(last_event_at)),updated_at=VALUES(updated_at)", (value.tenant_id, device, 'GATEWAY' if payload.get('gateway_id') else 'DCU', status, sql_time(value.event_time), sql_time(now), sql_time(now)))
            health = self._health(connection, value.tenant_id, value.meter_id, now, meter, value.event_time) if meter else None
            incident = self._correlate(connection, value, gateway) if gateway and value.event_type in {'METER_COMM_FAILURE', 'METER_SILENT', 'POWER_FAILURE', 'GATEWAY_DISCONNECTED', 'DCU_DISCONNECTED'} else None
            semantic = 'intervals' if value.event_type.startswith('INTERVAL') else 'topology' if value.event_type == 'TOPOLOGY_UPDATE' else 'network' if not value.meter_id else 'communication'
            self._outbox(connection, value.tenant_id, value.partition_key, value.event_type, semantic, value.to_dict())
            if health:
                self._outbox(connection, value.tenant_id, value.meter_id, 'METER_HEALTH_UPDATED', 'meter_health', {'tenant_id': value.tenant_id, 'meter_id': value.meter_id, 'health': public(health), 'correlation_id': value.correlation_id})
            return {'duplicate': False, 'event_id': value.event_id, 'deduplication_key': value.deduplication_key, 'health': public(health), 'incident_id': incident, 'late': (value.received_at - value.event_time).total_seconds() > self.config.get_int('ingestion', 'allowed_lateness_seconds', 900)}

    def _bounded(self, value: Any, maximum: int) -> str | None:
        if value is not None and (not isinstance(value, str) or len(value) > maximum):
            raise ValidationError('Source field exceeds its typed storage limit')
        return value

    def _mapping(self, connection: Any, tenant: str, meter: str, at: datetime) -> dict[str, Any] | None:
        return connection.fetch_one('SELECT mapping_id,gateway_id,dcu_id,region_id,mapping_quality FROM meter_topology_history WHERE tenant_id=%s AND meter_id=%s AND valid_from<=%s AND (valid_to IS NULL OR valid_to>%s) ORDER BY valid_from DESC LIMIT 1', (tenant, meter, sql_time(at), sql_time(at)))

    def _topology(self, connection: Any, event: CanonicalEvent) -> None:
        payload = event.payload
        start, end = (utc(payload['valid_from']), utc(payload['valid_to']) if payload.get('valid_to') else None)
        overlaps = connection.fetch_all('SELECT mapping_id,valid_from,valid_to FROM meter_topology_history WHERE tenant_id=%s AND meter_id=%s AND (%s IS NULL OR valid_from<%s) AND (valid_to IS NULL OR valid_to>%s) ORDER BY valid_from LIMIT 2 FOR UPDATE', (event.tenant_id, event.meter_id, sql_time(end), sql_time(end), sql_time(start)))
        for row in overlaps:
            if row['valid_to'] is None and row['valid_from'] < sql_time(start):
                connection.execute('UPDATE meter_topology_history SET valid_to=%s WHERE mapping_id=%s', (sql_time(start), row['mapping_id']))
            else:
                raise ConflictError('Topology effective ranges overlap')
        connection.execute('INSERT INTO meter_topology_history(mapping_id,tenant_id,meter_id,gateway_id,dcu_id,feeder_id,transformer_id,sim_provider_id,region_id,valid_from,valid_to,source_system,mapping_quality) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (str(uuid.uuid5(uuid.NAMESPACE_URL, event.deduplication_key)), event.tenant_id, event.meter_id, payload.get('gateway_id'), payload.get('dcu_id'), payload.get('feeder_id'), payload.get('transformer_id'), payload.get('sim_provider_id'), payload.get('region_id'), sql_time(start), sql_time(end), event.source_system, self._bounded(payload.get('mapping_quality', 'SOURCE_REPORTED'), 24)))
        if payload.get('region_id'):
            connection.execute('INSERT IGNORE INTO regions(tenant_id,region_id,display_name) VALUES(%s,%s,%s)', (event.tenant_id, payload['region_id'], payload.get('region_name', payload['region_id'])))
        connection.execute('UPDATE meters SET region_id=%s,updated_at=%s WHERE tenant_id=%s AND meter_id=%s', (payload.get('region_id'), sql_time(event.ingested_at), event.tenant_id, event.meter_id))

    def _interval(self, connection: Any, event: CanonicalEvent) -> None:
        payload = event.payload
        start, end = (utc(payload['interval_start_utc']), utc(payload['interval_end_utc']))
        received = utc(payload['mdms_received_at']) if payload.get('mdms_received_at') else None
        hes = utc(payload['hes_received_at']) if payload.get('hes_received_at') else None
        estimated = payload.get('estimation_flag', False)
        delivery = 'ESTIMATED' if estimated else 'MISSING' if event.event_type == 'INTERVAL_MISSING' or received is None else 'LATE' if (received - end).total_seconds() > self.config.get_int('ingestion', 'allowed_lateness_seconds', 900) else 'RECEIVED'
        connection.execute('INSERT INTO interval_delivery_status(tenant_id,meter_id,interval_start,interval_end,interval_minutes,hes_received_at,mdms_received_at,quality_status,estimated_flag,delivery_status,source_system,event_time) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE hes_received_at=IF(VALUES(event_time)>=event_time,VALUES(hes_received_at),hes_received_at),mdms_received_at=IF(VALUES(event_time)>=event_time,VALUES(mdms_received_at),mdms_received_at),quality_status=IF(VALUES(event_time)>=event_time,VALUES(quality_status),quality_status),estimated_flag=IF(VALUES(event_time)>=event_time,VALUES(estimated_flag),estimated_flag),delivery_status=IF(VALUES(event_time)>=event_time,VALUES(delivery_status),delivery_status),event_time=GREATEST(event_time,VALUES(event_time))', (event.tenant_id, event.meter_id, sql_time(start), sql_time(end), math.ceil((end - start).total_seconds() / 60), sql_time(hes), sql_time(received), self._bounded(payload.get('quality_flag', 'UNKNOWN'), 24), estimated, delivery, event.source_system, sql_time(event.event_time)))

    def _event(self, connection: Any, event: CanonicalEvent, gateway: str | None) -> None:
        payload = event.payload
        connection.execute('INSERT INTO communication_events(event_id,tenant_id,source_event_id,meter_id,gateway_id,dcu_id,event_time,received_at,ingested_at,event_type,error_code,status,source_system,source_sequence,trace_id,correlation_id,payload_reference,evidence_json,is_late) VALUES(' + ','.join(['%s'] * 19) + ')', (event.deduplication_key, event.tenant_id, event.event_id, event.meter_id, gateway, payload.get('dcu_id'), sql_time(event.event_time), sql_time(event.received_at), sql_time(event.ingested_at), event.event_type, self._bounded(payload.get('error_code'), 64), self._bounded(payload.get('status'), 24), event.source_system, event.source_sequence, event.trace_id, event.correlation_id, event.raw_payload_reference, event.payload_json, (event.received_at - event.event_time).total_seconds() > self.config.get_int('ingestion', 'allowed_lateness_seconds', 900)))

    def _health(self, connection: Any, tenant: str, meter_id: str, now: datetime, meter: dict[str, Any], recompute_at: datetime | None=None) -> dict[str, Any]:
        old = connection.fetch_one('SELECT current_status,state_entered_at,last_seen_at FROM meter_current_health WHERE tenant_id=%s AND meter_id=%s FOR UPDATE', (tenant, meter_id))
        rows = connection.fetch_all("SELECT event_id,source_event_id,event_type,event_time,source_sequence FROM communication_events WHERE tenant_id=%s AND meter_id=%s AND event_type IN ('METER_COMM_SUCCESS','METER_COMM_FAILURE','POWER_FAILURE','POWER_RESTORED') ORDER BY event_time DESC,source_sequence DESC,source_event_id DESC LIMIT %s", (tenant, meter_id, self.config.get_int('ingestion', 'health_replay_events', 2048)))
        seen = connection.fetch_one("SELECT MAX(event_time) AS last_seen FROM communication_events WHERE tenant_id=%s AND meter_id=%s AND event_type IN ('METER_COMM_SUCCESS','POWER_RESTORED')", (tenant, meter_id))
        last_seen = max([v for v in [seen.get('last_seen'), old.get('last_seen_at') if old else None] if v], default=None)
        if last_seen:
            last_seen = last_seen.replace(tzinfo=timezone.utc)
        deliveries = connection.fetch_one("SELECT COALESCE(SUM(delivery_status='MISSING'),0) AS missing,COALESCE(SUM(delivery_status='LATE'),0) AS late FROM interval_delivery_status WHERE tenant_id=%s AND meter_id=%s AND interval_start>=%s AND interval_start<=%s", (tenant, meter_id, sql_time(now - timedelta(days=1)), sql_time(now)))
        health = derive_health(rows, now, meter['expected_communication_minutes'], self.config.get_float('health', 'cadence_grace_multiplier', 3), self.config.get_int('health', 'offline_after_failures', 3), self.config.get_int('health', 'recovery_successes', 2), meter['installation_status'], old['current_status'] if old else 'UNKNOWN', last_seen, int(deliveries['missing']), int(deliveries['late'])).to_dict()
        if old and old['current_status'] == health['current_status']:
            health['state_entered_at'] = old['state_entered_at']
        mapping = self._mapping(connection, tenant, meter_id, now)
        gateway = mapping.get('gateway_id') if mapping else None
        connection.execute('INSERT INTO meter_current_health(tenant_id,meter_id,last_seen_at,current_status,previous_status,state_entered_at,consecutive_failures,missing_interval_count,late_interval_count,communication_score,state_confidence,delivery_status,gateway_id,last_event_id,updated_at) VALUES(' + ','.join(['%s'] * 15) + ') ON DUPLICATE KEY UPDATE last_seen_at=VALUES(last_seen_at),current_status=VALUES(current_status),previous_status=VALUES(previous_status),state_entered_at=VALUES(state_entered_at),consecutive_failures=VALUES(consecutive_failures),missing_interval_count=VALUES(missing_interval_count),late_interval_count=VALUES(late_interval_count),communication_score=VALUES(communication_score),state_confidence=VALUES(state_confidence),delivery_status=VALUES(delivery_status),gateway_id=VALUES(gateway_id),last_event_id=VALUES(last_event_id),updated_at=VALUES(updated_at)', (tenant, meter_id, sql_time(health['last_seen_at']), health['current_status'], health['previous_status'], sql_time(health['state_entered_at']) if isinstance(health['state_entered_at'], datetime) and health['state_entered_at'].tzinfo else health['state_entered_at'], health['consecutive_failures'], health['missing_interval_count'], health['late_interval_count'], health['communication_score'], health['state_confidence'], health['delivery_status'], gateway, health['last_event_id'], sql_time(now)))
        if rows or recompute_at:
            hour = (sql_time(recompute_at) if recompute_at else max((row['event_time'] for row in rows))).replace(minute=0, second=0, microsecond=0)
            aggregate = connection.fetch_one("SELECT COALESCE(SUM(event_type='METER_COMM_SUCCESS'),0) AS successes,COALESCE(SUM(event_type='METER_COMM_FAILURE'),0) AS failures,COALESCE(SUM(is_late AND event_type='METER_COMM_SUCCESS'),0) AS late FROM communication_events WHERE tenant_id=%s AND meter_id=%s AND event_time>=%s AND event_time<%s", (tenant, meter_id, hour, hour + timedelta(hours=1)))
            total = int(aggregate['successes']) + int(aggregate['failures'])
            missing = connection.fetch_one("SELECT COUNT(*) AS count FROM interval_delivery_status WHERE tenant_id=%s AND meter_id=%s AND interval_start>=%s AND interval_start<%s AND delivery_status='MISSING'", (tenant, meter_id, hour, hour + timedelta(hours=1)))
            connection.execute('INSERT INTO meter_health_hourly(tenant_id,meter_id,hour_start,expected_reads,on_time_reads,late_reads,missing_reads,failure_count,communication_score) VALUES(%s,%s,%s,NULL,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE on_time_reads=VALUES(on_time_reads),late_reads=VALUES(late_reads),missing_reads=VALUES(missing_reads),failure_count=VALUES(failure_count),communication_score=VALUES(communication_score)', (tenant, meter_id, hour, max(0, int(aggregate['successes']) - int(aggregate['late'])), int(aggregate['late']), int(missing['count']), int(aggregate['failures']), int(aggregate['successes']) / total if total else None))
            day = hour.replace(hour=0)
            daily = connection.fetch_one('SELECT COALESCE(SUM(on_time_reads+late_reads),0) AS successes,COALESCE(SUM(failure_count),0) AS failures,COALESCE(SUM(late_reads),0) AS late,COALESCE(SUM(missing_reads),0) AS missing FROM meter_health_hourly WHERE tenant_id=%s AND meter_id=%s AND hour_start>=%s AND hour_start<%s', (tenant, meter_id, day, day + timedelta(days=1)))
            connection.execute('INSERT INTO meter_health_daily(tenant_id,meter_id,day_start,observed_successes,failure_count,late_reads,missing_reads) VALUES(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE observed_successes=VALUES(observed_successes),failure_count=VALUES(failure_count),late_reads=VALUES(late_reads),missing_reads=VALUES(missing_reads)', (tenant, meter_id, day.date(), int(daily['successes']), int(daily['failures']), int(daily['late']), int(daily['missing'])))
        return health

    def _correlate(self, connection: Any, event: CanonicalEvent, gateway: str) -> str | None:
        seconds = self.config.get_int('incident', 'correlation_window_minutes', 15) * 60
        start = datetime.fromtimestamp(int(event.event_time.timestamp()) // seconds * seconds, timezone.utc)
        end = start + timedelta(seconds=seconds)
        eligible = connection.fetch_one('SELECT COUNT(*) AS count FROM meter_topology_history WHERE tenant_id=%s AND gateway_id=%s AND valid_from<=%s AND (valid_to IS NULL OR valid_to>%s)', (event.tenant_id, gateway, sql_time(event.event_time), sql_time(event.event_time)))
        failures = connection.fetch_all("SELECT DISTINCT e.meter_id FROM communication_events e JOIN meter_topology_history t ON t.tenant_id=e.tenant_id AND t.meter_id=e.meter_id AND t.valid_from<=e.event_time AND (t.valid_to IS NULL OR t.valid_to>e.event_time) WHERE e.tenant_id=%s AND t.gateway_id=%s AND e.event_time>=%s AND e.event_time<%s AND e.event_type IN ('METER_COMM_FAILURE','METER_SILENT','POWER_FAILURE') LIMIT %s", (event.tenant_id, gateway, sql_time(start), sql_time(end), self.config.get_int('incident', 'maximum_cluster_meter_count', 100000)))
        grouping = correlate_gateway([row['meter_id'] for row in failures], int(eligible['count']), self.config.get_int('incident', 'minimum_cluster_meter_count', 10), self.config.get_float('incident', 'shared_gateway_failure_ratio', 0.7))
        device_alarm = event.event_type in {'GATEWAY_DISCONNECTED', 'DCU_DISCONNECTED'}
        if not grouping['is_shared_failure'] and (not device_alarm):
            return None
        group = f'gateway:{gateway}:{stamp(start)}'
        incident_id = str(uuid.uuid5(uuid.NAMESPACE_URL, event.tenant_id + ':' + group))
        existing = connection.fetch_one('SELECT status,affected_meter_count,suspected_root_cause FROM incidents WHERE tenant_id=%s AND incident_id=%s FOR UPDATE', (event.tenant_id, incident_id))
        alarm = connection.fetch_one('SELECT event_type,event_time FROM communication_events WHERE tenant_id=%s AND gateway_id=%s AND meter_id IS NULL AND event_time>=%s AND event_time<%s ORDER BY event_time DESC LIMIT 1', (event.tenant_id, gateway, sql_time(start), sql_time(end)))
        evidence = {'shared_gateway_failure': grouping['is_shared_failure'], 'gateway_alarm': bool(alarm and alarm['event_type'] == 'GATEWAY_DISCONNECTED'), 'dcu_alarm': event.event_type == 'DCU_DISCONNECTED', 'power_alarm': event.event_type == 'POWER_FAILURE', 'device_heartbeat_ok': bool(alarm and alarm['event_type'] == 'GATEWAY_RESTORED')}
        candidates = rank_causes(evidence)
        top = candidates[0]
        now = sql_time(datetime.now(timezone.utc))
        if not existing:
            connection.execute("INSERT INTO incidents(incident_id,tenant_id,incident_type,severity,status,suspected_root_cause,root_cause_confidence,affected_meter_count,gateway_id,detected_at,first_event_at,correlation_group_id,created_at,updated_at) VALUES(%s,%s,'SHARED_COMMUNICATION_FAILURE','HIGH','OPEN',%s,%s,%s,%s,%s,%s,%s,%s,%s)", (incident_id, event.tenant_id, top['cause'], top['confidence'], grouping['affected_meter_count'], gateway, now, sql_time(event.event_time), group, now, now))
        else:
            connection.execute('UPDATE incidents SET affected_meter_count=%s,suspected_root_cause=%s,root_cause_confidence=%s,first_event_at=LEAST(first_event_at,%s),updated_at=%s WHERE tenant_id=%s AND incident_id=%s', (grouping['affected_meter_count'], top['cause'], top['confidence'], sql_time(event.event_time), now, event.tenant_id, incident_id))
        state = {'affected_meter_count': grouping['affected_meter_count'], 'candidates': candidates, 'failure_ratio': grouping['failure_ratio'], 'status': existing['status'] if existing else 'OPEN'}
        connection.execute("INSERT IGNORE INTO incident_evidence(tenant_id,incident_id,evidence_type,source_system,source_reference,event_time,evidence_json,created_at) VALUES(%s,%s,'CORRELATION_WINDOW',%s,%s,%s,%s,%s)", (event.tenant_id, incident_id, event.source_system, event.deduplication_key, sql_time(event.event_time), canonical_json({**state, 'signals': evidence, 'historical_gateway_id': gateway, 'mapping_as_of': stamp(event.event_time)}), now))
        connection.execute('INSERT INTO incident_history(tenant_id,incident_id,change_type,before_state,after_state,correlation_id,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s)', (event.tenant_id, incident_id, 'EVIDENCE_UPDATED' if existing else 'CREATED', canonical_json(existing) if existing else None, canonical_json(state), event.correlation_id, now))
        connection.execute('INSERT INTO gateway_health_snapshots(tenant_id,gateway_id,window_start,window_end,active_meter_count,offline_meter_count,failure_rate,computed_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE active_meter_count=VALUES(active_meter_count),offline_meter_count=VALUES(offline_meter_count),failure_rate=VALUES(failure_rate),computed_at=VALUES(computed_at)', (event.tenant_id, gateway, sql_time(start), sql_time(end), eligible['count'], grouping['affected_meter_count'], grouping['failure_ratio'], now))
        self._outbox(connection, event.tenant_id, incident_id, 'INCIDENT_UPDATED' if existing else 'INCIDENT_CREATED', 'incidents_updated' if existing else 'incidents_created', {'tenant_id': event.tenant_id, 'incident_id': incident_id, 'correlation_id': event.correlation_id, **state})
        return incident_id

    def _outbox(self, connection: Any, tenant: str, aggregate: str, kind: str, semantic: str, payload: dict[str, Any]) -> None:
        defaults = {'communication': 'ami.normalized.communication', 'intervals': 'ami.normalized.intervals', 'network': 'ami.normalized.network', 'topology': 'ami.normalized.topology', 'meter_health': 'ami.derived.meter_health', 'incidents_created': 'ami.incidents.created', 'incidents_updated': 'ami.incidents.updated', 'incidents_resolved': 'ami.incidents.resolved'}
        connection.execute('INSERT INTO outbox_events(outbox_id,tenant_id,aggregate_id,event_type,topic,partition_key,payload,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', (str(uuid.uuid4()), tenant, aggregate, kind, self.config.get('kafka_topics', semantic, defaults[semantic]), aggregate, canonical_json(public(payload)), sql_time(datetime.now(timezone.utc))))

    def sweep(self, tenant_id: str, limit: int=500) -> dict[str, Any]:
        """Expire silent observed meters in a bounded batch using their cadence."""
        limit = max(1, min(int(limit), 5000))
        now = datetime.now(timezone.utc)
        with self.database.read() as connection:
            rows = connection.fetch_all("SELECT h.meter_id,h.last_seen_at,m.installation_status,m.expected_communication_minutes FROM meter_current_health h JOIN meters m ON m.tenant_id=h.tenant_id AND m.meter_id=h.meter_id WHERE h.tenant_id=%s AND h.current_status NOT IN ('OFFLINE','DECOMMISSIONED','UNKNOWN') AND h.last_seen_at IS NOT NULL AND TIMESTAMPDIFF(SECOND,h.last_seen_at,%s)>m.expected_communication_minutes*60*%s ORDER BY h.last_seen_at,h.meter_id LIMIT %s", (tenant_id, sql_time(now), self.config.get_float('health', 'cadence_grace_multiplier', 3), limit))
        changed = 0
        for row in rows:
            grace = self.config.get_float('health', 'cadence_grace_multiplier', 3)
            expired = row['last_seen_at'].replace(tzinfo=timezone.utc) + timedelta(minutes=row['expected_communication_minutes'] * grace)
            identity = str(uuid.uuid5(uuid.NAMESPACE_URL, tenant_id + ':' + row['meter_id'] + ':' + stamp(expired)))
            result = self.ingest({'schema_version': '1.0', 'event_id': identity, 'event_type': 'METER_SILENT', 'source_system': 'cadence_monitor', 'tenant_id': tenant_id, 'meter_id': row['meter_id'], 'event_time_utc': stamp(expired), 'received_at_utc': stamp(now), 'ingested_at_utc': stamp(now), 'correlation_id': identity, 'trace_id': identity, 'payload': {'last_seen_at': stamp(row['last_seen_at']), 'expected_communication_minutes': row['expected_communication_minutes'], 'grace_multiplier': grace}})
            if not result['duplicate'] and result['health']['current_status'] == 'OFFLINE':
                changed += 1
        return {'examined': len(rows), 'changed': changed, 'batch_limit': limit, 'may_have_more': len(rows) == limit}

    def _page(self, filters: dict[str, Any]) -> tuple[int, str]:
        try:
            limit = max(1, min(int(filters.get('limit', 50)), 200))
        except (ValueError, TypeError) as error:
            raise ValidationError('Pagination limit must be an integer') from error
        cursor = filters.get('cursor', '')
        if cursor:
            identifier(cursor, 'cursor')
        return (limit, cursor)

    def list_meters(self, tenant_id: str, filters: dict[str, Any]) -> dict[str, Any]:
        """Return bounded, keyset-paginated tenant meters."""
        limit, cursor = self._page(filters)
        clauses = ['m.tenant_id=%s', 'm.meter_id>%s']
        params = [tenant_id, cursor]
        if filters.get('status'):
            clauses.append('h.current_status=%s')
            params.append(filters['status'])
        if filters.get('region_id'):
            clauses.append('m.region_id=%s')
            params.append(filters['region_id'])
        if filters.get('gateway_id'):
            clauses.append('h.gateway_id=%s')
            params.append(filters['gateway_id'])
        if filters.get('q'):
            query = self._bounded(filters['q'], 128)
            clauses.append('m.meter_id LIKE %s')
            params.append('%' + query + '%')
        params.append(limit + 1)
        with self.database.read() as connection:
            rows = connection.fetch_all("SELECT m.meter_id,m.model,m.communication_type,m.installation_status,m.region_id,COALESCE(h.current_status,'UNKNOWN') AS current_status,h.last_seen_at,h.consecutive_failures,h.missing_interval_count,h.gateway_id FROM meters m LEFT JOIN meter_current_health h ON h.tenant_id=m.tenant_id AND h.meter_id=m.meter_id WHERE " + ' AND '.join(clauses) + ' ORDER BY m.meter_id LIMIT %s', tuple(params))
        return {'items': public(rows[:limit]), 'next_cursor': rows[limit - 1]['meter_id'] if len(rows) > limit else None}

    def meter_detail(self, tenant_id: str, meter_id: str) -> dict[str, Any]:
        """Return bounded detail, observed health, facts and historical topology."""
        with self.database.read() as connection:
            meter = connection.fetch_one('SELECT tenant_id,meter_id,manufacturer_id,model,firmware_version,communication_type,installation_status,region_id,expected_communication_minutes,installed_at FROM meters WHERE tenant_id=%s AND meter_id=%s', (tenant_id, meter_id))
            if not meter:
                raise NotFoundError('Requested meter does not exist')
            meter['health'] = connection.fetch_one('SELECT current_status,previous_status,state_entered_at,last_seen_at,consecutive_failures,missing_interval_count,late_interval_count,communication_score,state_confidence,delivery_status,gateway_id,updated_at FROM meter_current_health WHERE tenant_id=%s AND meter_id=%s', (tenant_id, meter_id)) or {'current_status': 'UNKNOWN'}
            meter['events'] = [decoded(row) for row in connection.fetch_all('SELECT source_event_id,event_type,event_time,received_at,source_system,error_code,evidence_json,is_late FROM communication_events WHERE tenant_id=%s AND meter_id=%s ORDER BY event_time DESC LIMIT 100', (tenant_id, meter_id))]
            meter['topology'] = connection.fetch_all('SELECT mapping_id,gateway_id,dcu_id,feeder_id,transformer_id,sim_provider_id,region_id,valid_from,valid_to,mapping_quality FROM meter_topology_history WHERE tenant_id=%s AND meter_id=%s ORDER BY valid_from DESC LIMIT 100', (tenant_id, meter_id))
            meter['predictions'] = [decoded(row) for row in connection.fetch_all('SELECT prediction_id,model_id,prediction_type,prediction_value,confidence_score,predicted_at,explanation_json FROM ml_predictions WHERE tenant_id=%s AND entity_id=%s ORDER BY predicted_at DESC LIMIT 20', (tenant_id, meter_id))]
        return public(meter)

    def overview(self, tenant_id: str) -> dict[str, Any]:
        """Expose computed fleet metrics without fabricated completeness or SLA."""
        with self.database.read() as connection:
            fleet = connection.fetch_one("SELECT COUNT(*) AS registered_meters,COALESCE(SUM(installation_status='ACTIVE'),0) AS active_meters FROM meters WHERE tenant_id=%s", (tenant_id,))
            statuses = connection.fetch_all('SELECT current_status AS status,COUNT(*) AS count FROM meter_current_health WHERE tenant_id=%s GROUP BY current_status', (tenant_id,))
            incidents = connection.fetch_one("SELECT COALESCE(SUM(status<>'RESOLVED'),0) AS open_incidents,COALESCE(SUM(status<>'RESOLVED' AND severity IN ('HIGH','CRITICAL')),0) AS high_risk_incidents FROM incidents WHERE tenant_id=%s", (tenant_id,))
            causes = connection.fetch_all("SELECT suspected_root_cause AS cause,COUNT(*) AS count FROM incidents WHERE tenant_id=%s AND status<>'RESOLVED' GROUP BY suspected_root_cause ORDER BY count DESC LIMIT 12", (tenant_id,))
            timing = connection.fetch_one('SELECT MAX(ingested_at) AS updated_at,AVG(TIMESTAMPDIFF(SECOND,event_time,ingested_at)) AS delay,MAX(source_system LIKE %s) AS synthetic FROM communication_events WHERE tenant_id=%s AND event_time>=%s', ('synthetic%', tenant_id, sql_time(datetime.now(timezone.utc) - timedelta(days=1))))
        counts = {row['status']: int(row['count']) for row in statuses}
        metrics = {**fleet, **incidents, 'healthy_meters': counts.get('HEALTHY', 0), 'offline_meters': counts.get('OFFLINE', 0), 'degraded_meters': counts.get('DEGRADED', 0) + counts.get('DELAYED', 0), 'reporting_completeness': None, 'sla_risk': None, 'ingestion_delay_seconds': float(timing['delay']) if timing['delay'] is not None else None}
        return public({'data_mode': 'empty' if not fleet['registered_meters'] else 'synthetic' if timing['synthetic'] else 'live', 'updated_at': timing['updated_at'], 'metrics': metrics, 'status_distribution': statuses, 'root_causes': causes})

    def analytics(self, tenant_id: str, kind: str, filters: dict[str, Any]) -> dict[str, Any]:
        """Query bounded aggregate windows or explicitly approved SLA data."""
        now = datetime.now(timezone.utc)
        start = utc(filters['start']) if filters.get('start') else now - timedelta(days=1)
        end = utc(filters['end']) if filters.get('end') else now
        if end <= start or end - start > timedelta(days=93):
            raise ValidationError('Analytics requires a positive window of at most93 days')
        with self.database.read() as connection:
            if kind == 'communication':
                rows = connection.fetch_all('SELECT hour_start,SUM(on_time_reads) AS success_count,SUM(failure_count) AS failure_count,SUM(late_reads) AS late_count FROM meter_health_hourly WHERE tenant_id=%s AND hour_start>=%s AND hour_start<%s GROUP BY hour_start ORDER BY hour_start LIMIT 2232', (tenant_id, sql_time(start), sql_time(end)))
                return {'series': public(rows), 'summary': {'observed_hours': len(rows)}}
            if kind == 'data-quality':
                rows = connection.fetch_all('SELECT delivery_status,COUNT(*) AS count FROM interval_delivery_status WHERE tenant_id=%s AND interval_start>=%s AND interval_start<%s GROUP BY delivery_status', (tenant_id, sql_time(start), sql_time(end)))
                return {'items': rows, 'estimated_is_physical_receipt': False}
            if kind == 'regions':
                rows = connection.fetch_all("SELECT m.region_id,COALESCE(r.display_name,m.region_id) AS display_name,COUNT(*) AS meter_count,COALESCE(SUM(h.current_status='OFFLINE'),0) AS offline_count FROM meters m LEFT JOIN regions r ON r.tenant_id=m.tenant_id AND r.region_id=m.region_id LEFT JOIN meter_current_health h ON h.tenant_id=m.tenant_id AND h.meter_id=m.meter_id WHERE m.tenant_id=%s GROUP BY m.region_id,r.display_name LIMIT 200", (tenant_id,))
                return {'items': public(rows)}
            if kind == 'gateways':
                rows = connection.fetch_all('SELECT gateway_id,window_start,window_end,active_meter_count,offline_meter_count,failure_rate,network_availability FROM gateway_health_snapshots WHERE tenant_id=%s AND window_start>=%s AND window_start<%s ORDER BY window_start DESC LIMIT 200', (tenant_id, sql_time(start), sql_time(end)))
                return {'items': public(rows)}
            if kind == 'sla':
                contract = decoded(connection.fetch_one("SELECT contract_id,approval_status,definition_json FROM sla_contracts WHERE tenant_id=%s AND approval_status='APPROVED' ORDER BY approved_at DESC LIMIT 1", (tenant_id,)))
                if not contract:
                    return evaluate_sla(None, [])
                rows = connection.fetch_all('SELECT meter_id,interval_start,mdms_received_at,quality_status,estimated_flag FROM interval_delivery_status WHERE tenant_id=%s AND interval_start>=%s AND interval_start<%s LIMIT 10001', (tenant_id, sql_time(start), sql_time(end)))
                if len(rows) > 10000:
                    raise ValidationError('SLA window exceeds pilot evaluation bound; use a narrower window')
                return evaluate_sla({**contract['definition_json'], 'contract_id': contract['contract_id'], 'approval_status': 'APPROVED'}, rows)
        raise ValidationError('Unknown analytics kind')

    def list_incidents(self, tenant_id: str, filters: dict[str, Any]) -> dict[str, Any]:
        """Return tenant incidents using bounded keyset pagination."""
        limit, cursor = self._page(filters)
        clauses = ['tenant_id=%s', 'incident_id>%s']
        params = [tenant_id, cursor]
        if filters.get('status'):
            clauses.append('status=%s')
            params.append(filters['status'])
        if filters.get('assigned_to') is not None:
            try:
                assigned = int(filters['assigned_to'])
            except (ValueError, TypeError) as error:
                raise ValidationError('Invalid incident assignee filter') from error
            clauses.append('assigned_to=%s')
            params.append(assigned)
        for name in ('severity', 'region_id', 'gateway_id'):
            if filters.get(name):
                clauses.append(name + '=%s')
                params.append(self._bounded(filters[name], 64))
        if filters.get('q'):
            query = self._bounded(filters['q'], 128)
            clauses.append('(incident_id LIKE %s OR suspected_root_cause LIKE %s)')
            params.extend(['%' + query + '%'] * 2)
        params.append(limit + 1)
        with self.database.read() as connection:
            rows = connection.fetch_all('SELECT incident_id,incident_type,severity,status,suspected_root_cause,root_cause_confidence,affected_meter_count,gateway_id,region_id,detected_at,first_event_at,assigned_to FROM incidents WHERE ' + ' AND '.join(clauses) + ' ORDER BY incident_id LIMIT %s', tuple(params))
        return {'items': public(rows[:limit]), 'next_cursor': rows[limit - 1]['incident_id'] if len(rows) > limit else None}

    def incident_detail(self, tenant_id: str, incident_id: str) -> dict[str, Any]:
        """Return evidence and immutable history with bounded child collections."""
        with self.database.read() as connection:
            row = connection.fetch_one('SELECT incident_id,incident_type,severity,status,suspected_root_cause,root_cause_confidence,affected_meter_count,gateway_id,region_id,detected_at,first_event_at,acknowledged_at,resolved_at,assigned_to,correlation_group_id FROM incidents WHERE tenant_id=%s AND incident_id=%s', (tenant_id, incident_id))
            if not row:
                raise NotFoundError('Requested incident does not exist')
            for name, table, fields in (('evidence', 'incident_evidence', 'evidence_type,source_system,source_reference,event_time,evidence_json'), ('history', 'incident_history', 'change_type,before_state,after_state,actor_user_id,created_at'), ('actions', 'incident_actions', 'action_id,action_type,action_status,executed_by,result_json,created_at')):
                row[name] = [decoded(item) for item in connection.fetch_all(f'SELECT {fields} FROM {table} WHERE tenant_id=%s AND incident_id=%s ORDER BY created_at DESC LIMIT 200', (tenant_id, incident_id))]
        return public(row)

    def incident_action(self, principal: Any, incident_id: str, action: str, body: dict[str, Any], correlation_id: str) -> dict[str, Any]:
        """Apply permission-checked, audited incident transitions transactionally."""
        permission = f'incidents.{action}'
        if permission not in principal['permissions']:
            raise AuthorizationError('Permission denied')
        if action not in {'acknowledge', 'assign', 'resolve', 'feedback'}:
            raise ValidationError('Unknown incident action')
        tenant = principal['tenant_id']
        now = sql_time(datetime.now(timezone.utc))
        with self.database.transaction() as connection:
            old = connection.fetch_one('SELECT incident_id,status,assigned_to,suspected_root_cause,severity FROM incidents WHERE tenant_id=%s AND incident_id=%s FOR UPDATE', (tenant, incident_id))
            if not old:
                raise NotFoundError('Requested incident does not exist')
            if principal.get('incident_scope') == 'assigned' and old['assigned_to'] != principal['user_id']:
                raise AuthorizationError('Incident is outside the assigned scope')
            after = dict(old)
            if action == 'acknowledge':
                if old['status'] not in {'OPEN', 'ACKNOWLEDGED'}:
                    raise ConflictError('Only open incidents can be acknowledged')
                after['status'] = 'ACKNOWLEDGED'
                connection.execute("UPDATE incidents SET status='ACKNOWLEDGED',acknowledged_at=COALESCE(acknowledged_at,%s),updated_at=%s WHERE tenant_id=%s AND incident_id=%s", (now, now, tenant, incident_id))
            elif action == 'assign':
                target = body.get('assigned_to')
                if type(target) is not int or not connection.fetch_one("SELECT id FROM users WHERE tenant_id=%s AND id=%s AND status='ACTIVE'", (tenant, target)):
                    raise ValidationError('Assignee must be an active user in this tenant')
                after['assigned_to'] = target
                connection.execute('UPDATE incidents SET assigned_to=%s,updated_at=%s WHERE tenant_id=%s AND incident_id=%s', (target, now, tenant, incident_id))
            elif action == 'resolve':
                reason = self._bounded(body.get('resolution'), 2000)
                if not reason:
                    raise ValidationError('Resolution explanation is required')
                after['status'] = 'RESOLVED'
                connection.execute("UPDATE incidents SET status='RESOLVED',resolved_at=COALESCE(resolved_at,%s),updated_at=%s WHERE tenant_id=%s AND incident_id=%s", (now, now, tenant, incident_id))
            elif action == 'feedback':
                cause = body.get('confirmed_cause')
                if cause not in {'GATEWAY_FAILURE', 'DCU_FAILURE', 'NETWORK_PROVIDER_ISSUE', 'LOW_SIGNAL_QUALITY', 'POWER_INTERRUPTION', 'HES_PROCESSING_DELAY', 'HES_MDMS_INTEGRATION_DELAY', 'MDMS_VALIDATION_FAILURE', 'METER_FIRMWARE_ISSUE', 'METER_SPECIFIC_FAILURE', 'TOPOLOGY_MAPPING_ERROR', 'UNKNOWN'}:
                    raise ValidationError('Unknown confirmed cause')
                after['suspected_root_cause'] = cause
                connection.execute('UPDATE incidents SET suspected_root_cause=%s,updated_at=%s WHERE tenant_id=%s AND incident_id=%s', (cause, now, tenant, incident_id))
            connection.execute('INSERT INTO incident_history(tenant_id,incident_id,actor_user_id,change_type,before_state,after_state,correlation_id,created_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', (tenant, incident_id, principal['user_id'], action.upper(), canonical_json(old), canonical_json(after), correlation_id, now))
            connection.execute("INSERT INTO incident_actions(action_id,tenant_id,incident_id,action_type,action_status,executed_by,result_json,created_at,completed_at) VALUES(%s,%s,%s,%s,'COMPLETED',%s,%s,%s,%s)", (str(uuid.uuid4()), tenant, incident_id, action.upper(), principal['user_id'], canonical_json(body), now, now))
            self.audit.append(connection, tenant, principal['user_id'], 'incident.' + action, 'incident', incident_id, correlation_id, before=old, after=after, session_id=principal['session_id'])
            self._outbox(connection, tenant, incident_id, 'INCIDENT_RESOLVED' if action == 'resolve' else 'INCIDENT_UPDATED', 'incidents_resolved' if action == 'resolve' else 'incidents_updated', {'tenant_id': tenant, 'incident_id': incident_id, 'correlation_id': correlation_id, **after})
        return after

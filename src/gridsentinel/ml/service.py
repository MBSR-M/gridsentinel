"""Audited temporal training, model approval and atomic activation/rollback."""
from __future__ import annotations
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any
import uuid
from gridsentinel.core.exceptions import AuthorizationError, ConflictError, NotFoundError, ValidationError
from gridsentinel.domain.events import canonical_json
from gridsentinel.ml.algorithms import HistoricalBaseline, LogisticModel, classification_metrics, load_artifact, population_stability
from gridsentinel.ml.datasets import FEATURES, RecoveryEpisode, build_labels, temporal_split
from gridsentinel.processing.operations import decoded, public, sql_time

class ModelService:
    """Keep learned artifacts tenant-scoped and inactive until human approval."""

    def __init__(self, database: Any, config: Any, audit: Any) -> None:
        self.database, self.config, self.audit = (database, config, audit)

    def list_models(self, tenant_id: str) -> dict[str, Any]:
        """Return bounded registry metadata without executable artifact content."""
        with self.database.read() as connection:
            rows = connection.fetch_all('SELECT model_id,model_name,model_version,algorithm,feature_schema_version,metrics_json,approval_status,is_active,created_at,approved_at,activated_at FROM ml_model_registry WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 200', (tenant_id,))
        return {'items': public([decoded(row) for row in rows]), 'next_cursor': None}

    def training_runs(self, tenant_id: str) -> dict[str, Any]:
        """Return the latest bounded training outcomes and dataset boundaries."""
        with self.database.read() as connection:
            rows = connection.fetch_all('SELECT training_run_id,model_name,dataset_start,dataset_end,validation_start,validation_end,feature_schema_version,sample_count,status,started_at,completed_at,metrics_json FROM ml_training_runs WHERE tenant_id=%s ORDER BY started_at DESC LIMIT 100', (tenant_id,))
        return {'items': public([decoded(row) for row in rows])}

    def train(self, tenant_id: str, body: dict[str, Any], actor: Any, correlation_id: str) -> dict[str, Any]:
        """Train a bounded supplied point-in-time dataset with purged temporal holdouts."""
        raw = body.get('episodes')
        maximum = self.config.get_int('ml', 'max_training_samples', 20000)
        if not isinstance(raw, list) or not 1 <= len(raw) <= maximum:
            raise ValidationError('A bounded episode dataset is required')
        run_id = body.get('training_run_id') or str(uuid.uuid4())
        try:
            uuid.UUID(run_id)
        except (ValueError, TypeError, AttributeError) as error:
            raise ValidationError('training_run_id must be a UUID') from error
        with self.database.read() as connection:
            existing = connection.fetch_one('SELECT model_id,model_version FROM ml_model_registry WHERE tenant_id=%s AND training_run_id=%s', (tenant_id, run_id))
            if existing:
                return {**existing, 'training_run_id': run_id, 'duplicate': True}
        episodes = [RecoveryEpisode.from_dict(row) for row in raw]
        try:
            horizon = int(body.get('horizon_hours', self.config.get_int('ml', 'recovery_horizon_hours', 12)))
        except (ValueError,TypeError) as error:
            raise ValidationError('Recovery horizon must be an integer') from error
        labelled, censored = build_labels(episodes, horizon)
        if len(labelled) < self.config.get_int('ml', 'minimum_training_samples', 100):
            raise ValidationError('Insufficient fully observed training samples')
        train, validation, test = temporal_split(labelled)
        validation_start = validation[0][0].as_of
        test_start = test[0][0].as_of
        from datetime import timedelta
        available = lambda pair: pair[0].recovered_at if pair[1] else pair[0].as_of + timedelta(hours=horizon)
        train = [pair for pair in train if available(pair) <= validation_start]
        validation = [pair for pair in validation if available(pair) <= test_start]
        if not train or not validation:
            raise ValidationError('Purged temporal cohorts lack observed labels')
        baseline = HistoricalBaseline.fit(train)
        model = LogisticModel.fit(train, self.config.get_int('ml', 'epochs', 150), self.config.get_float('ml', 'learning_rate', 0.05), self.config.get_float('ml', 'l2', 0.001), self.config.get_int('ml', 'seed', 42))

        def evaluate(rows: list[Any], algorithm: str) -> dict[str, Any]:
            return classification_metrics([y for _, y in rows], [model.predict(row.features) if algorithm == 'logistic' else baseline.predict(row.stratum) for row, _ in rows])
        metrics = {'validation': evaluate(validation, 'logistic'), 'test': evaluate(test, 'logistic'), 'baseline_validation': evaluate(validation, 'baseline'), 'baseline_test': evaluate(test, 'baseline'), 'censored_excluded': censored, 'purged_samples': len(labelled) - len(train) - len(validation) - len(test), 'cohorts': {'train': len(train), 'validation': len(validation), 'test': len(test)}, 'target': 'OBSERVED_COMMUNICATION_RECOVERY', 'synthetic': body.get('data_source') == 'synthetic', 'group_test': {key: classification_metrics([y for row, y in test if row.stratum == key], [model.predict(row.features) for row, _ in test if row.stratum == key]) for key in sorted(set((row.stratum for row, _ in test)))}}
        now = datetime.now(timezone.utc)
        model_id = str(uuid.uuid4())
        version = now.strftime('%Y%m%dT%H%M%S') + '-' + model_id[:8]
        data_source = body.get('data_source')
        if not isinstance(data_source, str) or not 1 <= len(data_source) <= 128:
            raise ValidationError('Training data source provenance is required')
        artifact = {'artifact_format_version': '1.0', 'model_version': version, 'feature_schema_version': 'recovery-1.0', 'features': list(FEATURES), 'parameters': model.artifact(), 'baseline': baseline.artifact(), 'horizon_hours': horizon, 'target': 'OBSERVED_COMMUNICATION_RECOVERY', 'data_source': data_source, 'label_definition': 'Recovered by horizon; ongoing episodes without full follow-up excluded; intervention status retained, never claim spontaneous recovery', 'metrics': metrics, 'trained_at': public(now), 'training_seed': self.config.get_int('ml', 'seed', 42), 'feature_reference': {name: [row.features[i] for row, _ in train[:500]] for i, name in enumerate(FEATURES)}}
        serialized = canonical_json(artifact)
        checksum = hashlib.sha256(serialized.encode()).hexdigest()
        load_artifact(artifact, checksum)
        user_id = actor.get('user_id', actor.get('id')) if isinstance(actor, dict) else getattr(actor, 'user_id', actor)
        snapshot = {'epochs': self.config.get_int('ml', 'epochs', 150), 'learning_rate': self.config.get_float('ml', 'learning_rate', 0.05), 'l2': self.config.get_float('ml', 'l2', 0.001), 'horizon_hours': horizon, 'data_source': data_source, 'source_schema_version': '1.0', 'split': 'temporal60/20/20 with label-availability purge'}
        with self.database.transaction() as connection:
            connection.fetch_one('SELECT tenant_id FROM tenants WHERE tenant_id=%s FOR UPDATE', (tenant_id,))
            existing = connection.fetch_one('SELECT model_id,model_version FROM ml_model_registry WHERE tenant_id=%s AND training_run_id=%s', (tenant_id, run_id))
            if existing:
                return {**existing, 'training_run_id': run_id, 'duplicate': True}
            connection.execute("INSERT INTO ml_training_runs(training_run_id,tenant_id,model_name,dataset_start,dataset_end,validation_start,validation_end,feature_schema_version,sample_count,status,started_at,completed_at,metrics_json,configuration_snapshot,artifact_reference,model_owner) VALUES(%s,%s,'recovery_probability',%s,%s,%s,%s,'recovery-1.0',%s,'COMPLETED',%s,%s,%s,%s,%s,%s)", (run_id, tenant_id, sql_time(train[0][0].as_of), sql_time(test[-1][0].observed_until), sql_time(validation[0][0].as_of), sql_time(validation[-1][0].observed_until), len(labelled), sql_time(now), sql_time(now), canonical_json(metrics), canonical_json(snapshot), 'registry:' + model_id, user_id))
            connection.execute("INSERT INTO ml_model_registry(model_id,tenant_id,model_name,model_version,algorithm,feature_schema_version,artifact_reference,artifact_json,artifact_checksum,metrics_json,approval_status,created_at,training_run_id) VALUES(%s,%s,'recovery_probability',%s,'logistic_sgd','recovery-1.0',%s,%s,%s,%s,'PENDING',%s,%s)", (model_id, tenant_id, version, 'registry:' + model_id, serialized, checksum, canonical_json(metrics), sql_time(now), run_id))
            persisted = connection.fetch_one('SELECT artifact_json FROM ml_model_registry WHERE tenant_id=%s AND model_id=%s', (tenant_id, model_id))
            stored = json.loads(persisted['artifact_json']) if isinstance(persisted['artifact_json'], str) else persisted['artifact_json']
            checksum = hashlib.sha256(canonical_json(stored).encode()).hexdigest()
            load_artifact(stored, checksum)
            connection.execute('UPDATE ml_model_registry SET artifact_checksum=%s WHERE tenant_id=%s AND model_id=%s', (checksum, tenant_id, model_id))
            self.audit.append(connection, tenant_id, user_id, 'model.train', 'model', model_id, correlation_id, after={'version': version, 'checksum': checksum, 'metrics': metrics})
        return {'model_id': model_id, 'model_version': version, 'training_run_id': run_id, 'approval_status': 'PENDING', 'metrics': metrics, 'duplicate': False}

    def model_action(self, principal: Any, model_id: str, action: str, body: dict[str, Any], correlation_id: str) -> dict[str, Any]:
        """Approve, activate or roll back valid artifacts with atomic deployment."""
        permission = 'models.activate' if action == 'rollback' else 'models.' + action
        if permission not in principal['permissions']:
            raise AuthorizationError('Permission denied')
        if action not in {'approve', 'activate', 'rollback'}:
            raise ValidationError('Unknown model action')
        tenant = principal['tenant_id']
        now = sql_time(datetime.now(timezone.utc))
        with self.database.transaction() as connection:
            connection.fetch_one('SELECT tenant_id FROM tenants WHERE tenant_id=%s FOR UPDATE', (tenant,))
            model = connection.fetch_one('SELECT model_id,model_name,approval_status,artifact_json,artifact_checksum,is_active FROM ml_model_registry WHERE tenant_id=%s AND model_id=%s FOR UPDATE', (tenant, model_id))
            if not model:
                raise NotFoundError('Requested model does not exist')
            artifact = load_artifact(model['artifact_json'], model['artifact_checksum'])
            if action == 'approve':
                reason = body.get('reason')
                if not isinstance(reason, str) or not 1 <= len(reason) <= 2000:
                    raise ValidationError('Model approval rationale is required')
                connection.execute("UPDATE ml_model_registry SET approval_status='APPROVED',approved_by=%s,approved_at=%s WHERE tenant_id=%s AND model_id=%s", (principal['user_id'], now, tenant, model_id))
            else:
                if model['approval_status'] != 'APPROVED':
                    raise ConflictError('Only an approved model can be activated')
                connection.execute('INSERT IGNORE INTO ml_model_deployments(tenant_id,model_name,updated_at) VALUES(%s,%s,%s)', (tenant, model['model_name'], now))
                deployment = connection.fetch_one('SELECT active_model_id,previous_model_id FROM ml_model_deployments WHERE tenant_id=%s AND model_name=%s FOR UPDATE', (tenant, model['model_name']))
                if action == 'rollback' and deployment['previous_model_id'] != model_id:
                    raise ConflictError('Requested model is not the previous deployment')
                connection.execute('UPDATE ml_model_registry SET is_active=FALSE WHERE tenant_id=%s AND model_name=%s AND is_active=TRUE', (tenant, model['model_name']))
                connection.execute('UPDATE ml_model_registry SET is_active=TRUE,activated_at=%s WHERE tenant_id=%s AND model_id=%s', (now, tenant, model_id))
                connection.execute('UPDATE ml_model_deployments SET previous_model_id=active_model_id,active_model_id=%s,updated_at=%s WHERE tenant_id=%s AND model_name=%s', (model_id, now, tenant, model['model_name']))
            self.audit.append(connection, tenant, principal['user_id'], 'model.' + action, 'model', model_id, correlation_id, before={'approval_status': model['approval_status'], 'is_active': bool(model['is_active'])}, after={'action': action, 'model_version': artifact['model_version']}, metadata=body, session_id=principal['session_id'])
        return {'model_id': model_id, 'action': action, 'approval_status': 'APPROVED', 'is_active': action != 'approve' or bool(model['is_active'])}

    def predictions(self, tenant_id: str, meter_id: str) -> dict[str, Any]:
        """Return bounded stored predictions for a tenant meter."""
        with self.database.read() as connection:
            rows = connection.fetch_all('SELECT prediction_id,model_id,prediction_type,prediction_value,confidence_score,predicted_at,explanation_json FROM ml_predictions WHERE tenant_id=%s AND entity_id=%s ORDER BY predicted_at DESC LIMIT 100', (tenant_id, meter_id))
        return {'items': public([decoded(row) for row in rows])}

    def infer_current(self, tenant_id: str, meter_id: str) -> dict[str, Any]:
        """Generate features only from source facts observable at the current time."""
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        with self.database.read() as connection:
            health = connection.fetch_one('SELECT h.current_status,h.last_seen_at,h.state_entered_at,h.gateway_id,m.communication_type FROM meter_current_health h JOIN meters m ON m.tenant_id=h.tenant_id AND m.meter_id=h.meter_id WHERE h.tenant_id=%s AND h.meter_id=%s', (tenant_id, meter_id))
            if not health or health['current_status'] not in {'OFFLINE', 'DEGRADED', 'RECOVERING'} or (not health['state_entered_at']):
                return {'available': False, 'method': 'DETERMINISTIC_RULES', 'probability': None, 'reason': 'No observed current outage'}
            count = connection.fetch_one("SELECT COUNT(*) AS count FROM communication_events WHERE tenant_id=%s AND meter_id=%s AND event_time>=%s AND event_time<=%s AND event_type='METER_COMM_FAILURE'", (tenant_id, meter_id, sql_time(now - timedelta(days=1)), sql_time(now)))
            recoveries = connection.fetch_one("SELECT COUNT(*) AS count FROM communication_events e WHERE e.tenant_id=%s AND e.meter_id=%s AND e.event_type='METER_COMM_SUCCESS' AND e.event_time>=%s AND e.event_time<=%s AND (SELECT prior.event_type FROM communication_events prior WHERE prior.tenant_id=e.tenant_id AND prior.meter_id=e.meter_id AND prior.event_time<e.event_time ORDER BY prior.event_time DESC LIMIT 1) IN ('METER_COMM_FAILURE','POWER_FAILURE')", (tenant_id, meter_id, sql_time(now - timedelta(days=30)), sql_time(now)))
            snapshot = connection.fetch_one('SELECT failure_rate,offline_meter_count FROM gateway_health_snapshots WHERE tenant_id=%s AND gateway_id=%s AND window_start<=%s ORDER BY window_start DESC LIMIT 1', (tenant_id, health['gateway_id'], sql_time(now)))
            power = connection.fetch_one("SELECT event_type FROM communication_events WHERE tenant_id=%s AND meter_id=%s AND event_type IN ('POWER_FAILURE','POWER_RESTORED') AND event_time>=%s AND event_time<=%s ORDER BY event_time DESC LIMIT 1", (tenant_id, meter_id, sql_time(now - timedelta(days=1)), sql_time(now)))
            recent = connection.fetch_one('SELECT prediction_id FROM ml_predictions WHERE tenant_id=%s AND entity_id=%s AND predicted_at>=%s ORDER BY predicted_at DESC LIMIT 1', (tenant_id, meter_id, sql_time(now - timedelta(minutes=self.config.get_int('ml', 'prediction_interval_minutes', 60)))))
        if recent:
            return {'available': False, 'reason': 'Current prediction interval already scored', 'prediction_id': recent['prediction_id']}
        if not snapshot or snapshot['failure_rate'] is None:
            return {'available': False, 'method': 'DETERMINISTIC_RULES', 'probability': None, 'reason': 'Historical gateway failure population is unknown'}
        if not power:
            return {'available': False, 'method': 'DETERMINISTIC_RULES', 'probability': None, 'reason': 'Power alarm observation is unknown'}
        entered = health['state_entered_at'].replace(tzinfo=timezone.utc)
        features = {'outage_duration_hours': max(0, (now - entered).total_seconds() / 3600), 'failures_last_24h': int(count['count']), 'recoveries_last_30d': int(recoveries['count']), 'gateway_failure_ratio': float(snapshot['failure_rate']), 'shared_outage_size': int(snapshot['offline_meter_count']), 'power_alarm_present': 1 if power['event_type'] == 'POWER_FAILURE' else 0, 'hour_of_day': now.hour}
        return self.infer(tenant_id, meter_id, features, health['communication_type'] or 'UNKNOWN')

    def infer(self, tenant_id: str, meter_id: str, features: dict[str, float], stratum: str='UNKNOWN') -> dict[str, Any]:
        """Score the approved active model; invalid artifacts safely return rules."""
        if set(features) != set(FEATURES):
            raise ValidationError('Inference feature schema mismatch')
        if any(type(value) not in (int,float) or not math.isfinite(value) or value<0 or value>1e9 for value in features.values()):
            raise ValidationError('Inference features must be bounded finite numbers')
        if not 0<=features['gateway_failure_ratio']<=1 or features['power_alarm_present'] not in (0,1) or not 0<=features['hour_of_day']<=23:
            raise ValidationError('Inference feature range violated')
        values = tuple((float(features[name]) for name in FEATURES))
        with self.database.transaction() as connection:
            if not connection.fetch_one('SELECT meter_id FROM meters WHERE tenant_id=%s AND meter_id=%s',(tenant_id,meter_id)):
                raise NotFoundError('Requested meter does not exist')
            model = connection.fetch_one("SELECT r.model_id,r.artifact_json,r.artifact_checksum FROM ml_model_deployments d JOIN ml_model_registry r ON r.tenant_id=d.tenant_id AND r.model_id=d.active_model_id WHERE d.tenant_id=%s AND d.model_name='recovery_probability' AND r.approval_status='APPROVED'", (tenant_id,))
            if not model:
                return {'available': False, 'method': 'DETERMINISTIC_RULES', 'probability': None, 'reason': 'No approved active model'}
            try:
                artifact = load_artifact(model['artifact_json'], model['artifact_checksum'])
            except ValidationError:
                return {'available': False, 'method': 'DETERMINISTIC_RULES', 'probability': None, 'reason': 'Model artifact validation failed'}
            parameters = artifact['parameters']
            logistic = LogisticModel(tuple(parameters['coefficients']), parameters['intercept'], tuple(parameters['means']), tuple(parameters['scales']))
            use_baseline = artifact['metrics']['validation']['brier_score'] > artifact['metrics']['baseline_validation']['brier_score']
            probability = artifact['baseline']['rates'].get(stratum, artifact['baseline']['global_rate']) if use_baseline else logistic.predict(values)
            result = {'available': True, 'method': 'HISTORICAL_BASELINE' if use_baseline else 'LOGISTIC_SGD', 'probability': probability, 'horizon_hours': artifact['horizon_hours'], 'target': 'OBSERVED_COMMUNICATION_RECOVERY', 'synthetic_training': artifact['data_source'] == 'synthetic', 'contributions': {name: weight * (value - mean) / scale for name, weight, value, mean, scale in zip(FEATURES, logistic.coefficients, values, logistic.means, logistic.scales)}}
            prediction_id = str(uuid.uuid4())
            connection.execute("INSERT INTO ml_predictions(prediction_id,tenant_id,model_id,entity_id,entity_type,prediction_type,prediction_value,predicted_at,feature_snapshot,explanation_json) VALUES(%s,%s,%s,%s,'METER','RECOVERY_PROBABILITY',%s,%s,%s,%s)", (prediction_id, tenant_id, model['model_id'], meter_id, probability, sql_time(datetime.now(timezone.utc)), canonical_json(features), canonical_json(result)))
            return {'prediction_id': prediction_id, 'model_id': model['model_id'], **result}

    def drift(self, tenant_id: str) -> dict[str, Any]:
        """Return persisted drift observations for approved model monitoring."""
        with self.database.read() as connection:
            rows = connection.fetch_all('SELECT drift_id,model_id,observation_start,observation_end,metrics_json,alert_threshold,computed_at FROM ml_drift_metrics WHERE tenant_id=%s ORDER BY computed_at DESC LIMIT 100', (tenant_id,))
        return {'items': public([decoded(row) for row in rows])}

    def monitor_drift(self, tenant_id: str, model_id: str, observations: dict[str, list[float]], start: datetime, end: datetime) -> dict[str, Any]:
        """Persist feature PSI using the frozen training reference distributions."""
        if end <= start or set(observations) != set(FEATURES) or any((len(values) > 10000 for values in observations.values())):
            raise ValidationError('Invalid bounded drift observation window')
        with self.database.transaction() as connection:
            model = connection.fetch_one('SELECT artifact_json,artifact_checksum FROM ml_model_registry WHERE tenant_id=%s AND model_id=%s', (tenant_id, model_id))
            if not model:
                raise NotFoundError('Requested model does not exist')
            artifact = load_artifact(model['artifact_json'], model['artifact_checksum'])
            metrics = {name: population_stability(artifact['feature_reference'][name], values) for name, values in observations.items()}
            threshold = self.config.get_float('ml', 'drift_psi_threshold', 0.2)
            connection.execute('INSERT INTO ml_drift_metrics(drift_id,tenant_id,model_id,observation_start,observation_end,metrics_json,alert_threshold,computed_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)', (str(uuid.uuid4()), tenant_id, model_id, sql_time(start), sql_time(end), canonical_json(metrics), threshold, sql_time(datetime.now(timezone.utc))))
        return {'feature_psi': metrics, 'threshold': threshold, 'alert': any((value > threshold for value in metrics.values()))}

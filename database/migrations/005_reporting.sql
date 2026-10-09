-- Safe empty reporting views. Trusted provision_reporting replaces __TENANT__.
-- Reporter grants are added only by the explicit tenant-binding provisioning step.
CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_fleet_health AS
SELECT COALESCE(h.current_status,'UNKNOWN') AS health_status,
       COUNT(*) AS meter_count,
       SUM(m.installation_status='ACTIVE') AS active_meter_count,
       AVG(h.communication_score) AS communication_score,
       MAX(h.updated_at) AS observed_at
FROM meters m LEFT JOIN meter_current_health h ON h.tenant_id=m.tenant_id AND h.meter_id=m.meter_id
WHERE m.tenant_id='__TENANT__'
GROUP BY COALESCE(h.current_status,'UNKNOWN');

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_region_health AS
SELECT COALESCE(r.display_name,m.region_id,'UNMAPPED') AS region_name,
       COALESCE(h.current_status,'UNKNOWN') AS health_status,
       COUNT(*) AS meter_count,AVG(h.communication_score) AS communication_score,
       MAX(h.updated_at) AS observed_at
FROM meters m LEFT JOIN regions r ON r.tenant_id=m.tenant_id AND r.region_id=m.region_id
LEFT JOIN meter_current_health h ON h.tenant_id=m.tenant_id AND h.meter_id=m.meter_id
WHERE m.tenant_id='__TENANT__'
GROUP BY COALESCE(r.display_name,m.region_id,'UNMAPPED'),COALESCE(h.current_status,'UNKNOWN');

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_delivery_latency AS
SELECT TIMESTAMP(DATE(interval_end),MAKETIME(HOUR(interval_end),0,0)) AS observed_at,
       source_system,COUNT(*) AS observed_intervals,
       SUM(estimated_flag) AS estimated_intervals,
       SUM(delivery_status='MISSING') AS observed_missing_intervals,
       AVG(CASE WHEN estimated_flag=FALSE AND hes_received_at IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND,interval_end,hes_received_at) END) AS mean_hes_latency_seconds,
       AVG(CASE WHEN estimated_flag=FALSE AND mdms_received_at IS NOT NULL
           THEN TIMESTAMPDIFF(SECOND,interval_end,mdms_received_at) END) AS mean_mdms_latency_seconds
FROM interval_delivery_status WHERE tenant_id='__TENANT__'
GROUP BY TIMESTAMP(DATE(interval_end),MAKETIME(HOUR(interval_end),0,0)),source_system;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_ingestion_operations AS
SELECT CONCAT('connector:',connector_id) AS component,source_shard AS stream,
       updated_at AS observed_at,last_successful_extract AS last_handoff_at,
       watermark,NULL AS pending_events,NULL AS published_events
FROM connector_checkpoints WHERE tenant_id='__TENANT__'
UNION ALL
SELECT 'outbox' AS component,topic AS stream,MAX(created_at) AS observed_at,
       MAX(published_at) AS last_handoff_at,NULL AS watermark,
       SUM(published_at IS NULL) AS pending_events,SUM(published_at IS NOT NULL) AS published_events
FROM outbox_events WHERE tenant_id='__TENANT__' GROUP BY topic;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_queue_health AS
SELECT queue,status,COUNT(*) AS durable_jobs,MIN(created_at) AS oldest_created_at,
       MAX(updated_at) AS observed_at,MAX(attempt_number) AS max_attempt_number,
       SUM(status='FAILED') AS exhausted_jobs,
       SUM(status='RUNNING' AND locked_until<UTC_TIMESTAMP(6)) AS expired_leases
FROM background_jobs WHERE tenant_id='__TENANT__' GROUP BY queue,status;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_platform_health AS
SELECT service_name,instance_id,status,updated_at AS observed_at,
       TIMESTAMPDIFF(SECOND,updated_at,UTC_TIMESTAMP(6)) AS heartbeat_age_seconds,
       CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(metrics_json,'$.processed')),'null') AS UNSIGNED) AS processed_events,
       CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(metrics_json,'$.invalid')),'null') AS UNSIGNED) AS invalid_events,
       CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(metrics_json,'$.lag')),'null') AS UNSIGNED) AS observed_kafka_lag,
       CAST(NULLIF(JSON_UNQUOTE(JSON_EXTRACT(metrics_json,'$.dropped_debug_logs')),'null') AS UNSIGNED) AS dropped_debug_logs
FROM service_heartbeats WHERE tenant_id='__TENANT__';

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_error_trends AS
SELECT TIMESTAMP(DATE(event_time),MAKETIME(HOUR(event_time),0,0)) AS observed_at,
       service_name,level,exception_type,COUNT(*) AS error_count
FROM application_logs WHERE tenant_id='__TENANT__' AND level IN ('WARNING','ERROR','CRITICAL')
GROUP BY TIMESTAMP(DATE(event_time),MAKETIME(HOUR(event_time),0,0)),service_name,level,exception_type;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_prediction_monitoring AS
SELECT TIMESTAMP(DATE(predicted_at),MAKETIME(HOUR(predicted_at),0,0)) AS observed_at,
       prediction_type,model_id,COUNT(*) AS prediction_count,
       AVG(prediction_value) AS mean_predicted_value,AVG(confidence_score) AS mean_confidence
FROM ml_predictions WHERE tenant_id='__TENANT__'
GROUP BY TIMESTAMP(DATE(predicted_at),MAKETIME(HOUR(predicted_at),0,0)),prediction_type,model_id;

CREATE OR REPLACE SQL SECURITY DEFINER VIEW v_incident_sla AS
SELECT TIMESTAMP(DATE(detected_at),MAKETIME(HOUR(detected_at),0,0)) AS observed_at,
       severity,status,suspected_root_cause,COUNT(*) AS incident_count,
       AVG(CASE WHEN acknowledged_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND,detected_at,acknowledged_at)/60.0 END) AS mean_ack_minutes,
       AVG(CASE WHEN resolved_at IS NOT NULL THEN TIMESTAMPDIFF(SECOND,detected_at,resolved_at)/60.0 END) AS mean_resolution_minutes,
       MAX(CASE WHEN resolved_at IS NULL THEN TIMESTAMPDIFF(SECOND,detected_at,UTC_TIMESTAMP(6))/60.0 END) AS oldest_open_minutes,
       (SELECT COUNT(*) FROM sla_contracts WHERE tenant_id='__TENANT__' AND approval_status='APPROVED') AS approved_sla_contracts
FROM incidents WHERE tenant_id='__TENANT__'
GROUP BY TIMESTAMP(DATE(detected_at),MAKETIME(HOUR(detected_at),0,0)),severity,status,suspected_root_cause;

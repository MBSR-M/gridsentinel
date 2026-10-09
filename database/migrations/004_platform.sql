-- Operational platform metadata; no bulk raw telemetry is stored here.
CREATE TABLE IF NOT EXISTS connector_definitions (
 tenant_id VARCHAR(64) NOT NULL, connector_id VARCHAR(64) NOT NULL,
 source_type VARCHAR(32) NOT NULL, source_mode VARCHAR(24) NOT NULL,
 enabled BOOLEAN NOT NULL DEFAULT FALSE, configuration_json JSON NOT NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,connector_id),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS background_jobs (
 job_id CHAR(36) PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL,
 job_type VARCHAR(64) NOT NULL, queue VARCHAR(128) NOT NULL,
 status VARCHAR(24) NOT NULL, payload JSON NOT NULL,
 correlation_id VARCHAR(128) NOT NULL, idempotency_key VARCHAR(128) NOT NULL,
 attempt_number INT NOT NULL DEFAULT 0, max_attempts INT NOT NULL DEFAULT 5,
 next_retry_at DATETIME(6) NULL, locked_until DATETIME(6) NULL,
 deadline DATETIME(6) NOT NULL, originating_user_id BIGINT UNSIGNED NULL,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 completed_at DATETIME(6) NULL, last_error VARCHAR(255) NULL, result_json JSON NULL,
 UNIQUE KEY uq_job_idempotency(tenant_id,idempotency_key),
 KEY ix_job_retry(tenant_id,status,next_retry_at), KEY ix_job_time(tenant_id,created_at,job_id),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),
 FOREIGN KEY(tenant_id,originating_user_id) REFERENCES users(tenant_id,id),
 CHECK(status IN ('PENDING','RUNNING','RETRY','SUCCEEDED','FAILED','CANCELED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS application_logs (
 id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, event_id CHAR(36) NOT NULL,
 tenant_id VARCHAR(64) NOT NULL, event_time DATETIME(6) NOT NULL,
 level VARCHAR(16) NOT NULL, service_name VARCHAR(64) NOT NULL,
 instance_id VARCHAR(128) NOT NULL, logger_name VARCHAR(128) NOT NULL,
 message TEXT NOT NULL, trace_id VARCHAR(128) NULL, correlation_id VARCHAR(128) NULL,
 user_id BIGINT UNSIGNED NULL, incident_id CHAR(36) NULL,
 exception_type VARCHAR(128) NULL, sanitized_stack_trace TEXT NULL, metadata_json JSON NOT NULL,
 UNIQUE KEY uq_log_event(tenant_id,event_id),
 KEY ix_log_time(tenant_id,event_time,id), KEY ix_log_level(tenant_id,level,event_time),
 KEY ix_log_trace(tenant_id,trace_id), KEY ix_log_correlation(tenant_id,correlation_id),
 KEY ix_log_service(tenant_id,service_name,event_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS service_heartbeats (
 tenant_id VARCHAR(64) NOT NULL, service_name VARCHAR(64) NOT NULL,
 instance_id VARCHAR(128) NOT NULL, updated_at DATETIME(6) NOT NULL,
 status VARCHAR(24) NOT NULL, metrics_json JSON NOT NULL,
 PRIMARY KEY(tenant_id,service_name,instance_id), KEY ix_heartbeat_time(updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS api_requests (
 tenant_id VARCHAR(64) NOT NULL, actor_user_id BIGINT UNSIGNED NOT NULL,
 idempotency_key VARCHAR(128) NOT NULL, request_digest BINARY(32) NOT NULL,
 status VARCHAR(24) NOT NULL, response_json JSON NULL, created_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,actor_user_id,idempotency_key), KEY ix_request_time(created_at),
 CHECK(status IN ('PROCESSING','COMPLETE','UNCERTAIN'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS report_exports (
 export_id CHAR(36) PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL,
 export_type VARCHAR(64) NOT NULL, actor_user_id BIGINT UNSIGNED NOT NULL,
 created_at DATETIME(6) NOT NULL, content_json JSON NOT NULL,
 KEY ix_report_time(tenant_id,created_at,export_id),
 FOREIGN KEY(tenant_id,actor_user_id) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE IF NOT EXISTS audit_anchors (
 anchor_id CHAR(36) PRIMARY KEY, tenant_id VARCHAR(64) NOT NULL,
 sequence_number BIGINT UNSIGNED NOT NULL, final_digest BINARY(32) NOT NULL,
 destination VARCHAR(255) NOT NULL, anchored_at DATETIME(6) NOT NULL,
 KEY ix_anchor_time(tenant_id,anchored_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;


-- UTC, non-partitioned InnoDB tables. Raw source retention remains external.
CREATE TABLE regions (
 tenant_id VARCHAR(64) NOT NULL, region_id VARCHAR(64) NOT NULL,
 display_name VARCHAR(128) NOT NULL, parent_region_id VARCHAR(64) NULL,
 PRIMARY KEY(tenant_id,region_id), KEY ix_region_parent(tenant_id,parent_region_id),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE meters (
 tenant_id VARCHAR(64) NOT NULL, meter_id VARCHAR(64) NOT NULL,
 manufacturer_id VARCHAR(64) NULL, model VARCHAR(64) NULL, firmware_version VARCHAR(64) NULL,
 meter_type VARCHAR(24) NULL, communication_type VARCHAR(24) NULL,
 installation_status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE', installed_at DATETIME(6) NULL,
 activated_at DATETIME(6) NULL, deactivated_at DATETIME(6) NULL, region_id VARCHAR(64) NULL,
 expected_communication_minutes INT NOT NULL DEFAULT 60,
 created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,meter_id), KEY ix_meter_region(tenant_id,region_id,meter_id),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),
 CONSTRAINT ck_meter_cadence CHECK(expected_communication_minutes BETWEEN 1 AND 10080)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE network_devices (
 tenant_id VARCHAR(64) NOT NULL,device_id VARCHAR(64) NOT NULL,device_type VARCHAR(24) NOT NULL,
 vendor_id VARCHAR(64) NULL,firmware_version VARCHAR(64) NULL,communication_type VARCHAR(24) NULL,
 status VARCHAR(24) NOT NULL DEFAULT 'UNKNOWN',region_id VARCHAR(64) NULL,
 last_event_at DATETIME(6) NULL,created_at DATETIME(6) NOT NULL,updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,device_id),FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE network_links (
 link_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,parent_device_id VARCHAR(64) NOT NULL,
 child_device_id VARCHAR(64) NOT NULL,link_type VARCHAR(24) NOT NULL,valid_from DATETIME(6) NOT NULL,
 valid_to DATETIME(6) NULL,source_system VARCHAR(64) NOT NULL,
 KEY ix_network_link(tenant_id,parent_device_id,valid_from,valid_to),
 FOREIGN KEY(tenant_id,parent_device_id) REFERENCES network_devices(tenant_id,device_id),
 FOREIGN KEY(tenant_id,child_device_id) REFERENCES network_devices(tenant_id,device_id),
 CONSTRAINT ck_link_range CHECK(valid_to IS NULL OR valid_to>valid_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE meter_topology_history (
 mapping_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,meter_id VARCHAR(64) NOT NULL,
 gateway_id VARCHAR(64) NULL,dcu_id VARCHAR(64) NULL,feeder_id VARCHAR(64) NULL,transformer_id VARCHAR(64) NULL,
 sim_provider_id VARCHAR(64) NULL,region_id VARCHAR(64) NULL,valid_from DATETIME(6) NOT NULL,
 valid_to DATETIME(6) NULL,source_system VARCHAR(64) NOT NULL,mapping_quality VARCHAR(24) NOT NULL,
 UNIQUE KEY uq_topology_start(tenant_id,meter_id,valid_from),
 KEY ix_topology_gateway(tenant_id,gateway_id,valid_from,valid_to),
 FOREIGN KEY(tenant_id,meter_id) REFERENCES meters(tenant_id,meter_id),
 CONSTRAINT ck_topology_range CHECK(valid_to IS NULL OR valid_to>valid_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE event_deduplication (
 tenant_id VARCHAR(64) NOT NULL,namespace VARCHAR(64) NOT NULL,idempotency_key CHAR(64) NOT NULL,
 first_observed_at DATETIME(6) NOT NULL,event_time DATETIME(6) NOT NULL,
 processing_status VARCHAR(24) NOT NULL,expires_at DATETIME(6) NULL,
 PRIMARY KEY(tenant_id,namespace,idempotency_key),KEY ix_dedup_expiry(expires_at),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE communication_events (
 event_id CHAR(64) NOT NULL,tenant_id VARCHAR(64) NOT NULL,source_event_id VARCHAR(128) NOT NULL,
 meter_id VARCHAR(64) NULL,gateway_id VARCHAR(64) NULL,dcu_id VARCHAR(64) NULL,
 event_time DATETIME(6) NOT NULL,received_at DATETIME(6) NOT NULL,ingested_at DATETIME(6) NOT NULL,
 event_type VARCHAR(64) NOT NULL,error_code VARCHAR(64) NULL,status VARCHAR(24) NULL,
 source_system VARCHAR(64) NOT NULL,source_sequence BIGINT NULL,trace_id VARCHAR(128) NOT NULL,
 correlation_id VARCHAR(128) NOT NULL,payload_reference VARCHAR(255) NULL,evidence_json JSON NOT NULL,
 is_late BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY(tenant_id,event_id),KEY ix_comm_meter_time(tenant_id,meter_id,event_time,event_id),
 KEY ix_comm_gateway_time(tenant_id,gateway_id,event_time),KEY ix_comm_retention(event_time),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE interval_delivery_status (
 tenant_id VARCHAR(64) NOT NULL,meter_id VARCHAR(64) NOT NULL,interval_start DATETIME(6) NOT NULL,
 interval_end DATETIME(6) NOT NULL,interval_minutes INT NOT NULL,hes_received_at DATETIME(6) NULL,
 mdms_received_at DATETIME(6) NULL,quality_status VARCHAR(24) NOT NULL,estimated_flag BOOLEAN NOT NULL,
 delivery_status VARCHAR(24) NOT NULL,source_system VARCHAR(64) NOT NULL,event_time DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,meter_id,interval_start),KEY ix_interval_window(tenant_id,interval_start),
 FOREIGN KEY(tenant_id,meter_id) REFERENCES meters(tenant_id,meter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE meter_current_health (
 tenant_id VARCHAR(64) NOT NULL,meter_id VARCHAR(64) NOT NULL,last_seen_at DATETIME(6) NULL,
 current_status VARCHAR(24) NOT NULL,previous_status VARCHAR(24) NOT NULL,state_entered_at DATETIME(6) NULL,
 consecutive_failures INT NOT NULL,missing_interval_count INT NOT NULL,late_interval_count INT NOT NULL,
 communication_score DOUBLE NULL,state_confidence DOUBLE NOT NULL,delivery_status VARCHAR(24) NOT NULL,
 gateway_id VARCHAR(64) NULL,last_event_id CHAR(64) NULL,updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,meter_id),KEY ix_health_status(tenant_id,current_status,meter_id),
 KEY ix_health_seen(tenant_id,last_seen_at,meter_id),KEY ix_health_gateway(tenant_id,gateway_id,current_status),
 FOREIGN KEY(tenant_id,meter_id) REFERENCES meters(tenant_id,meter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE meter_health_hourly (
 tenant_id VARCHAR(64) NOT NULL,meter_id VARCHAR(64) NOT NULL,hour_start DATETIME(6) NOT NULL,
 expected_reads INT NULL,on_time_reads INT NOT NULL,late_reads INT NOT NULL,missing_reads INT NOT NULL,
 failure_count INT NOT NULL,communication_score DOUBLE NULL,
 PRIMARY KEY(tenant_id,meter_id,hour_start),KEY ix_hourly_window(tenant_id,hour_start),
 FOREIGN KEY(tenant_id,meter_id) REFERENCES meters(tenant_id,meter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE meter_health_daily (
 tenant_id VARCHAR(64) NOT NULL,meter_id VARCHAR(64) NOT NULL,day_start DATE NOT NULL,
 observed_successes INT NOT NULL,failure_count INT NOT NULL,late_reads INT NOT NULL,
 missing_reads INT NOT NULL,availability DOUBLE NULL,outage_seconds BIGINT NULL,
 PRIMARY KEY(tenant_id,meter_id,day_start),KEY ix_daily_window(tenant_id,day_start),
 FOREIGN KEY(tenant_id,meter_id) REFERENCES meters(tenant_id,meter_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE gateway_health_snapshots (
 tenant_id VARCHAR(64) NOT NULL,gateway_id VARCHAR(64) NOT NULL,window_start DATETIME(6) NOT NULL,
 window_end DATETIME(6) NOT NULL,active_meter_count INT NOT NULL,offline_meter_count INT NOT NULL,
 failure_rate DOUBLE NULL,network_availability DOUBLE NULL,signal_quality_summary JSON NULL,
 computed_at DATETIME(6) NOT NULL,PRIMARY KEY(tenant_id,gateway_id,window_start),
 KEY ix_gateway_window(tenant_id,window_start),FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE incidents (
 incident_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,incident_type VARCHAR(64) NOT NULL,
 severity VARCHAR(16) NOT NULL,status VARCHAR(24) NOT NULL,suspected_root_cause VARCHAR(128) NOT NULL,
 root_cause_confidence DOUBLE NOT NULL,affected_meter_count INT NOT NULL,gateway_id VARCHAR(64) NULL,
 region_id VARCHAR(64) NULL,detected_at DATETIME(6) NOT NULL,first_event_at DATETIME(6) NOT NULL,
 acknowledged_at DATETIME(6) NULL,resolved_at DATETIME(6) NULL,assigned_to BIGINT UNSIGNED NULL,
 correlation_group_id VARCHAR(128) NOT NULL,model_version VARCHAR(64) NULL,created_at DATETIME(6) NOT NULL,
 updated_at DATETIME(6) NOT NULL,UNIQUE KEY uq_incident_tenant(tenant_id,incident_id),
 UNIQUE KEY uq_incident_group(tenant_id,correlation_group_id),KEY ix_incident_status(tenant_id,status,detected_at,incident_id),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),FOREIGN KEY(tenant_id,assigned_to) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE incident_evidence (
 evidence_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,
 incident_id CHAR(36) NOT NULL,evidence_type VARCHAR(64) NOT NULL,source_system VARCHAR(64) NOT NULL,
 source_reference VARCHAR(255) NOT NULL,event_time DATETIME(6) NOT NULL,evidence_json JSON NOT NULL,
 created_at DATETIME(6) NOT NULL,UNIQUE KEY uq_evidence(incident_id,source_reference),
 FOREIGN KEY(tenant_id,incident_id) REFERENCES incidents(tenant_id,incident_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE incident_history (
 history_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,
 incident_id CHAR(36) NOT NULL,actor_user_id BIGINT UNSIGNED NULL,change_type VARCHAR(64) NOT NULL,
 before_state JSON NULL,after_state JSON NOT NULL,correlation_id VARCHAR(128) NOT NULL,created_at DATETIME(6) NOT NULL,
 KEY ix_incident_history(incident_id,history_id),FOREIGN KEY(tenant_id,incident_id) REFERENCES incidents(tenant_id,incident_id),
 FOREIGN KEY(tenant_id,actor_user_id) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE incident_actions (
 action_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,incident_id CHAR(36) NOT NULL,
 action_type VARCHAR(64) NOT NULL,action_status VARCHAR(24) NOT NULL,executed_by BIGINT UNSIGNED NOT NULL,
 recommendation_json JSON NULL,result_json JSON NOT NULL,created_at DATETIME(6) NOT NULL,completed_at DATETIME(6) NULL,
 KEY ix_incident_action(incident_id,created_at),FOREIGN KEY(tenant_id,incident_id) REFERENCES incidents(tenant_id,incident_id),
 FOREIGN KEY(tenant_id,executed_by) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE connector_checkpoints (
 tenant_id VARCHAR(64) NOT NULL,connector_id VARCHAR(64) NOT NULL,source_shard VARCHAR(64) NOT NULL,
 watermark VARCHAR(255) NOT NULL,source_event_id VARCHAR(128) NULL,last_successful_extract DATETIME(6) NOT NULL,
 checksum CHAR(64) NULL,updated_at DATETIME(6) NOT NULL,PRIMARY KEY(tenant_id,connector_id,source_shard),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE outbox_events (
 outbox_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,aggregate_id VARCHAR(128) NOT NULL,
 event_type VARCHAR(128) NOT NULL,topic VARCHAR(128) NOT NULL,partition_key VARCHAR(128) NOT NULL,payload JSON NOT NULL,
 created_at DATETIME(6) NOT NULL,published_at DATETIME(6) NULL,attempt_count INT NOT NULL DEFAULT 0,last_error VARCHAR(255) NULL,
 KEY ix_outbox_pending(published_at,created_at),FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE sla_contracts (
 tenant_id VARCHAR(64) NOT NULL,contract_id VARCHAR(64) NOT NULL,approval_status VARCHAR(24) NOT NULL,
 definition_json JSON NOT NULL,approved_by BIGINT UNSIGNED NULL,approved_at DATETIME(6) NULL,
 PRIMARY KEY(tenant_id,contract_id),FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),
 FOREIGN KEY(tenant_id,approved_by) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

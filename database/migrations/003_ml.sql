CREATE TABLE ml_feature_definitions (
 feature_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,feature_name VARCHAR(64) NOT NULL,
 feature_version VARCHAR(24) NOT NULL,entity_type VARCHAR(24) NOT NULL,data_type VARCHAR(24) NOT NULL,
 transformation_definition TEXT NOT NULL,default_value DOUBLE NULL,created_at DATETIME(6) NOT NULL,
 UNIQUE KEY uq_feature_version(feature_name,feature_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_training_runs (
 training_run_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,model_name VARCHAR(64) NOT NULL,
 dataset_start DATETIME(6) NOT NULL,dataset_end DATETIME(6) NOT NULL,validation_start DATETIME(6) NOT NULL,
 validation_end DATETIME(6) NOT NULL,feature_schema_version VARCHAR(24) NOT NULL,sample_count INT NOT NULL,
 status VARCHAR(24) NOT NULL,started_at DATETIME(6) NOT NULL,completed_at DATETIME(6) NULL,
 metrics_json JSON NOT NULL,configuration_snapshot JSON NOT NULL,artifact_reference VARCHAR(255) NULL,
 model_owner BIGINT UNSIGNED NULL,KEY ix_training_tenant(tenant_id,started_at),
 FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),FOREIGN KEY(tenant_id,model_owner) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_model_registry (
 model_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,model_name VARCHAR(64) NOT NULL,
 model_version VARCHAR(64) NOT NULL,algorithm VARCHAR(64) NOT NULL,feature_schema_version VARCHAR(24) NOT NULL,
 artifact_reference VARCHAR(255) NOT NULL,artifact_json JSON NOT NULL,artifact_checksum CHAR(64) NOT NULL,
 metrics_json JSON NOT NULL,approval_status VARCHAR(24) NOT NULL,created_at DATETIME(6) NOT NULL,
 approved_by BIGINT UNSIGNED NULL,approved_at DATETIME(6) NULL,activated_at DATETIME(6) NULL,
 is_active BOOLEAN NOT NULL DEFAULT FALSE,training_run_id CHAR(36) NOT NULL,
 UNIQUE KEY uq_model_version(tenant_id,model_name,model_version),KEY ix_model_active(tenant_id,model_name,is_active),
 UNIQUE KEY uq_model_tenant(tenant_id,model_id),FOREIGN KEY(tenant_id) REFERENCES tenants(tenant_id),
 FOREIGN KEY(tenant_id,approved_by) REFERENCES users(tenant_id,id),FOREIGN KEY(training_run_id) REFERENCES ml_training_runs(training_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_model_deployments (
 tenant_id VARCHAR(64) NOT NULL,model_name VARCHAR(64) NOT NULL,active_model_id CHAR(36) NULL,
 previous_model_id CHAR(36) NULL,updated_at DATETIME(6) NOT NULL,
 PRIMARY KEY(tenant_id,model_name),FOREIGN KEY(tenant_id,active_model_id) REFERENCES ml_model_registry(tenant_id,model_id),
 FOREIGN KEY(tenant_id,previous_model_id) REFERENCES ml_model_registry(tenant_id,model_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_predictions (
 prediction_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,model_id CHAR(36) NULL,
 entity_id VARCHAR(64) NOT NULL,entity_type VARCHAR(24) NOT NULL,prediction_type VARCHAR(64) NOT NULL,
 prediction_value DOUBLE NOT NULL,confidence_score DOUBLE NULL,predicted_at DATETIME(6) NOT NULL,
 feature_snapshot_reference VARCHAR(255) NULL,feature_snapshot JSON NOT NULL,explanation_json JSON NOT NULL,
 KEY ix_prediction_entity(tenant_id,entity_id,predicted_at),UNIQUE KEY uq_prediction_tenant(tenant_id,prediction_id),
 FOREIGN KEY(tenant_id,model_id) REFERENCES ml_model_registry(tenant_id,model_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_prediction_outcomes (
 tenant_id VARCHAR(64) NOT NULL,prediction_id CHAR(36) NOT NULL,actual_outcome BOOLEAN NULL,
 outcome_observed_at DATETIME(6) NOT NULL,intervention_occurred BOOLEAN NULL,
 intervention_reference VARCHAR(255) NULL,evaluated_at DATETIME(6) NULL,PRIMARY KEY(tenant_id,prediction_id),
 FOREIGN KEY(tenant_id,prediction_id) REFERENCES ml_predictions(tenant_id,prediction_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;
CREATE TABLE ml_drift_metrics (
 drift_id CHAR(36) PRIMARY KEY,tenant_id VARCHAR(64) NOT NULL,model_id CHAR(36) NOT NULL,
 observation_start DATETIME(6) NOT NULL,observation_end DATETIME(6) NOT NULL,
 metrics_json JSON NOT NULL,alert_threshold DOUBLE NOT NULL,computed_at DATETIME(6) NOT NULL,
 KEY ix_drift_model(tenant_id,model_id,computed_at),FOREIGN KEY(tenant_id,model_id) REFERENCES ml_model_registry(tenant_id,model_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

-- MySQL 8.4 InnoDB identity. Composite foreign keys enforce tenant boundaries.
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    display_name VARCHAR(128) NOT NULL,
    created_at DATETIME(6) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS users (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    username VARCHAR(128) NOT NULL,
    email VARCHAR(255) NULL,
    password_hash VARBINARY(512) NOT NULL,
    password_salt VARBINARY(64) NOT NULL,
    password_algorithm VARCHAR(64) NOT NULL,
    password_iterations INT NOT NULL,
    status VARCHAR(24) NOT NULL DEFAULT 'ACTIVE',
    failed_login_count INT NOT NULL DEFAULT 0,
    locked_until DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    last_login_at DATETIME(6) NULL,
    created_by BIGINT UNSIGNED NULL,
    UNIQUE KEY uq_user_tenant_id (tenant_id,id),
    UNIQUE KEY uq_user_username (tenant_id,username),
    UNIQUE KEY uq_user_email (tenant_id,email),
    CONSTRAINT fk_user_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id),
    CONSTRAINT fk_user_creator FOREIGN KEY (tenant_id,created_by) REFERENCES users(tenant_id,id),
    CHECK (status IN ('ACTIVE','DISABLED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS roles (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    role_code VARCHAR(64) NOT NULL,
    display_name VARCHAR(128) NOT NULL,
    description TEXT NOT NULL,
    is_system_role BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE KEY uq_role_tenant_id (tenant_id,id),
    UNIQUE KEY uq_role_code (tenant_id,role_code),
    CONSTRAINT fk_role_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS permissions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    permission_code VARCHAR(128) NOT NULL,
    resource VARCHAR(64) NOT NULL,
    action VARCHAR(64) NOT NULL,
    description TEXT NOT NULL,
    UNIQUE KEY uq_permission_tenant_id (tenant_id,id),
    UNIQUE KEY uq_permission_code (tenant_id,permission_code),
    CONSTRAINT fk_permission_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS user_roles (
    tenant_id VARCHAR(64) NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    role_id BIGINT UNSIGNED NOT NULL,
    assigned_by BIGINT UNSIGNED NULL,
    assigned_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id,user_id,role_id),
    CONSTRAINT fk_ur_user FOREIGN KEY (tenant_id,user_id) REFERENCES users(tenant_id,id),
    CONSTRAINT fk_ur_role FOREIGN KEY (tenant_id,role_id) REFERENCES roles(tenant_id,id),
    CONSTRAINT fk_ur_actor FOREIGN KEY (tenant_id,assigned_by) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS role_permissions (
    tenant_id VARCHAR(64) NOT NULL,
    role_id BIGINT UNSIGNED NOT NULL,
    permission_id BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id,role_id,permission_id),
    CONSTRAINT fk_rp_role FOREIGN KEY (tenant_id,role_id) REFERENCES roles(tenant_id,id),
    CONSTRAINT fk_rp_permission FOREIGN KEY (tenant_id,permission_id) REFERENCES permissions(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS user_sessions (
    id CHAR(36) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    session_token_hash BINARY(32) NOT NULL,
    refresh_token_hash BINARY(32) NOT NULL,
    csrf_token_hash BINARY(32) NOT NULL,
    token_family_id CHAR(36) NOT NULL,
    created_at DATETIME(6) NOT NULL,
    last_activity_at DATETIME(6) NOT NULL,
    access_expires_at DATETIME(6) NOT NULL,
    absolute_expires_at DATETIME(6) NOT NULL,
    revoked_at DATETIME(6) NULL,
    revoke_reason VARCHAR(255) NULL,
    ip_address VARBINARY(16) NULL,
    user_agent_hash BINARY(32) NOT NULL,
    device_label VARCHAR(128) NOT NULL,
    UNIQUE KEY uq_session_tenant_id (tenant_id,id),
    UNIQUE KEY uq_access_token (session_token_hash),
    UNIQUE KEY uq_refresh_token (refresh_token_hash),
    KEY ix_session_user (tenant_id,user_id,revoked_at),
    KEY ix_session_family (tenant_id,token_family_id),
    KEY ix_session_expiry (absolute_expires_at,revoked_at),
    CONSTRAINT fk_session_user FOREIGN KEY (tenant_id,user_id) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS refresh_token_history (
    token_hash BINARY(32) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    session_id CHAR(36) NOT NULL,
    token_family_id CHAR(36) NOT NULL,
    consumed_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    KEY ix_refresh_expiry (expires_at),
    CONSTRAINT fk_refresh_session FOREIGN KEY (tenant_id,session_id) REFERENCES user_sessions(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS api_tokens (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    owner_user_id BIGINT UNSIGNED NOT NULL,
    token_name VARCHAR(128) NOT NULL,
    token_hash BINARY(32) NOT NULL,
    scopes JSON NOT NULL,
    created_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    last_used_at DATETIME(6) NULL,
    revoked_at DATETIME(6) NULL,
    UNIQUE KEY uq_api_token_hash(token_hash),
    CONSTRAINT fk_api_token_user FOREIGN KEY (tenant_id,owner_user_id) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS login_throttle (
    tenant_id VARCHAR(64) NOT NULL,
    bucket_hash BINARY(32) NOT NULL,
    window_started_at DATETIME(6) NOT NULL,
    attempts INT NOT NULL,
    PRIMARY KEY (tenant_id,bucket_hash),
    CONSTRAINT fk_throttle_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS login_attempts (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    attempted_at DATETIME(6) NOT NULL,
    username_identifier BINARY(32) NOT NULL,
    ip_address VARBINARY(16) NULL,
    outcome VARCHAR(24) NOT NULL,
    reason_code VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    KEY ix_login_attempts (tenant_id,attempted_at),
    CONSTRAINT fk_login_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS audit_heads (
    tenant_id VARCHAR(64) PRIMARY KEY,
    sequence_number BIGINT UNSIGNED NOT NULL,
    last_hash BINARY(32) NOT NULL,
    CONSTRAINT fk_head_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE IF NOT EXISTS audit_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    sequence_number BIGINT UNSIGNED NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    actor_user_id BIGINT UNSIGNED NULL,
    session_id CHAR(36) NULL,
    action VARCHAR(128) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    correlation_id VARCHAR(128) NOT NULL,
    outcome VARCHAR(24) NOT NULL,
    before_state JSON NULL,
    after_state JSON NULL,
    metadata JSON NULL,
    canonical_payload LONGTEXT NOT NULL,
    previous_hash BINARY(32) NOT NULL,
    event_hash BINARY(32) NOT NULL,
    UNIQUE KEY uq_audit_sequence (tenant_id,sequence_number),
    KEY ix_audit_time (tenant_id,occurred_at,id),
    KEY ix_audit_correlation (tenant_id,correlation_id),
    CONSTRAINT fk_audit_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id),
    CONSTRAINT fk_audit_actor FOREIGN KEY (tenant_id,actor_user_id) REFERENCES users(tenant_id,id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_as_cs;

CREATE TABLE agent_profile_user (
    user_id BIGINT NOT NULL PRIMARY KEY,
    revision BIGINT NOT NULL DEFAULT 0,
    memory_epoch BIGINT NOT NULL DEFAULT 0
) ${tableOptions};

CREATE TABLE agent_profile_fact (
    fact_id VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    attribute_key VARCHAR(96) NOT NULL,
    fact_version BIGINT NOT NULL,
    state VARCHAR(16) NOT NULL,
    value_text LONGTEXT NOT NULL,
    source_json LONGTEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE (user_id, attribute_key)
) ${tableOptions};
CREATE INDEX agent_profile_owner ON agent_profile_fact (user_id, state, updated_at);

CREATE TABLE agent_profile_version (
    fact_id VARCHAR(64) NOT NULL,
    fact_version BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    attribute_key VARCHAR(96) NOT NULL,
    state VARCHAR(16) NOT NULL,
    value_text LONGTEXT NOT NULL,
    source_json LONGTEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (fact_id, fact_version)
) ${tableOptions};

CREATE TABLE agent_profile_extraction (
    summary_job_id VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    result_format VARCHAR(24) NOT NULL DEFAULT 'COMPOSITE_V2',
    snapshot_json LONGTEXT NOT NULL,
    event_body LONGTEXT NOT NULL,
    result_body LONGTEXT,
    state VARCHAR(16) NOT NULL,
    worker_epoch BIGINT NOT NULL DEFAULT 0,
    lease_until BIGINT NOT NULL DEFAULT 0,
    retry_at BIGINT NOT NULL DEFAULT 0,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(128),
    updated_at BIGINT NOT NULL
) ${tableOptions};
CREATE INDEX agent_profile_extraction_recovery ON agent_profile_extraction (state, retry_at, lease_until);

CREATE TABLE agent_profile_command (
    command_id VARCHAR(64) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    fact_id VARCHAR(64) NOT NULL,
    fact_version BIGINT NOT NULL
) ${tableOptions};

CREATE TABLE agent_profile_projection (
    projection_id VARCHAR(64) NOT NULL PRIMARY KEY,
    fact_id VARCHAR(64) NOT NULL,
    user_id BIGINT NOT NULL,
    fact_version BIGINT NOT NULL,
    target_tag VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    worker_epoch BIGINT NOT NULL DEFAULT 0,
    lease_until BIGINT NOT NULL DEFAULT 0,
    retry_at BIGINT NOT NULL DEFAULT 0,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(128),
    updated_at BIGINT NOT NULL,
    UNIQUE (fact_id, fact_version, target_tag)
) ${tableOptions};
CREATE INDEX agent_profile_projection_recovery ON agent_profile_projection (target_tag, state, retry_at, lease_until);

-- M03 is additive. Never rewrite V1 or delete original turns while compacting hot context.
CREATE TABLE agent_memory_summary (
    conversation_id VARCHAR(128) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    epoch BIGINT NOT NULL,
    summary_version BIGINT NOT NULL,
    covered_through BIGINT NOT NULL,
    summary_text LONGTEXT NOT NULL,
    job_id VARCHAR(64) NOT NULL,
    updated_at BIGINT NOT NULL
) ${tableOptions};

CREATE TABLE agent_memory_job (
    job_id VARCHAR(64) NOT NULL PRIMARY KEY,
    conversation_id VARCHAR(128) NOT NULL,
    user_id BIGINT NOT NULL,
    epoch BIGINT NOT NULL,
    base_version BIGINT NOT NULL,
    covered_from BIGINT NOT NULL,
    covered_to BIGINT NOT NULL,
    input_body LONGTEXT NOT NULL,
    state VARCHAR(16) NOT NULL,
    worker_epoch BIGINT NOT NULL DEFAULT 0,
    lease_until BIGINT NOT NULL DEFAULT 0,
    retry_at BIGINT NOT NULL DEFAULT 0,
    result_text LONGTEXT,
    vector_state VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    attempts BIGINT NOT NULL DEFAULT 0,
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(128),
    updated_at BIGINT NOT NULL,
    UNIQUE (conversation_id, epoch, base_version)
) ${tableOptions};
CREATE INDEX agent_memory_job_recovery ON agent_memory_job (state, retry_at, lease_until);

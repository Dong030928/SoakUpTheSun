CREATE TABLE agent_rag_target (
    target_tag VARCHAR(64) NOT NULL PRIMARY KEY,
    redis_prefix VARCHAR(512) NOT NULL,
    es_endpoint VARCHAR(2048) NOT NULL,
    es_index VARCHAR(128) NOT NULL
) ${tableOptions};

CREATE TABLE agent_rag_document (
    document_id VARCHAR(128) NOT NULL PRIMARY KEY,
    owner_id BIGINT NOT NULL,
    desired_version BIGINT NOT NULL DEFAULT 0,
    desired_target VARCHAR(64) NOT NULL DEFAULT '',
    current_version BIGINT NOT NULL DEFAULT 0,
    current_target VARCHAR(64) NOT NULL DEFAULT '',
    revision BIGINT NOT NULL DEFAULT 0,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at BIGINT NOT NULL
) ${tableOptions};

CREATE TABLE agent_rag_version (
    document_id VARCHAR(128) NOT NULL,
    document_version BIGINT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    source_hash VARCHAR(64) NOT NULL,
    pipeline_tag VARCHAR(64) NOT NULL,
    visibility VARCHAR(16) NOT NULL,
    title VARCHAR(512) NOT NULL,
    source_ref VARCHAR(2048) NOT NULL,
    parent_count BIGINT NOT NULL,
    child_count BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (document_id, document_version)
) ${tableOptions};

CREATE TABLE agent_rag_parent (
    parent_id VARCHAR(80) NOT NULL PRIMARY KEY,
    document_id VARCHAR(128) NOT NULL,
    document_version BIGINT NOT NULL,
    ordinal_no BIGINT NOT NULL,
    body LONGTEXT NOT NULL,
    heading_path VARCHAR(2048) NOT NULL,
    page_start BIGINT NOT NULL,
    page_end BIGINT NOT NULL,
    line_start BIGINT NOT NULL,
    line_end BIGINT NOT NULL,
    UNIQUE (document_id, document_version, ordinal_no)
) ${tableOptions};

CREATE TABLE agent_rag_child (
    child_id VARCHAR(80) NOT NULL PRIMARY KEY,
    parent_id VARCHAR(80) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    document_version BIGINT NOT NULL,
    ordinal_no BIGINT NOT NULL,
    body LONGTEXT NOT NULL,
    heading_path VARCHAR(2048) NOT NULL,
    page_start BIGINT NOT NULL,
    page_end BIGINT NOT NULL,
    line_start BIGINT NOT NULL,
    line_end BIGINT NOT NULL,
    content_hash VARCHAR(64) NOT NULL,
    UNIQUE (document_id, document_version, ordinal_no)
) ${tableOptions};
CREATE INDEX agent_rag_child_parent ON agent_rag_child (parent_id);

CREATE TABLE agent_rag_job (
    job_id VARCHAR(64) NOT NULL PRIMARY KEY,
    document_id VARCHAR(128) NOT NULL,
    document_version BIGINT NOT NULL,
    target_tag VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    redis_done BOOLEAN NOT NULL DEFAULT FALSE,
    es_done BOOLEAN NOT NULL DEFAULT FALSE,
    worker_epoch BIGINT NOT NULL DEFAULT 0,
    lease_until BIGINT NOT NULL DEFAULT 0,
    retry_at BIGINT NOT NULL DEFAULT 0,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(128),
    updated_at BIGINT NOT NULL,
    UNIQUE (document_id, document_version, target_tag)
) ${tableOptions};
CREATE INDEX agent_rag_job_recovery ON agent_rag_job (target_tag, state, retry_at, lease_until);

CREATE TABLE agent_rag_embedding (
    child_id VARCHAR(80) NOT NULL,
    target_tag VARCHAR(64) NOT NULL,
    vector_dimension BIGINT NOT NULL,
    vector_payload LONGTEXT NOT NULL,
    PRIMARY KEY (child_id, target_tag)
) ${tableOptions};

CREATE TABLE agent_rag_cleanup (
    job_id VARCHAR(64) NOT NULL,
    child_id VARCHAR(80) NOT NULL,
    target_tag VARCHAR(64) NOT NULL,
    PRIMARY KEY (job_id, child_id, target_tag)
) ${tableOptions};

-- Independent of a job lease: late network completions can occur after that job has finished.
CREATE TABLE agent_rag_orphan (
    child_id VARCHAR(80) NOT NULL,
    target_tag VARCHAR(64) NOT NULL,
    revision BIGINT NOT NULL DEFAULT 1,
    state VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    retry_at BIGINT NOT NULL DEFAULT 0,
    attempts BIGINT NOT NULL DEFAULT 0,
    last_error VARCHAR(128),
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (child_id, target_tag)
) ${tableOptions};
CREATE INDEX agent_rag_orphan_recovery ON agent_rag_orphan (state, retry_at, updated_at);

CREATE TABLE agent_memory_session (
    conversation_id VARCHAR(128) NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    epoch BIGINT NOT NULL,
    highest_seen BIGINT NOT NULL DEFAULT 0,
    archived_through BIGINT NOT NULL DEFAULT 0,
    last_activity BIGINT NOT NULL,
    retired BOOLEAN NOT NULL DEFAULT FALSE
) ${tableOptions};

CREATE TABLE agent_memory_turn (
    event_id VARCHAR(128) NOT NULL PRIMARY KEY,
    conversation_id VARCHAR(128) NOT NULL,
    epoch BIGINT NOT NULL,
    turn_seq BIGINT NOT NULL,
    user_text LONGTEXT NOT NULL,
    assistant_text LONGTEXT NOT NULL,
    occurred_at BIGINT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    CONSTRAINT agent_memory_turn_position UNIQUE (conversation_id, epoch, turn_seq)
) ${tableOptions};

CREATE TABLE memory_log (
    event_id VARCHAR(128) NOT NULL PRIMARY KEY,
    conversation_id VARCHAR(128) NOT NULL,
    epoch BIGINT NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    applied_at BIGINT NOT NULL
) ${tableOptions};

-- The framework's default VARCHAR(36) cannot hold our user-scoped IDs and epoch.
CREATE TABLE SPRING_AI_CHAT_MEMORY (
    conversation_id VARCHAR(160) NOT NULL,
    content LONGTEXT NOT NULL,
    type VARCHAR(16) NOT NULL,
    `timestamp` TIMESTAMP(6) NOT NULL,
    sequence_id BIGINT NOT NULL,
    CONSTRAINT agent_memory_snapshot_position UNIQUE (conversation_id, sequence_id)
) ${tableOptions};

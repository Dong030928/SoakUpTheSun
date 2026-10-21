CREATE TABLE agent_profile_backfill (
    target_tag VARCHAR(64) NOT NULL PRIMARY KEY,
    last_fact_id VARCHAR(64) NOT NULL DEFAULT '',
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at BIGINT NOT NULL
) ${tableOptions};

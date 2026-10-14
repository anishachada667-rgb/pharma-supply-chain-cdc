BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;

-- Preserve each Kafka message once, including tombstones.
CREATE TABLE IF NOT EXISTS raw.cdc_events (
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    message_key JSONB NOT NULL,
    message_value JSONB,
    operation TEXT CHECK (operation IN ('r', 'c', 'u', 'd')),
    source_lsn BIGINT,
    source_timestamp_ms BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, kafka_partition, kafka_offset)
);

-- Current source state; retain delete markers to prevent
-- older events from resurrecting deleted records.
CREATE TABLE IF NOT EXISTS staging.current_records (
    source_table TEXT NOT NULL,
    record_id INTEGER NOT NULL,
    record_data JSONB,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    last_topic TEXT NOT NULL,
    last_partition INTEGER NOT NULL,
    last_offset BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_table, record_id),
    CHECK (is_deleted OR record_data IS NOT NULL)
);

-- Next offset to process for each topic partition.
-- Updated in the same database transaction as event application.
CREATE TABLE IF NOT EXISTS audit.consumer_checkpoints (
    topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    next_offset BIGINT NOT NULL CHECK (next_offset >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (topic, kafka_partition)
);

COMMIT;


CREATE TABLE sink_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    kafka_topic TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE sink_writes_log (
    log_id SERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    kafka_offset BIGINT,
    phase TEXT,
    ts TIMESTAMP DEFAULT now()
);
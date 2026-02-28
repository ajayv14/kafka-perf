

CREATE TABLE sink_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    kafka_topic TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);

// Do not alter when kafka transaction is disabled.
ALTER TABLE sink_events 
ADD CONSTRAINT unique_kafka_offset 
UNIQUE (kafka_topic, kafka_partition, kafka_offset);
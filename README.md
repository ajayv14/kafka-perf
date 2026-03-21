# Kafka Perf Baseline Consumer Package

This repository contains a small Kafka performance harness. The baseline package under `/Users/ajay/Workspace/kafka-perf/app/src/main/java/com/kafka/perf/baseline` provides:

- `BaselineProducer`: sends benchmark traffic to Kafka.
- `BaselineConsumer`: consumes continuously with minimal processing overhead.
- `PostgresSinkConsumer`: consumes from Kafka and writes each record to PostgreSQL.

The shared runtime configuration for both consumers lives in `/Users/ajay/Workspace/kafka-perf/app/src/main/java/com/kafka/perf/configs/KafkaConsumerConfig.java` and is loaded from `/Users/ajay/Workspace/kafka-perf/app/src/main/resources/benchmark.properties` with environment-variable overrides.

## Prerequisites

- Java 17
- Maven 3.9+
- Docker and Docker Compose
- A Kafka cluster reachable from the app
- PostgreSQL only if you want to run `PostgresSinkConsumer`

## Core Configuration

Default values are defined in `/Users/ajay/Workspace/kafka-perf/app/src/main/resources/benchmark.properties`.

Consumer overrides use these environment variables:

| Property | Environment variable | Default |
| --- | --- | --- |
| `consumer.bootstrap.servers` | `KAFKA_BROKERS` | `localhost:9092,localhost:9093,localhost:9094` |
| `consumer.topic` | `KAFKA_TOPIC` | `eos-topic` |
| `consumer.group.id` | `KAFKA_GROUP_ID` | `scalable-consumer-group` |
| `consumer.isolation.level` | `KAFKA_ISOLATION_LEVEL` | `read_uncommitted` |
| `consumer.max.poll.records` | `KAFKA_MAX_POLL_RECORDS` | `3000` |
| `consumer.poll.timeout.ms` | `KAFKA_POLL_TIMEOUT_MS` | `100` |
| `consumer.enable.auto.commit` | `KAFKA_ENABLE_AUTO_COMMIT` | `false` |
| `consumer.auto.commit.interval.ms` | `KAFKA_AUTO_COMMIT_INTERVAL_MS` | `5000` |
| `consumer.auto.offset.reset` | `KAFKA_AUTO_OFFSET_RESET` | `earliest` |
| `consumer.fetch.min.bytes` | `KAFKA_FETCH_MIN_BYTES` | `16384` |
| `consumer.fetch.max.wait.ms` | `KAFKA_FETCH_MAX_WAIT_MS` | `50` |
| `consumer.max.partition.fetch.bytes` | `KAFKA_MAX_PARTITION_FETCH_BYTES` | `10485760` |
| `consumer.session.timeout.ms` | `KAFKA_SESSION_TIMEOUT_MS` | `30000` |
| `consumer.heartbeat.interval.ms` | `KAFKA_HEARTBEAT_INTERVAL_MS` | `10000` |
| `consumer.max.poll.interval.ms` | `KAFKA_MAX_POLL_INTERVAL_MS` | `600000` |
| `consumer.log.interval.secs` | `KAFKA_LOG_INTERVAL_SECS` | `10` |

PostgreSQL overrides for `PostgresSinkConsumer`:

| Property | Environment variable | Default |
| --- | --- | --- |
| `postgres.url` | `POSTGRES_URL` | `jdbc:postgresql://localhost:5432/eos_sink` |
| `postgres.user` | `POSTGRES_USER` | `eos` |
| `postgres.password` | `POSTGRES_PASSWORD` | `eos` |
| `postgres.sink.table` | `POSTGRES_SINK_TABLE` | `sink_events` |
| `postgres.connection.pool.size` | `POSTGRES_POOL_SIZE` | `10` |
| `postgres.write.batch.size` | `POSTGRES_WRITE_BATCH_SIZE` | `100` |

## Start Infrastructure

Kafka cluster:

```bash
cd /Users/ajay/Workspace/kafka-perf/kafka-cluster
docker-compose -f docker-compose-kafka-cluster.yml up -d
```

PostgreSQL sink table:

```sql
CREATE TABLE sink_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64),
    kafka_topic TEXT,
    kafka_partition INT,
    kafka_offset BIGINT,
    payload TEXT,
    created_at TIMESTAMP DEFAULT now()
);
```

If you need exactly-once deduplication at the sink, add the unique constraint documented in `/Users/ajay/Workspace/kafka-perf/sink/README.md`.

## Build

```bash
cd /Users/ajay/Workspace/kafka-perf/app
mvn clean package
```

## Run Locally

Run the producer:

```bash
cd /Users/ajay/Workspace/kafka-perf/app
mvn -q exec:java -Dexec.mainClass=com.kafka.perf.baseline.BaselineProducer
```

Run the minimal consumer:

```bash
cd /Users/ajay/Workspace/kafka-perf/app
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094 \
KAFKA_TOPIC=eos-topic \
KAFKA_GROUP_ID=baseline-consumer-group \
mvn -q exec:java -Dexec.mainClass=com.kafka.perf.baseline.BaselineConsumer
```

Run the PostgreSQL sink consumer:

```bash
cd /Users/ajay/Workspace/kafka-perf/app
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094 \
KAFKA_TOPIC=eos-topic \
KAFKA_GROUP_ID=baseline-sink-group \
POSTGRES_URL=jdbc:postgresql://localhost:5432/eos_sink \
POSTGRES_USER=eos \
POSTGRES_PASSWORD=eos \
mvn -q exec:java -Dexec.mainClass=com.kafka.perf.baseline.PostgresSinkConsumer
```

`PostgresSinkConsumer` is intended for sink-side throughput and EOS-boundary observations. It writes each record to PostgreSQL and commits Kafka offsets with explicit `commitSync(...)` calls on the last successfully persisted offsets. This keeps the database-write/offset-commit boundary easier to interpret than `commitAsync(...)`, which is important for replay, late-commit, and audit-overhead measurements.

## Run With Docker Compose

The baseline compose file is `/Users/ajay/Workspace/kafka-perf/app/docker-compose-baseline.yml`.

```bash
cd /Users/ajay/Workspace/kafka-perf/app
docker-compose -f docker-compose-baseline.yml up -d --scale consumer=3
docker-compose -f docker-compose-baseline.yml logs -f consumer
docker-compose -f docker-compose-baseline.yml down
```

Override runtime settings with the same environment variable names used by `KafkaConsumerConfig`, for example:

```bash
cd /Users/ajay/Workspace/kafka-perf/app
KAFKA_BROKERS=kafka-1:29092,kafka-2:29092,kafka-3:29092 \
KAFKA_TOPIC=eos-topic \
KAFKA_GROUP_ID=baseline-consumer-group \
KAFKA_ISOLATION_LEVEL=read_committed \
docker-compose -f docker-compose-baseline.yml up -d --scale consumer=3
```

## Package Notes

- `BaselineConsumer` polls continuously and does not do application-level processing. It is appropriate as a low-overhead consumer baseline, not as an end-to-end correctness harness.
- `PostgresSinkConsumer` writes one row per message using a pooled JDBC connection and per-record `INSERT`.
- The sink consumer requires `consumer.enable.auto.commit=false` and commits only explicitly persisted offsets with `commitSync(...)`.
- The synchronous commit path is intentional for paper measurements: it avoids the ambiguity of `commitAsync(...)` when comparing baseline, transactions-enabled, and transactions-plus-audit runs.
- The sink consumer is still not a strict end-to-end exactly-once sink for PostgreSQL. It is a controlled external-sink benchmark with clearer commit semantics at the Kafka boundary.
- Logging in `PostgresSinkConsumer` reports interval and lifetime progress from the consumer itself. Use Grafana/Prometheus as the primary throughput source if that is your reporting path.

## Review Summary

Review of the baseline consumer package found:

- The compose file was using `TOPIC`, `GROUP_ID`, and `ISOLATION_LEVEL`, but the code only reads `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, and `KAFKA_ISOLATION_LEVEL`. The README now documents the names the code actually accepts, and the compose file was aligned to match.
- `PostgresSinkConsumer` now uses explicit persisted-offset `commitSync(...)` commits and fail-fast sink handling so offset progress is not acknowledged past a terminal PostgreSQL write failure.

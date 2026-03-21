# Audit Package

This package adds lightweight audit events around the consumer-side EOS boundary.
It emits one event after a non-empty `poll()` and one event after a successful
`commitSync()`, then uses a Kafka Streams processor to detect batches that remain
unmatched beyond a timeout threshold.

`BATCH_READ` events now carry a deterministic batch fingerprint and per-partition
offset metadata so the same Kafka work item can be recognized when it is replayed.

## Framework Summary

The implemented audit framework has three parts:

1. `AuditableConsumer`
   Wraps a `KafkaConsumer` and intercepts two points in the sink consumer flow:
   after a non-empty `poll()` and after a successful `commitSync()`.
2. `AuditProducer`
   Publishes those audit events asynchronously to the audit topic using a
   best-effort producer so the observer does not block the business path.
3. `AuditAggregator`
   Reads the audit topic with Kafka Streams, keeps pending batch state in a
   persistent key-value store, and emits lifecycle outcomes to `audit.outcomes`.

At runtime the framework behaves as follows:

1. A consumer polls a batch.
2. `AuditableConsumer` computes a deterministic batch fingerprint from the
   consumer group, source topic, record count, and per-partition offset ranges.
3. It emits a `BATCH_READ` event carrying that fingerprint and the batch's
   per-partition metadata.
4. After synchronous processing completes and `commitSync()` succeeds, it emits
   an `OFFSET_COMMITTED` event with the same fingerprint.
5. `AuditAggregator` stores each `BATCH_READ` as a pending lifecycle entry.
6. If the same fingerprint appears again before commit, the aggregator emits
   `REPLAY_OBSERVED`.
7. If a pending batch exceeds the configured timeout threshold, the aggregator
   emits `ESTIMATED_FAILED` and keeps the entry in timed-out state.
8. If a timed-out batch later receives `OFFSET_COMMITTED`, the aggregator emits
   `LATE_COMMIT`; otherwise a normal successful path emits `COMMITTED`.

The outcome topic is therefore a compact summary of the consumer-side EOS
boundary as observed by the audit framework, not a full event-by-event trace.

## Current Scope

The wrapper is intentionally narrow and matches the current fault-injection
consumer flow.

Supported execution model:

1. `poll()` returns one batch.
2. The application processes that batch synchronously.
3. The application calls `commitSync()` once for that batch.

Out of scope:

- `commitAsync()` auditing. Async commits are forwarded to the underlying
  consumer, but no audit event is emitted for them.
- Multi-commit flows for a single polled batch.
- Overlapping in-flight batches or more complex offset-management strategies.

If a consumer does not follow the `poll -> synchronous processing -> commitSync`
pattern, this package may misattribute or miss audit transitions.

## Audit Events

Event | Pointcut
--- | ---
`BATCH_READ` | After `consumer.poll()` returns non-empty records
`OFFSET_COMMITTED` | After `consumer.commitSync()` returns successfully

`BATCH_READ` payload fields:

- Deterministic `eventId` derived from the batch contents
- `recordCount`
- `partitionRanges[]`, where each entry contains `partition`, `offsetMin`,
  `offsetMax`, and per-partition record count

## Integrating With The Consumer

Two integration changes are required.

### 1. Initialize the shared audit producer

Call `AuditProducer.init(...)` before entering the consumer poll loop, and
`AuditProducer.shutdown()` during shutdown.

```java
public static void main(String[] args) throws Exception {
    KafkaConsumerConfig config = KafkaConsumerConfig.load();

    try {
        dbConfig.verifyDatabaseConnection(config);
        AuditProducer.init(config.bootstrapServers, "audit-topic");
        runConsumer(config);
    } finally {
        dbConfig.close();
        AuditProducer.shutdown();
    }
}
```

### 2. Wrap the Kafka consumer

Replace the plain `KafkaConsumer` with `AuditableConsumer`.

```java
KafkaConsumer<String, String> consumer =
    new AuditableConsumer<>(
        new KafkaConsumer<>(props),
        config.topic,
        config.groupId
    );

consumer.subscribe(Collections.singletonList(config.topic));
```

## Running The Audit Aggregator

`AuditAggregator` is a Kafka Streams application that tracks pending batches and
emits lifecycle outcomes for deterministic batch fingerprints.

### Build

```bash
cd app
mvn clean package -DskipTests
```

### Run

```bash
export BOOTSTRAP_SERVERS=localhost:9092
export AUDIT_TOPIC=audit-topic
export TRANSACTION_TIMEOUT_MS=60000

java -cp target/kafka-perf-1.0.0.jar com.kafka.perf.audit.AuditAggregator
```

### Configuration

- `BOOTSTRAP_SERVERS` default: `localhost:9092`
- `AUDIT_TOPIC` default: `audit-topic`
- `TRANSACTION_TIMEOUT_MS` default: `60000`

### Output Topics

- `audit.outcomes` - Audit lifecycle outcomes currently including:
- `ESTIMATED_FAILED`
- `REPLAY_OBSERVED`
- `LATE_COMMIT`
- `COMMITTED`

## Notes

- The audit producer is intentionally best-effort and should be treated as an
  estimator, not a source of exact truth.
- The current implementation tracks one audited batch lifecycle at a time in the
  wrapper.
- The aggregator keeps timed-out entries in state long enough to reconcile a
  later commit as `LATE_COMMIT`.
- Replay is estimated by seeing the same deterministic batch fingerprint on a
  later `BATCH_READ`.

## Audit Outcomes Exporter

`AuditOutcomesExporter` consumes the final Kafka topic `audit.outcomes` and
exposes Prometheus counters over HTTP for Prometheus and Grafana.

### Environment Variables

- `BOOTSTRAP_SERVERS` default: `localhost:9092`
- `AUDIT_OUTCOMES_TOPIC` default: `audit.outcomes`
- `GROUP_ID` default: `audit-outcomes-exporter`
- `METRICS_PORT` default: `8085`

### Run

```bash
cd app
mvn clean package -DskipTests
mvn dependency:copy-dependencies -DincludeScope=runtime

export BOOTSTRAP_SERVERS=localhost:9092
export AUDIT_OUTCOMES_TOPIC=audit.outcomes
export METRICS_PORT=8085

java -cp "target/classes:target/dependency/*" com.kafka.perf.audit.AuditOutcomesExporter
```

### Exported Prometheus Metrics

- `audit_outcomes_total{outcome,consumer_group,source_topic}`
- `audit_replay_count_total{consumer_group,source_topic}`
- `audit_timeout_count_total{consumer_group,source_topic}`
- `audit_partition_outcomes_total{outcome,consumer_group,source_topic,partition}`
- `audit_batches_seen_total{consumer_group,source_topic}`

### Prometheus Scrape Configuration

Add a scrape target for the exporter in Prometheus. The repo default points to:

```yaml
- job_name: 'audit-outcomes-exporter'
  static_configs:
    - targets: ['audit-outcomes-exporter:8085']
      labels:
        app: 'audit-outcomes-exporter'
```

If the exporter runs on a different host, replace the target with the reachable
host and port for its `/metrics` endpoint.

## Viewing Metrics From Final Topics

The final topic to analyze is `audit.outcomes`. Each message represents a batch
lifecycle observation keyed by the deterministic batch fingerprint.

Useful metrics from `audit.outcomes`:

- EOS boundary failure estimate:
  count messages where `outcome = ESTIMATED_FAILED`
- Replay frequency:
  count messages where `outcome = REPLAY_OBSERVED`
- Late commit recovery:
  count messages where `outcome = LATE_COMMIT`
- Clean completion count:
  count messages where `outcome = COMMITTED`
- Replay severity:
  aggregate `replayCount` by `consumerGroup`, `sourceTopic`, or partition
- Timeout pressure:
  aggregate `timeoutCount` by time window

Useful dimensions to group by:

- `consumerGroup`
- `sourceTopic`
- `partitionRanges[].partition`
- event time fields such as `observedAt` or `firstSeenAt`

Practical ways to view them:

- Prometheus scraping `AuditOutcomesExporter`, then Grafana dashboards on top
- Kafka Connect sink to PostgreSQL, ClickHouse, or Elasticsearch for deeper
  offline analysis if needed
- Simple batch export from `audit.outcomes` into CSV or Parquet for paper plots

Suggested paper-ready metrics:

- `estimated_failure_rate = ESTIMATED_FAILED / BATCH_READ`
- `replay_rate = REPLAY_OBSERVED / BATCH_READ`
- `late_commit_ratio = LATE_COMMIT / ESTIMATED_FAILED`
- per-partition replay concentration based on `partitionRanges`

Example PromQL queries:

- Total outcomes by type:
  `sum by (outcome) (increase(audit_outcomes_total[5m]))`
- Outcomes by topic:
  `sum by (source_topic, outcome) (increase(audit_outcomes_total[5m]))`
- Replay counts by consumer group:
  `sum by (consumer_group) (increase(audit_replay_count_total[5m]))`
- Replay concentration by partition:
  `sum by (partition) (increase(audit_partition_outcomes_total{outcome="REPLAY_OBSERVED"}[15m]))`
- Estimated failure rate:
  `sum(increase(audit_outcomes_total{outcome="ESTIMATED_FAILED"}[15m])) / sum(increase(audit_batches_seen_total[15m]))`

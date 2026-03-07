
Event                      Pointcut
Batch read from Kafka     After consumer.poll() returns non-empty records
Offset committed          After consumer.commitSync() returns@After



// ─────────────────────────────────────────────────────────────────────────────
// Changes required in FaultInjectorConsumer.java  (2 locations only)
// ─────────────────────────────────────────────────────────────────────────────


// ── LOCATION 1:  main()  ─────────────────────────────────────────────────────
// Add AuditProducer init BEFORE runConsumer(), and shutdown in the finally block.

    public static void main(String[] args) throws Exception {
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        // ... existing code unchanged ...

        try {
            dbConfig.verifyDatabaseConnection(config);

            // ADD THIS ↓  (before runConsumer)
            AuditProducer.init(config.bootstrapServers, "audit-topic");

            runConsumer(config);
        } finally {
            dbConfig.close();
            AuditProducer.shutdown();   // ADD THIS ↑
        }
    }


// ── LOCATION 2:  runConsumer()  ──────────────────────────────────────────────
// Replace the plain KafkaConsumer construction with AuditableConsumer.
// Every other line in runConsumer() stays exactly as-is.

    // BEFORE:
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(config.topic));

    // AFTER:
    KafkaConsumer<String, String> consumer =
        new AuditableConsumer<>(
            new KafkaConsumer<>(props),   // real consumer passed as delegate
            config.topic,
            config.groupId
        );
    consumer.subscribe(Collections.singletonList(config.topic));   // unchanged

// That's it. poll(), commitSync(), close() are called identically —
// AuditableConsumer intercepts them transparently.



## Running the Audit Aggregator

The AuditAggregator is a Kafka Streams application that monitors and audits batch processing operations, tracking when batches are read and committed with timeout detection.

### Build

```bash
cd app
mvn clean package -DskipTests
```

### Run

Configure via environment variables and start:

```bash
export BOOTSTRAP_SERVERS=localhost:9092
export AUDIT_TOPIC=audit-topic
export TRANSACTION_TIMEOUT_MS=60000

java -cp target/kafka-perf-1.0.0.jar com.kafka.perf.audit.AuditAggregator
```

### Configuration

- **BOOTSTRAP_SERVERS** (default: `localhost:9092`) - Kafka bootstrap servers
- **AUDIT_TOPIC** (default: `audit-topic`) - Source topic containing audit events
- **TRANSACTION_TIMEOUT_MS** (default: `60000`) - Timeout in milliseconds for detecting failed batches

### Output Topics

The aggregator writes to two topics:

- **audit.committed** - Successful batch commits with latency metrics
- **audit.failed** - Failed batches that exceeded the timeout threshold

### Features

- Stateful tracking of pending batches using local state store
- Wall-clock time based failure detection (10-second punctuation intervals)
- JSON serialization using Jackson (already included in project dependencies)
- Automatic cleanup of expired entries
- Comprehensive logging via SLF4J/Logback

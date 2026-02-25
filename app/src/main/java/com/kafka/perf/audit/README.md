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
package com.kafka.perf.baseline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.perf.configs.DBConfig;
import com.kafka.perf.configs.KafkaConsumerConfig;

/**
 * PostgreSQL Sink Consumer - Identical to BaselineConsumer but writes to PostgreSQL.
 * 
 * Consumes messages from Kafka topic and writes them to PostgreSQL sink database.
 * Configuration is centralized in KafkaConsumerConfig.
 * Uses HikariCP connection pool for efficient connection management.
 * 
 * Database Schema Required:
 * CREATE TABLE sink_events (
 *     id SERIAL PRIMARY KEY,
 *     event_id VARCHAR(64),
 *     kafka_topic TEXT,
 *     kafka_partition INT,
 *     kafka_offset BIGINT,
 *     payload TEXT,
 *     created_at TIMESTAMP DEFAULT now()
 * );
 */
public class PostgresSinkConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PostgresSinkConsumer.class);
    
    // Database configuration (initialized during startup)
    private static DBConfig dbConfig = null;

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        logger.info("==== PostgreSQL Sink Consumer ====");
        logger.info("{}", config);

        if (config.enableAutoCommit) {
            throw new IllegalStateException(
                    "PostgresSinkConsumer requires consumer.enable.auto.commit=false to avoid acknowledging unpersisted records");
        }
        PostgresSinkStats stats = new PostgresSinkStats();

        // Initialize database connection pool
        dbConfig = new DBConfig("PostgresSinkConsumer");
        dbConfig.initializeConnectionPool(config);
        
        try {
            // Verify database connectivity before starting consumer
            dbConfig.verifyDatabaseConnection(config);

            // Run consumer
            runConsumer(config, stats);
        } finally {
            // Cleanup: close connection pool
            dbConfig.close();
        }
    }

    /**
     * Run the consumer loop - identical to BaselineConsumer but with PostgreSQL writes
     */
    private static void runConsumer(KafkaConsumerConfig config, PostgresSinkStats stats) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId);

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPollRecords);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.valueDeserializer);

        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, config.isolationLevel);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.enableAutoCommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.autoCommitIntervalMs);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.autoOffsetReset);

        // ===== THROUGHPUT TUNING (mirrors BaselineConsumer) =====
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, config.fetchMaxWaitMs);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.maxPartitionFetchBytes);

        // ===== SESSION & POLLING TUNING =====
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, config.heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.maxPollIntervalMs);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.topic));
        PostgresSinkWriter sinkWriter = new PostgresSinkWriter(dbConfig, config, stats, logger);

        long lastCommitTime = System.currentTimeMillis();
        Map<TopicPartition, Long> persistedOffsets = new HashMap<>();

        logger.info("Started consuming from topic: {}", config.topic);
        logger.info("Consumer Group: {}", config.groupId);
        logger.info("Bootstrap Servers: {}", config.bootstrapServers);

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs));

                List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> batch =
                        new ArrayList<>();
                for (var record : records.records(config.topic)) {
                    batch.add(record);
                }
                for (var record : records) {
                    stats.recordConsumed();
                }

                PostgresSinkWriteResult result = sinkWriter.writeBatch(batch);
                if (result != PostgresSinkWriteResult.SUCCESS) {
                    KafkaCommitUtils.commitPersistedOffsetsSync(consumer, persistedOffsets);
                    throw new IllegalStateException(String.format(
                            "Stopping consumer after sink write failure for polled batch from topic %s",
                            config.topic));
                }

                for (var record : batch) {
                    persistedOffsets.put(
                            new TopicPartition(record.topic(), record.partition()),
                            record.offset() + 1
                    );
                }

                // Periodic synchronous commit so the sink/offset boundary is explicit
                if (System.currentTimeMillis() - lastCommitTime >= config.autoCommitIntervalMs) {
                    KafkaCommitUtils.commitPersistedOffsetsSync(consumer, persistedOffsets);
                    lastCommitTime = System.currentTimeMillis();
                }

                // Log statistics periodically
                long currentTime = System.currentTimeMillis();
                if (currentTime - stats.getLastLogTime() >= (config.logIntervalSecs * 1000)) {
                    logStatistics(stats, currentTime);
                }
            }
        } finally {
            try {
                KafkaCommitUtils.commitPersistedOffsetsSync(consumer, persistedOffsets);
            } finally {
                consumer.close();
                logStatistics(stats, System.currentTimeMillis());
                logger.info("Consumer closed gracefully");
            }
        }
    }

    /**
     * Log consumption and write statistics
     */
    private static void logStatistics(PostgresSinkStats stats, long currentTime) {
        PostgresSinkStats.StatsSnapshot snapshot = stats.snapshot(currentTime);

        logger.info(
            "[{}] Total Consumed: {} | Total Written: {} | Write Errors: {} | Interval Consumed: {} | Interval Written: {} | Interval Throughput: {} msg/sec | Interval Write Rate: {} msg/sec | Lifetime Throughput: {} msg/sec | Lifetime Write Rate: {} msg/sec",
            System.currentTimeMillis(),
            snapshot.totalConsumed,
            snapshot.totalWritten,
            snapshot.totalWriteErrors,
            snapshot.intervalConsumed,
            snapshot.intervalWritten,
            snapshot.intervalConsumedRate,
            snapshot.intervalWriteRate,
            snapshot.lifetimeConsumedRate,
            snapshot.lifetimeWriteRate
        );

        stats.resetInterval();
        stats.markLogTime(currentTime);
    }
}

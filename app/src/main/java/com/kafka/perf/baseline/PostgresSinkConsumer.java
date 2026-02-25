package com.kafka.perf.baseline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

    // Statistics
    private static long totalMessagesConsumed = 0;
    private static long totalMessagesWritten = 0;
    private static long totalWriteErrors = 0;
    private static long lastLogTime = System.currentTimeMillis();
    
    // Database configuration (initialized during startup)
    private static DBConfig dbConfig = null;

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        logger.info("==== PostgreSQL Sink Consumer ====");
        logger.info("{}", config);

        // Initialize database connection pool
        dbConfig = new DBConfig("PostgresSinkConsumer");
        dbConfig.initializeConnectionPool(config);
        
        try {
            // Verify database connectivity before starting consumer
            dbConfig.verifyDatabaseConnection(config);

            // Run consumer
            runConsumer(config);
        } finally {
            // Cleanup: close connection pool
            dbConfig.close();
        }
    }

    /**
     * Write message to PostgreSQL sink with retry logic
     */
    private static void writeToSink(KafkaConsumerConfig config, String topic, int partition, long offset, String key, String value) {
        String eventId = UUID.randomUUID().toString();
        String sql = String.format(
            "INSERT INTO %s (event_id, kafka_topic, kafka_partition, kafka_offset, payload) VALUES (?, ?, ?, ?, ?)",
            config.dbSinkTable
        );

        int maxRetries = 3;
        int retryCount = 0;
        long backoffMs = 50;

        while (retryCount < maxRetries) {
            try (Connection conn = dbConfig.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                stmt.setString(1, eventId);
                stmt.setString(2, topic);
                stmt.setInt(3, partition);
                stmt.setLong(4, offset);
                stmt.setString(5, value);

                stmt.executeUpdate();
                totalMessagesWritten++;
                return; // Success

            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    totalWriteErrors++;
                    logger.error("[ERROR] Failed to write message (offset={}) after {} attempts: {}",
                        offset, maxRetries, e.getMessage());
                    return; // Give up
                }
                logger.warn("[WARN] Failed to write message (offset={}), attempt {}/{}: {}. Retrying...",
                    offset, retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    totalWriteErrors++;
                    logger.error("[ERROR] Write interrupted (offset={}): {}", offset, ie.getMessage());
                    return;
                }
            }
        }
    }

    /**
     * Run the consumer loop - identical to BaselineConsumer but with PostgreSQL writes
     */
    private static void runConsumer(KafkaConsumerConfig config) throws Exception {

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

        long lastCommitTime = System.currentTimeMillis();
        long recordsInBatch = 0;

        logger.info("Started consuming from topic: {}", config.topic);
        logger.info("Consumer Group: {}", config.groupId);
        logger.info("Bootstrap Servers: {}", config.bootstrapServers);

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs));

                // Write each record to PostgreSQL
                for (var record : records) {
                    writeToSink(
                        config,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        record.value()
                    );
                    totalMessagesConsumed++;
                    recordsInBatch++;
                }

                // Periodic async commit (non-blocking)
                if (!config.enableAutoCommit &&
                        System.currentTimeMillis() - lastCommitTime >= config.autoCommitIntervalMs) {
                    consumer.commitAsync();
                    lastCommitTime = System.currentTimeMillis();
                }

                // Log statistics periodically
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime >= (config.logIntervalSecs * 1000)) {
                    logStatistics();
                    lastLogTime = currentTime;
                    recordsInBatch = 0;
                }
            }
        } finally {
            try {
                if (!config.enableAutoCommit) {
                    consumer.commitSync(); // final safe commit
                }
            } finally {
                consumer.close();
                logStatistics();
                logger.info("Consumer closed gracefully");
            }
        }
    }

    /**
     * Log consumption and write statistics
     */
    private static void logStatistics() {
        long currentTime = System.currentTimeMillis();
        double elapsedSecs = (currentTime - lastLogTime) / 1000.0;
        double throughputMsgSec = totalMessagesConsumed > 0 ? totalMessagesConsumed / elapsedSecs : 0;
        double writeThroughput = totalMessagesWritten > 0 ? totalMessagesWritten / elapsedSecs : 0;

        logger.info("[{}] Consumed: {} | Written: {} | Write Errors: {} | Throughput: {:.2f} msg/sec | Write Rate: {:.2f} msg/sec",
            System.currentTimeMillis(),
            totalMessagesConsumed,
            totalMessagesWritten,
            totalWriteErrors,
            throughputMsgSec,
            writeThroughput
        );
    }
}

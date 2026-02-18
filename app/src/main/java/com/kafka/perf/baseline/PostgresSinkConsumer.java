package com.kafka.perf.baseline;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

    // Statistics
    private static long totalMessagesConsumed = 0;
    private static long totalMessagesWritten = 0;
    private static long totalWriteErrors = 0;
    private static long lastLogTime = System.currentTimeMillis();
    
    // Connection pool (initialized during startup)
    private static HikariDataSource dataSource = null;

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        System.out.println("==== PostgreSQL Sink Consumer ====");
        System.out.println(config);

        // Initialize connection pool
        initializeConnectionPool(config);
        
        try {
            // Verify database connectivity before starting consumer
            verifyDatabaseConnection(config);

            // Run consumer
            runConsumer(config);
        } finally {
            // Cleanup: close connection pool
            if (dataSource != null && !dataSource.isClosed()) {
                System.out.println("[PostgresSinkConsumer] Closing connection pool...");
                dataSource.close();
            }
        }
    }

    /**
     * Initialize HikariCP connection pool with optimized settings
     */
    private static void initializeConnectionPool(KafkaConsumerConfig config) throws Exception {
        System.out.println("[PostgresSinkConsumer] Initializing HikariCP connection pool...");
        
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl);
        hikariConfig.setUsername(config.dbUser);
        hikariConfig.setPassword(config.dbPassword);
        
        // Connection pool settings
        hikariConfig.setMaximumPoolSize(config.dbConnectionPoolSize);
        hikariConfig.setMinimumIdle(2); // Keep minimum 2 idle connections
        hikariConfig.setConnectionTimeout(10000); // 10s timeout for acquiring connection
        hikariConfig.setIdleTimeout(600000); // 10 minutes idle timeout
        hikariConfig.setMaxLifetime(1800000); // 30 minutes max lifetime
        hikariConfig.setAutoCommit(true); // Auto-commit for simple writes
        
        // Connection test query for health checks
        hikariConfig.setConnectionTestQuery("SELECT 1");
        
        dataSource = new HikariDataSource(hikariConfig);
        System.out.println("[PostgresSinkConsumer] ✓ Connection pool initialized (max size: " + config.dbConnectionPoolSize + ")");
    }

    /**
     * Verify PostgreSQL connection and table existence
     */
    private static void verifyDatabaseConnection(KafkaConsumerConfig config) throws Exception {
        System.out.println("[PostgresSinkConsumer] Verifying database connection...");
        System.out.println("[PostgresSinkConsumer] Attempting to connect to: " + config.dbUrl);
        
        int maxRetries = 15;
        int retryCount = 0;
        long backoffMs = 300;
        long verifyStartTime = System.currentTimeMillis();
        
        while (retryCount < maxRetries) {
            try (Connection conn = dataSource.getConnection()) {
                long elapsedMs = System.currentTimeMillis() - verifyStartTime;
                System.out.println("[PostgresSinkConsumer] ✓ Database connection successful (took " + elapsedMs + "ms)");
                System.out.println("[PostgresSinkConsumer] Database URL: " + config.dbUrl);
                System.out.println("[PostgresSinkConsumer] Table: " + config.dbSinkTable);
                System.out.println("[PostgresSinkConsumer] PostgreSQL version: " + conn.getMetaData().getDatabaseProductVersion());
                System.out.println("[PostgresSinkConsumer] Connection pool ready");
                return;
            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    System.err.println("[PostgresSinkConsumer] ✗ Database connection failed after " + maxRetries + " attempts");
                    System.err.println("[PostgresSinkConsumer] Error: " + e.getMessage());
                    System.err.println("[PostgresSinkConsumer] Ensure PostgreSQL is running at: " + config.dbUrl);
                    System.err.println("[PostgresSinkConsumer] Credentials - User: " + config.dbUser + " (password configured)");
                    throw e;
                }
                long elapsedMs = System.currentTimeMillis() - verifyStartTime;
                System.err.printf("[PostgresSinkConsumer] Connection attempt %d/%d failed (%dms elapsed): %s%n", 
                    retryCount, maxRetries, elapsedMs, e.getMessage());
                System.err.printf("[PostgresSinkConsumer] Retrying in %dms...%n", backoffMs);
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new Exception("Connection verification interrupted", ie);
                }
            }
        }
    }

    /**
     * Get database connection from pool with retry logic
     */
    private static Connection getConnection(KafkaConsumerConfig config) throws SQLException {
        int maxRetries = 3;
        int retryCount = 0;
        long backoffMs = 100;
        
        while (retryCount < maxRetries) {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    throw new SQLException("Failed to get connection from pool after " + maxRetries + " attempts: " + e.getMessage(), e);
                }
                System.err.printf("[WARN] Failed to get pooled connection, attempt %d/%d: %s%n", 
                    retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection retry interrupted", ie);
                }
            }
        }
        throw new SQLException("Failed to get connection from pool");
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
            try (Connection conn = getConnection(config);
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
                    System.err.printf("[ERROR] Failed to write message (offset=%d) after %d attempts: %s%n", 
                        offset, maxRetries, e.getMessage());
                    return; // Give up
                }
                System.err.printf("[WARN] Failed to write message (offset=%d), attempt %d/%d: %s. Retrying...%n",
                    offset, retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    totalWriteErrors++;
                    System.err.printf("[ERROR] Write interrupted (offset=%d): %s%n", offset, ie.getMessage());
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

        System.out.println("[PostgresSinkConsumer] Started consuming from topic: " + config.topic);
        System.out.println("[PostgresSinkConsumer] Consumer Group: " + config.groupId);
        System.out.println("[PostgresSinkConsumer] Bootstrap Servers: " + config.bootstrapServers);

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
                System.out.println("[PostgresSinkConsumer] Consumer closed gracefully");
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

        System.out.printf(
            "[%s] Consumed: %d | Written: %d | Write Errors: %d | Throughput: %.2f msg/sec | Write Rate: %.2f msg/sec%n",
            System.currentTimeMillis(),
            totalMessagesConsumed,
            totalMessagesWritten,
            totalWriteErrors,
            throughputMsgSec,
            writeThroughput
        );
    }
}

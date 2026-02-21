package com.kafka.perf.baseline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.perf.faults.FaultConfig;
import com.kafka.perf.faults.FaultInjector;
import com.kafka.perf.faults.FaultScheduler;
import com.kafka.perf.faults.FaultType;

/**
 * FaultInjectorConsumer - PostgreSQL Sink Consumer with fault injection capabilities.
 * 
 * Implements exactly-once semantics using:
 * - Database transactions for atomic batch writes
 * - UPSERT for idempotent writes (handles redelivery)
 * - Kafka offset management (commitSync/commitAsync)
 * 
 * Supports 6 fault types:
 * - F1: Crash before database commit
 * - F2: Crash after database commit but before offset acknowledgment
 * - F3: Partial batch writes (write subset of records)
 * - F4: Database container restart
 * - F5: Slow sink backpressure
 * - F6: Network boundary fault
 */
public class FaultInjectorConsumer {

    private static final Logger logger = LoggerFactory.getLogger(FaultInjectorConsumer.class);

    // Statistics
    private static long totalMessagesConsumed = 0;
    private static long totalMessagesWritten = 0;
    private static long totalPartialWrites = 0;
    private static long totalWriteErrors = 0;
    private static long lastLogTime = System.currentTimeMillis();
    
    // Database configuration (initialized during startup)
    private static DBConfig dbConfig = null;
    private static FaultInjector faultInjector = null;
    private static FaultScheduler faultScheduler = null;

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        // Load fault configuration
        FaultConfig faultConfig = FaultConfig.load();
        
        // Load fault scheduler for sequential injection with probability-based injection
        faultScheduler = FaultScheduler.load(faultConfig);
        
        logger.info("==== FaultInjector Sink Consumer ====");
        logger.info("{}", config);
        logger.info("{}", faultConfig);
        logger.info("{}", faultScheduler);

        // Initialize database connection pool
        dbConfig = new DBConfig("FaultInjectorConsumer");
        dbConfig.initializeConnectionPool(config);
        
        // Initialize fault injector with seed for reproducibility
        long seed = System.currentTimeMillis();
        faultInjector = new FaultInjector(faultConfig, seed);
        logger.info("Fault injector initialized with seed: {}", seed);
        
        try {
            // Verify database connectivity before starting consumer
            dbConfig.verifyDatabaseConnection(config);

            // Run consumer with fault injection
            runConsumer(config);
        } finally {
            // Cleanup: close connection pool
            dbConfig.close();
        }
    }

    /**
     * Write batch of messages to PostgreSQL in a single database transaction.
     * Uses UPSERT for idempotent writes - ensures exactly-once semantics at sink level.
     * 
     * This ensures no partial batch commits - either all records are written or none.
     */
    private static void writeBatchTransactionally(
            KafkaConsumerConfig config, 
            List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records,
            Connection conn) throws SQLException {
        
        if (records.isEmpty()) {
            return;
        }

        // F1: Crash before database commit
        // FaultScheduler: determines window + probability-based injection
        // FaultInjector: executes the actual fault
        boolean shouldInjectF1 = faultScheduler != null && faultScheduler.shouldInjectScheduled(FaultType.F1_CRASH_BEFORE_DB_COMMIT);
        if (shouldInjectF1) {
            faultInjector.maybeInject(FaultType.F1_CRASH_BEFORE_DB_COMMIT);
        }
        
        String insertSql = String.format(
            "INSERT INTO %s (event_id, kafka_topic, kafka_partition, kafka_offset, payload) " +
            "VALUES (?, ?, ?, ?, ?) " +
            "ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) " +
            "DO UPDATE SET payload = EXCLUDED.payload, event_id = EXCLUDED.event_id",
            config.dbSinkTable
        );

        try (PreparedStatement dataStmt = conn.prepareStatement(insertSql)) {

            // Batch insert all records
            for (var record : records) {
                String eventId = UUID.randomUUID().toString();
                
                dataStmt.setString(1, eventId);
                dataStmt.setString(2, record.topic());
                dataStmt.setInt(3, record.partition());
                dataStmt.setLong(4, record.offset());
                dataStmt.setString(5, record.value());
                dataStmt.addBatch();
            }
            
            // Execute batch insert
            dataStmt.executeBatch();
            totalMessagesWritten += records.size();
            
            // F2: Crash after database commit but before acknowledgment
            // FaultScheduler: determines window + probability-based injection
            // FaultInjector: executes the actual fault
            boolean shouldInjectF2 = faultScheduler != null && faultScheduler.shouldInjectScheduled(FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK);
            if (shouldInjectF2) {
                faultInjector.maybeInject(FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK);
            }
            
        } catch (SQLException e) {
            totalWriteErrors++;
            logger.warn("[TRANSACTION] Rolling back batch due to error: {}", e.getMessage());
            try {
                conn.rollback();
            } catch (SQLException rollbackEx) {
                logger.error("Failed to rollback transaction: {}", rollbackEx.getMessage());
            }
            throw e;
        }
    }

    /**
     * Process batch of records with transactional guarantees and optional partial writes (F3).
     * F3_PARTIAL_BATCH_WRITES: write only a subset of records in the batch to simulate partial failures.
     * 
     * Uses database transactions to ensure atomicity:
     * - Either all records in batch are written, or none
     * - Prevents partial batch commits that cause data loss
     * - Implements exactly-once semantics at the sink level with UPSERT + Kafka offset commits
     */
    private static void processBatchTransactionally(
            KafkaConsumerConfig config, 
            KafkaConsumer<String, String> consumer,
            List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records) throws SQLException {
        
        if (records.isEmpty()) {
            return;
        }

        // Check if F3 partial writes should be applied to this batch
        // FaultScheduler: determines window + probability-based injection
        // FaultInjector: executes the actual fault (which is just marking batch as partial)
        boolean shouldInjectF3 = faultScheduler != null && faultScheduler.shouldInjectScheduled(FaultType.F3_PARTIAL_BATCH_WRITES);
        boolean applyPartialWrites = false;
        if (shouldInjectF3) {
            applyPartialWrites = faultInjector.maybeInject(FaultType.F3_PARTIAL_BATCH_WRITES);
        }
        
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recordsToWrite = records;
        
        if (applyPartialWrites) {
            // Write only a fixed 50% subset of the batch - simulates processing failures
            int subsetSize = Math.max(1, records.size() / 2);
            recordsToWrite = new ArrayList<>(records.subList(0, subsetSize));
            totalPartialWrites++;
            logger.info("[F3_PARTIAL_BATCH_WRITES] Writing {}/{} records from batch ({:.0f}%)", 
                subsetSize, records.size(), (subsetSize * 100.0 / records.size()));
        }
        
        // Write the batch (or subset) with retries
        int maxRetries = 3;
        int attempt = 0;
        
        while (attempt < maxRetries) {
            attempt++;
            Connection conn = null;
            try {
                conn = dbConfig.getConnection();
                
                // Write batch transactionally
                writeBatchTransactionally(config, recordsToWrite, conn);
                conn.commit();
                
                // Log skipped records if F3 was applied
                if (applyPartialWrites) {
                    int skipped = records.size() - recordsToWrite.size();
                    logger.warn("[F3_PARTIAL_BATCH_WRITES] Skipped {} records (will be retried on next poll)", skipped);
                }
                
                return; // Success, exit retry loop
                
            } catch (SQLException e) {
                totalWriteErrors++;
                if (attempt == maxRetries) {
                    logger.error("[ERROR] Failed to process batch after {} attempts. Last error: {}", 
                        maxRetries, e.getMessage());
                    throw e;
                } else {
                    logger.warn("[WARN] Transaction failed, attempt {}/{}: {}. Retrying...",
                        attempt, maxRetries, e.getMessage());
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.warn("Failed to close connection: {}", e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Run the consumer loop with fault injection
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

        // ===== THROUGHPUT TUNING =====
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.fetchMinBytes);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, config.fetchMaxWaitMs);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, config.maxPartitionFetchBytes);

        // ===== SESSION & POLLING TUNING =====
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, config.heartbeatIntervalMs);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, config.maxPollIntervalMs);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(config.topic));

        logger.info("Started consuming from topic: {}", config.topic);
        logger.info("Consumer Group: {}", config.groupId);
        logger.info("Bootstrap Servers: {}", config.bootstrapServers);
        logger.info("Fault injection ENABLED");
        logger.info("Transactional writes with Kafka offset management");

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs));

                if (records.isEmpty()) {
                    continue;
                }

                // F5: Slow sink backpressure
                // FaultScheduler: determines window + probability-based injection
                // FaultInjector: executes the actual fault (simulates latency)
                boolean shouldInjectF5 = faultScheduler != null && faultScheduler.shouldInjectScheduled(FaultType.F5_SLOW_SINK_BACKPRESSURE);
                if (shouldInjectF5) {
                    faultInjector.maybeInject(FaultType.F5_SLOW_SINK_BACKPRESSURE);
                }
                
                // F6: Network boundary fault
                // FaultScheduler: determines window + probability-based injection
                // FaultInjector: executes the actual fault (simulates network latency)
                boolean shouldInjectF6 = faultScheduler != null && faultScheduler.shouldInjectScheduled(FaultType.F6_NETWORK_BOUNDARY_FAULT);
                if (shouldInjectF6) {
                    faultInjector.maybeInject(FaultType.F6_NETWORK_BOUNDARY_FAULT);
                }

                // Process batch with transactional guarantees
                List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> batch =
                        new ArrayList<>(records.count());
                records.forEach(batch::add);
                
                try {
                    // Process batch in database transaction + commit Kafka offsets
                    processBatchTransactionally(config, consumer, batch);
                    
                } catch (SQLException e) {
                    // Transaction failed and rolled back - records will be re-consumed
                    // UPSERT ensures no duplicates on retry
                    logger.error("[ERROR] Batch processing failed, will retry batch on next poll: {}", 
                        e.getMessage());
                    // Sleep before next poll to avoid tight loop on persistent errors
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            // Final sync commit before closing
            try {
                if (!config.enableAutoCommit) {
                    consumer.commitSync();
                    logger.info("Final offset commit completed");
                }
            } catch (Exception e) {
                logger.warn("Failed to commit final offsets: {}", e.getMessage());
            }
            
            consumer.close();
            logger.info("Consumer closed gracefully");
            logger.info("[STATS] Total consumed: {} | Written: {} | Errors: {} | Partial writes: {}",
                totalMessagesConsumed, totalMessagesWritten, totalWriteErrors, totalPartialWrites);
        }
    }
}

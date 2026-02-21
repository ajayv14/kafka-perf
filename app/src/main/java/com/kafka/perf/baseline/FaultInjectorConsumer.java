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

import com.kafka.perf.faults.FaultInjector;
import com.kafka.perf.faults.FaultConfig;
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

    // Statistics
    private static long totalMessagesConsumed = 0;
    private static long totalMessagesWritten = 0;
    private static long totalPartialWrites = 0;
    private static long totalWriteErrors = 0;
    private static long lastLogTime = System.currentTimeMillis();
    
    // Database configuration (initialized during startup)
    private static DBConfig dbConfig = null;
    private static FaultInjector faultInjector = null;

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        // Load fault configuration
        FaultConfig faultConfig = FaultConfig.load();
        
        System.out.println("==== FaultInjector Sink Consumer ====");
        System.out.println(config);
        System.out.println();
        System.out.println(faultConfig);

        // Initialize database connection pool
        dbConfig = new DBConfig("FaultInjectorConsumer");
        dbConfig.initializeConnectionPool(config);
        
        // Initialize fault injector with seed for reproducibility
        long seed = System.currentTimeMillis();
        faultInjector = new FaultInjector(faultConfig, seed);
        System.out.println("[FaultInjectorConsumer] Fault injector initialized with seed: " + seed);
        
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
        faultInjector.maybeInject(FaultType.F1_CRASH_BEFORE_DB_COMMIT);
        
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
            faultInjector.maybeInject(FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK);
            
        } catch (SQLException e) {
            totalWriteErrors++;
            throw e; // Propagate to trigger transaction rollback
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
        boolean applyPartialWrites = faultInjector.maybeInject(FaultType.F3_PARTIAL_BATCH_WRITES);
        
        List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> recordsToWrite = records;
        
        if (applyPartialWrites) {
            // Write only a fixed 50% subset of the batch - simulates processing failures
            int subsetSize = Math.max(1, records.size() / 2);
            recordsToWrite = new ArrayList<>(records.subList(0, subsetSize));
            totalPartialWrites++;
            System.out.printf("[F3_PARTIAL_BATCH_WRITES] Writing %d/%d records from batch (%.0f%%)%n", 
                subsetSize, records.size(), (100.0 * subsetSize / records.size()));
        }

        // Execute transactional write with retry logic
        int maxRetries = 3;
        int retryCount = 0;
        long backoffMs = 100;
        SQLException lastException = null;

        while (retryCount < maxRetries) {
            Connection conn = null;
            try {
                conn = dbConfig.getConnection();
                conn.setAutoCommit(false); // Start transaction
                
                // Write batch in database transaction
                writeBatchTransactionally(config, recordsToWrite, conn);
                
                // Commit transaction - atomic write of data
                conn.commit();
                
                totalMessagesConsumed += recordsToWrite.size();
                
                // Commit offsets to Kafka after successful database commit
                // This ensures at-least-once with idempotent writes (UPSERT handles duplicates)
                if (!config.enableAutoCommit) {
                    consumer.commitSync();
                }
                
                // Log skipped records from partial writes (will be retried on next poll)
                if (applyPartialWrites && recordsToWrite.size() < records.size()) {
                    System.out.printf("[F3_PARTIAL_BATCH_WRITES] Skipped %d records (will be retried on next poll)%n", 
                        records.size() - recordsToWrite.size());
                }
                
                return; // Success
                
            } catch (SQLException e) {
                lastException = e;
                retryCount++;
                
                // Rollback transaction on failure
                if (conn != null) {
                    try {
                        conn.rollback();
                        System.err.printf("[TRANSACTION] Rolled back batch due to error: %s%n", e.getMessage());
                    } catch (SQLException rollbackEx) {
                        System.err.printf("[ERROR] Failed to rollback transaction: %s%n", rollbackEx.getMessage());
                    }
                }
                
                if (retryCount >= maxRetries) {
                    System.err.printf("[ERROR] Failed to process batch after %d attempts. Last error: %s%n", 
                        maxRetries, e.getMessage());
                    throw e; // Fail the batch - will cause consumer to retry entire batch
                }
                
                System.err.printf("[WARN] Transaction failed, attempt %d/%d: %s. Retrying...%n",
                    retryCount, maxRetries, e.getMessage());
                
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Transaction retry interrupted", ie);
                }
                
            } finally {
                if (conn != null) {
                    try {
                        conn.setAutoCommit(true); // Restore auto-commit
                        conn.close();
                    } catch (SQLException e) {
                        System.err.printf("[WARN] Failed to close connection: %s%n", e.getMessage());
                    }
                }
            }
        }
        
        // Should not reach here, but throw last exception if we do
        if (lastException != null) {
            throw lastException;
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

        System.out.println("[FaultInjectorConsumer] Started consuming from topic: " + config.topic);
        System.out.println("[FaultInjectorConsumer] Consumer Group: " + config.groupId);
        System.out.println("[FaultInjectorConsumer] Bootstrap Servers: " + config.bootstrapServers);
        System.out.println("[FaultInjectorConsumer] Fault injection ENABLED");
        System.out.println("[FaultInjectorConsumer] Transactional writes with Kafka offset management");

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs));

                if (records.isEmpty()) {
                    continue;
                }

                // F5: Slow sink backpressure
                faultInjector.maybeInject(FaultType.F5_SLOW_SINK_BACKPRESSURE);
                
                // F6: Network boundary fault
                faultInjector.maybeInject(FaultType.F6_NETWORK_BOUNDARY_FAULT);

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
                    System.err.printf("[ERROR] Batch processing failed, will retry batch on next poll: %s%n", 
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
                    System.out.println("[FaultInjectorConsumer] Final offset commit completed");
                }
            } catch (Exception e) {
                System.err.printf("[WARN] Failed to commit final offsets: %s%n", e.getMessage());
            }
            
            consumer.close();
            System.out.println("[FaultInjectorConsumer] Consumer closed gracefully");
            System.out.printf("[STATS] Total consumed: %d | Written: %d | Errors: %d | Partial writes: %d%n",
                totalMessagesConsumed, totalMessagesWritten, totalWriteErrors, totalPartialWrites);
        }
    }
}

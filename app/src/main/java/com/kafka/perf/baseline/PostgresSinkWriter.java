package com.kafka.perf.baseline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;

import com.kafka.perf.configs.DBConfig;
import com.kafka.perf.configs.KafkaConsumerConfig;

final class PostgresSinkWriter {

    private final DBConfig dbConfig;
    private final KafkaConsumerConfig config;
    private final PostgresSinkStats stats;
    private final Logger logger;

    PostgresSinkWriter(DBConfig dbConfig, KafkaConsumerConfig config, PostgresSinkStats stats, Logger logger) {
        this.dbConfig = dbConfig;
        this.config = config;
        this.stats = stats;
        this.logger = logger;
    }

    PostgresSinkWriteResult writeBatch(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return PostgresSinkWriteResult.SUCCESS;
        }

        int maxRetries = 3;
        int retryCount = 0;
        long backoffMs = 50;

        while (retryCount < maxRetries) {
            try (Connection conn = dbConfig.getConnection()) {
                conn.setAutoCommit(false);

                try (PreparedStatement stmt = conn.prepareStatement(insertSql())) {
                    for (ConsumerRecord<String, String> record : records) {
                        stmt.setString(1, UUID.randomUUID().toString());
                        stmt.setString(2, record.topic());
                        stmt.setInt(3, record.partition());
                        stmt.setLong(4, record.offset());
                        stmt.setString(5, record.value());
                        stmt.addBatch();
                    }

                    stmt.executeBatch();
                    conn.commit();
                } catch (SQLException e) {
                    rollbackQuietly(conn);
                    throw e;
                }

                for (int i = 0; i < records.size(); i++) {
                    stats.recordWriteSuccess();
                }
                return PostgresSinkWriteResult.SUCCESS;

            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    stats.recordWriteFailure();
                    ConsumerRecord<String, String> lastRecord = records.get(records.size() - 1);
                    logger.error(
                            "[ERROR] Failed to write sink batch after {} attempts. batchSize={} lastRecord={}-{}@{} error={}. Offsets past this batch will not be committed.",
                            maxRetries,
                            records.size(),
                            lastRecord.topic(),
                            lastRecord.partition(),
                            lastRecord.offset(),
                            e.getMessage());
                    return PostgresSinkWriteResult.FAILED_PERMANENTLY;
                }
                logger.warn("[WARN] Failed to write sink batch of {} records, attempt {}/{}: {}. Retrying...",
                        records.size(), retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    stats.recordWriteFailure();
                    ConsumerRecord<String, String> lastRecord = records.get(records.size() - 1);
                    logger.error(
                            "[ERROR] Sink batch write interrupted during retry sleep. batchSize={} lastRecord={}-{}@{} attempts={} error={}. Offsets past this batch will not be committed.",
                            records.size(),
                            lastRecord.topic(),
                            lastRecord.partition(),
                            lastRecord.offset(),
                            retryCount,
                            ie.getMessage());
                    return PostgresSinkWriteResult.FAILED_PERMANENTLY;
                }
            }
        }

        stats.recordWriteFailure();
        logger.error("[ERROR] Sink batch write reached unexpected terminal state. Offsets past this batch will not be committed.");
        return PostgresSinkWriteResult.FAILED_PERMANENTLY;
    }

    private String insertSql() {
        return String.format(
                "INSERT INTO %s (event_id, kafka_topic, kafka_partition, kafka_offset, payload) VALUES (?, ?, ?, ?, ?)",
                config.dbSinkTable
        );
    }

    private void rollbackQuietly(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException rollbackEx) {
            logger.error("Failed to rollback sink batch transaction: {}", rollbackEx.getMessage());
        }
    }
}

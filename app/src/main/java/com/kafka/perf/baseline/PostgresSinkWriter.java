package com.kafka.perf.baseline;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

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

    PostgresSinkWriteResult write(String topic, int partition, long offset, String key, String value) {
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
                stats.recordWriteSuccess();
                return PostgresSinkWriteResult.SUCCESS;

            } catch (SQLException e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    stats.recordWriteFailure();
                    logger.error(
                            "[ERROR] Failed to write message to sink after {} attempts. topic={} partition={} offset={} error={}. Offsets past this record will not be committed.",
                            maxRetries,
                            topic,
                            partition,
                            offset,
                            e.getMessage());
                    return PostgresSinkWriteResult.FAILED_PERMANENTLY;
                }
                logger.warn("[WARN] Failed to write message (offset={}), attempt {}/{}: {}. Retrying...",
                        offset, retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, 500);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    stats.recordWriteFailure();
                    logger.error(
                            "[ERROR] Write interrupted during retry sleep. topic={} partition={} offset={} attempts={} error={}. Offsets past this record will not be committed.",
                            topic,
                            partition,
                            offset,
                            retryCount,
                            ie.getMessage());
                    return PostgresSinkWriteResult.FAILED_PERMANENTLY;
                }
            }
        }

        stats.recordWriteFailure();
        logger.error(
                "[ERROR] Sink write reached unexpected terminal state. topic={} partition={} offset={}. Offsets past this record will not be committed.",
                topic,
                partition,
                offset);
        return PostgresSinkWriteResult.FAILED_PERMANENTLY;
    }
}

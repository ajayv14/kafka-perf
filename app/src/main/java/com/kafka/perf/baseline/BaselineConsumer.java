package com.kafka.perf.baseline;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Baseline Kafka Consumer - Simple consumer without performance measurement overhead.
 * Configuration is centralized in KafkaConsumerConfig.
 */
public class BaselineConsumer {

    public static void main(String[] args) throws Exception {
        
        // Load configuration from centralized config class
        KafkaConsumerConfig config = KafkaConsumerConfig.load();
        
        System.out.println("==== Baseline Consumer ====");
        System.out.println(config);

        // Run consumer
        runConsumer(config);
    }

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

        long lastCommitTime = System.currentTimeMillis();

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(config.pollTimeoutMs));

                // Minimal processing overhead (benchmark mode)
                //int recordCount = records.count();

                // Periodic async commit (non-blocking)
                if (!config.enableAutoCommit &&
                        System.currentTimeMillis() - lastCommitTime >= config.autoCommitIntervalMs) {
                    consumer.commitAsync();
                    lastCommitTime = System.currentTimeMillis();
                }
            }
        } finally {
            try {
                if (!config.enableAutoCommit) {
                    consumer.commitSync(); // final safe commit
                }
            } finally {
                consumer.close();
            }
        }
    }

}
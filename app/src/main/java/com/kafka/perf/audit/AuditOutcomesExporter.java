package com.kafka.perf.audit;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;

public class AuditOutcomesExporter {

    private static final Logger logger = LoggerFactory.getLogger(AuditOutcomesExporter.class);
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final KafkaConsumer<String, String> consumer;
    private final AuditOutcomesMetrics metrics;

    public AuditOutcomesExporter(KafkaConsumer<String, String> consumer, AuditOutcomesMetrics metrics) {
        this.consumer = consumer;
        this.metrics = metrics;
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record.value(), metrics);
            }
        }
    }

    static boolean processRecord(String json, AuditOutcomesMetrics metrics) {
        try {
            AuditOutcomeEvent event = AuditOutcomeEvent.fromJson(json);
            metrics.record(event);
            return true;
        } catch (IllegalArgumentException e) {
            logger.warn("Skipping malformed audit.outcomes message: {}", e.getMessage());
            logger.debug("Malformed payload: {}", json);
            return false;
        }
    }

    private static KafkaConsumer<String, String> buildConsumer(
            String bootstrapServers,
            String groupId,
            String topic,
            String autoOffsetReset) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = System.getenv().getOrDefault("AUDIT_OUTCOMES_TOPIC", "audit.outcomes");
        String groupId = System.getenv().getOrDefault("GROUP_ID", "audit-outcomes-exporter");
        int metricsPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "8085"));
        String autoOffsetReset = System.getenv().getOrDefault("AUTO_OFFSET_RESET", "earliest");

        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        AuditOutcomesMetrics metrics = new AuditOutcomesMetrics(registry);
        KafkaConsumer<String, String> consumer = buildConsumer(bootstrapServers, groupId, topic, autoOffsetReset);
        HTTPServer httpServer = new HTTPServer(new InetSocketAddress(metricsPort), registry, true);
        AuditOutcomesExporter exporter = new AuditOutcomesExporter(consumer, metrics);

        logger.info("AuditOutcomesExporter started topic={} bootstrapServers={} metricsPort={} autoOffsetReset={}",
            topic, bootstrapServers, metricsPort, autoOffsetReset);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down AuditOutcomesExporter...");
            try {
                consumer.wakeup();
            } catch (Exception ignored) {
            }
            consumer.close();
            httpServer.close();
        }));

        try {
            exporter.run();
        } finally {
            consumer.close();
            httpServer.close();
        }
    }
}

package com.kafka.perf.audit;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread-safe singleton Kafka producer that publishes AuditRecord messages
 * to the configured audit topic.
 *
 * Design constraints
 * ------------------
 * - Fire-and-forget (async send with error callback) — audit must NEVER block
 *   or throw into the main consumer pipeline.
 * - A single shared producer instance is reused for the lifetime of the consumer
 *   process — creating a producer per audit event would be far too expensive.
 * 
 *
 * Lifecycle
 * ---------
 *   // At consumer startup, before the poll loop:
 *   AuditProducer.init(config.bootstrapServers, "audit-topic");
 *
 *   // In the consumer finally block, alongside consumer.close():
 *   AuditProducer.shutdown();
 */
public final class AuditProducer {

    private static final Logger logger = LoggerFactory.getLogger(AuditProducer.class);

    // Volatile + double-checked locking for safe lazy singleton initialization
    private static volatile AuditProducer INSTANCE = null;

    private final KafkaProducer<String, String> producer;
    private final String auditTopic;


    private AuditProducer(String bootstrapServers, String auditTopic) {
        this.auditTopic = auditTopic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                  bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer");

        // Audit events are best-effort: acks=1 avoids leader-wait overhead,
        // a small linger batches rapid back-to-back events together.
        props.put(ProducerConfig.ACKS_CONFIG,             "1");
        props.put(ProducerConfig.RETRIES_CONFIG,          "2");
        props.put(ProducerConfig.LINGER_MS_CONFIG,        "20");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Initialize the singleton. Must be called once before the consumer poll loop starts.
     *
     * @param bootstrapServers same cluster the consumer is reading from
     * @param auditTopic       destination topic, e.g. "audit-topic"
     */
    public static void init(String bootstrapServers, String auditTopic) {
        if (INSTANCE == null) {
            synchronized (AuditProducer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new AuditProducer(bootstrapServers, auditTopic);
                    logger.info("[AUDIT] AuditProducer initialized → {}", auditTopic);
                }
            }
        }
    }

    /**
     * Retrieve the initialized singleton. Throws if init() was not called first.
     */
    public static AuditProducer instance() {
        AuditProducer inst = INSTANCE;
        if (inst == null) {
            throw new IllegalStateException(
                "AuditProducer.init() must be called before the consumer poll loop");
        }
        return inst;
    }

    /**
     * Flush pending audit records and close the producer.
     * Call in the consumer finally block alongside consumer.close().
     */
    public static void shutdown() {
        AuditProducer inst = INSTANCE;
        if (inst != null) {
            inst.producer.flush();
            inst.producer.close();
            INSTANCE = null;
            logger.info("[AUDIT] AuditProducer shut down");
        }
    }

    /**
     * Publish an AuditRecord asynchronously.
     *
     * The record's eventId is used as the Kafka message key so records for
     * the same consumer group land on the same audit partition (useful for
     * ordered replay). Errors are logged but never propagated.
     *
     * @param record the audit event to publish
     */
    public void send(AuditRecord record) {
        try {
            String json = record.toJson();
            // Key by consumerGroup so all audit events for one group go to
            // the same partition — makes audit log replay straightforward.
            ProducerRecord<String, String> kafkaRecord =
                new ProducerRecord<>(auditTopic, record.consumerGroup, json);

            producer.send(kafkaRecord, (metadata, ex) -> {
                if (ex != null) {
                    logger.warn("[AUDIT] Failed to deliver audit record stage={} eventId={}: {}",
                        record.stage, record.eventId, ex.getMessage());
                } else {
                    logger.debug("[AUDIT] Delivered stage={} → {}@partition={} offset={}",
                        record.stage, auditTopic, metadata.partition(), metadata.offset());
                }
            });

        } catch (Exception e) {
            // Catch-all: audit must never disrupt the consumer pipeline
            logger.warn("[AUDIT] Unexpected error sending audit record: {}", e.getMessage());
        }
    }
}
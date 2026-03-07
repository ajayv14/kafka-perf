package com.kafka.perf.audit;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * AuditAggregator — stateful timeout check using Kafka Streams Processor API
 * with WALL_CLOCK_TIME punctuation and Jackson for all serialization.
 *
 * State store holds BATCH_READ entries keyed by eventId.
 * When OFFSET_COMMITTED arrives → entry deleted → forward to audit.committed.
 * When wall-clock punctuator fires → expired entries → forward to audit.failed.
 *
 * Uses Jackson (already in project dependencies):
 *   - jackson-databind: 2.18.2
 *   - jackson-datatype-jsr310: 2.18.2 (for Instant support)
 */
public class AuditAggregator {

    private static final Logger logger = LoggerFactory.getLogger(AuditAggregator.class);


    public static final String TOPIC_FAILED    = "audit.failed";
    private static final String STORE_NAME     = "pending-batches";

    static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    // -------------------------------------------------------------------------
    // Internal DTOs
    // -------------------------------------------------------------------------

    /** Stored in the state store — represents a pending (uncommitted) batch. */
    static class PendingBatch {
        String  eventId;
        String  consumerGroup;
        String  sourceTopic;
        Instant batchReadAt;
        int     recordCount;
        long    offsetMin;
        long    offsetMax;
    }

    /** Written to audit.failed topic for failed transactions. */
    static class AuditOutcome {
        String  eventId;
        String  outcome;          // "FAILED"
        String  consumerGroup;
        String  sourceTopic;
        Instant batchReadAt;
        Instant failedAt;         // when the failure was detected
        int     recordCount;
        long    offsetMin;
        long    offsetMax;
    }

    // -------------------------------------------------------------------------
    // Build topology
    // -------------------------------------------------------------------------

    public static KafkaStreams build(
            String bootstrapServers,
            String auditTopic,
            long   transactionTimeoutMs,
            long   failureThresholdExtraSeconds,
            long   punctuateIntervalSeconds,
            Properties streamsProperties) {

        Duration failureThreshold  = Duration.ofMillis(transactionTimeoutMs).plusSeconds(failureThresholdExtraSeconds);
        Duration punctuateInterval = Duration.ofSeconds(punctuateIntervalSeconds);

        logger.info("AuditAggregator — threshold={}s punctuate={}s source={}",
            failureThreshold.toSeconds(), punctuateInterval.toSeconds(), auditTopic);

        Topology topology = new Topology();

        topology.addSource("audit-source",
            Serdes.String().deserializer(),
            Serdes.String().deserializer(),
            auditTopic);

        topology.addProcessor("audit-processor",
            (ProcessorSupplier<String, String, String, String>)
                () -> new AuditProcessor(failureThreshold, punctuateInterval),
            "audit-source");

       /* topology.addSink("committed-sink",
            TOPIC_COMMITTED,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
            "audit-processor");*/

        topology.addSink("failed-sink",
            TOPIC_FAILED,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
            "audit-processor");

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()   // PendingBatch stored as Gson JSON string
            ),
            "audit-processor");

        KafkaStreams streams = new KafkaStreams(topology, streamsConfig(bootstrapServers, streamsProperties));

        streams.setUncaughtExceptionHandler(throwable -> {
            logger.error("AuditAggregator fatal: {}", throwable.getMessage(), throwable);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                       .StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        return streams;
    }

    // -------------------------------------------------------------------------
    // Processor
    // -------------------------------------------------------------------------

    static class AuditProcessor implements Processor<String, String, String, String> {

        private static final Logger log = LoggerFactory.getLogger(AuditProcessor.class);

        private final Duration failureThreshold;
        private final Duration punctuateInterval;

        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String>    store;

        AuditProcessor(Duration failureThreshold, Duration punctuateInterval) {
            this.failureThreshold  = failureThreshold;
            this.punctuateInterval = punctuateInterval;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.store   = context.getStateStore(STORE_NAME);

            context.schedule(
                punctuateInterval,
                PunctuationType.WALL_CLOCK_TIME,
                this::punctuate
            );
        }

        @Override
        public void process(Record<String, String> record) {
            // Deserialize the incoming AuditRecord JSON using Jackson
            AuditRecord auditRecord;
            try {
                auditRecord = MAPPER.readValue(record.value(), AuditRecord.class);
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize AuditRecord: {}", record.value(), e);
                return;
            }

            if (auditRecord == null || auditRecord.stage == null) return;

            switch (auditRecord.stage) {

                case BATCH_READ -> {
                    // Build and store a PendingBatch entry
                    PendingBatch pending   = new PendingBatch();
                    pending.eventId        = auditRecord.eventId;
                    pending.consumerGroup  = auditRecord.consumerGroup;
                    pending.sourceTopic    = auditRecord.sourceTopic;
                    pending.batchReadAt    = auditRecord.timestamp;
                    pending.recordCount    = auditRecord.recordCount;
                    pending.offsetMin      = auditRecord.offsetMin;
                    pending.offsetMax      = auditRecord.offsetMax;

                    // Serialize PendingBatch to JSON string for the state store
                    try {
                        store.put(auditRecord.eventId, MAPPER.writeValueAsString(pending));
                    } catch (JsonProcessingException e) {
                        log.error("Failed to serialize PendingBatch: {}", auditRecord.eventId, e);
                        return;
                    }
                    log.debug("[AUDIT] Pending — eventId={}", auditRecord.eventId);
                }

                case OFFSET_COMMITTED -> {
                    String pendingJson = store.get(auditRecord.eventId);

                    if (pendingJson != null) {
                        PendingBatch pending;
                        try {
                            pending = MAPPER.readValue(pendingJson, PendingBatch.class);
                        } catch (JsonProcessingException e) {
                            log.error("Failed to deserialize PendingBatch: {}", pendingJson, e);
                            return;
                        }

                        // Disabled: Not tracking committed batches, only failures
                        store.delete(auditRecord.eventId);
                        log.debug("[AUDIT] Committed — eventId={} (not tracked)", auditRecord.eventId);

                    } else {
                        // No matching BATCH_READ — either very late commit after punctuator
                        // already expired the entry, or post-restart gap before store restored
                        log.warn("[AUDIT] OFFSET_COMMITTED with no pending entry — eventId={} (late or post-restart)",
                            auditRecord.eventId);
                    }
                }
            }
        }

        /**
         * Fires every punctuateInterval on wall-clock time.
         * Emits any PendingBatch older than failureThreshold to audit.failed.
         */
        private void punctuate(long wallClockMs) {
            Instant cutoff = Instant.ofEpochMilli(wallClockMs).minus(failureThreshold);
            int expired = 0;

            try (KeyValueIterator<String, String> it = store.all()) {
                
                while (it.hasNext()) {
                    var entry = it.next();
                    PendingBatch pending;
                    
                    try {
                        pending = MAPPER.readValue(entry.value, PendingBatch.class);
                    } catch (JsonProcessingException e) {
                        log.error("Failed to deserialize PendingBatch during punctuation: {}", entry.value, e);
                        continue;
                    }

                    if (pending.batchReadAt != null && pending.batchReadAt.isBefore(cutoff)) {
                        AuditOutcome outcome = failedOutcome(pending, Instant.ofEpochMilli(wallClockMs));
                        try {
                            context.forward(
                                new Record<>(outcome.eventId, MAPPER.writeValueAsString(outcome), wallClockMs),
                                "failed-sink"
                            );
                        } catch (JsonProcessingException e) {
                            log.error("Failed to serialize AuditOutcome during punctuation: {}", outcome.eventId, e);
                            continue;
                        }
                        store.delete(entry.key);
                        log.warn("[AUDIT] FAILED — eventId={} batchReadAt={} consumerGroup={}",
                            pending.eventId, pending.batchReadAt, pending.consumerGroup);
                        expired++;
                    }
                }
            }

            if (expired > 0) {
                log.warn("[AUDIT] Punctuator expired {} pending batch(es) as FAILED", expired);
            }
        }

        @Override
        public void close() {}

        // -- Outcome builders ---------------------------------------------------

        private static AuditOutcome failedOutcome(PendingBatch pending, Instant failedAt) {
            AuditOutcome o  = new AuditOutcome();
            o.eventId       = pending.eventId;
            o.outcome       = "FAILED";
            o.consumerGroup = pending.consumerGroup;
            o.sourceTopic   = pending.sourceTopic;
            o.batchReadAt   = pending.batchReadAt;
            o.failedAt      = failedAt;
            o.recordCount   = pending.recordCount;
            o.offsetMin     = pending.offsetMin;
            o.offsetMax     = pending.offsetMax;
            return o;
        }
    }

    // -------------------------------------------------------------------------
    // AuditRecord DTO — mirrors AuditRecord.java fields for Gson deserialization
    // -------------------------------------------------------------------------

    static class AuditRecord {
        String     eventId;
        AuditStage stage;
        String     consumerGroup;
        String     sourceTopic;
        Instant    timestamp;
        int        recordCount;
        long       offsetMin;
        long       offsetMax;
    }

    // -------------------------------------------------------------------------
    // Streams config
    // -------------------------------------------------------------------------

    private static Properties streamsConfig(String bootstrapServers, Properties defaultProps) {
        Properties props = new Properties();
        
        // Set APPLICATION_ID from properties file or default
        String appId = defaultProps.getProperty("application.id", "audit-aggregator");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,            appId);
        
        // Set required Kafka Streams configs
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,         bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,   Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        
        // Set optional configs from properties file
        if (defaultProps.containsKey("commit.interval.ms")) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 
                defaultProps.getProperty("commit.interval.ms"));
        }
        if (defaultProps.containsKey("replication.factor")) {
            props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 
                defaultProps.getProperty("replication.factor"));
        }
        if (defaultProps.containsKey("num.standby.replicas")) {
            props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 
                defaultProps.getProperty("num.standby.replicas"));
        }
        if (defaultProps.containsKey("processing.guarantee")) {
            props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, 
                defaultProps.getProperty("processing.guarantee"));
        }
        
        return props;
    }

    // -------------------------------------------------------------------------
    // Load configuration
    // -------------------------------------------------------------------------

    private static Properties loadStreamsProperties() throws IOException {
        Properties props = new Properties();
        try (InputStream in = AuditAggregator.class.getClassLoader()
                .getResourceAsStream("streams.properties")) {
            if (in != null) {
                props.load(in);
                logger.info("Loaded streams.properties from classpath");
            } else {
                logger.warn("streams.properties not found in classpath, using defaults");
            }
        }
        return props;
    }

    public static void main(String[] args) {
        try {
            // Load default streams configuration from properties file
            Properties streamsProps = loadStreamsProperties();

            // Override with environment variables
            String bootstrapServers   = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", 
                                        streamsProps.getProperty("bootstrap.servers", "localhost:9092"));
            String auditTopic         = System.getenv().getOrDefault("AUDIT_TOPIC", 
                                        streamsProps.getProperty("audit.topic", "audit-topic"));
            long   transactionTimeout = Long.parseLong(
                                        System.getenv().getOrDefault("TRANSACTION_TIMEOUT_MS", 
                                        streamsProps.getProperty("transaction.timeout.ms", "60000")));
            long   failureThresholdExtra = Long.parseLong(
                                        streamsProps.getProperty("failure.threshold.extra.seconds", "30"));
            long   punctuateInterval = Long.parseLong(
                                        streamsProps.getProperty("punctuation.interval.seconds", "10"));

            KafkaStreams streams = build(bootstrapServers, auditTopic, transactionTimeout, 
                                        failureThresholdExtra, punctuateInterval, streamsProps);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down AuditAggregator...");
                streams.close(Duration.ofSeconds(10));
            }));

            streams.start();
            logger.info("AuditAggregator running — tracking failed transactions to {}", TOPIC_FAILED);
        } catch (Exception e) {
            logger.error("Failed to start AuditAggregator", e);
            System.exit(1);
        }
    }
}
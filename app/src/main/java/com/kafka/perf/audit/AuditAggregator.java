package com.kafka.perf.audit;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
 * AuditAggregator — stateful timeout and replay estimator using Kafka Streams.
 *
 * State store holds one lifecycle entry per deterministic batch fingerprint.
 * BATCH_READ creates or updates that lifecycle entry.
 * OFFSET_COMMITTED reconciles the lifecycle entry and can emit LATE_COMMIT when
 * a previous timeout estimate is later followed by a commit.
 * Wall-clock punctuation emits ESTIMATED_FAILED when a pending batch exceeds the
 * configured threshold and REPLAY_OBSERVED when the same batch fingerprint is
 * seen again on a later poll.
 */
public class AuditAggregator {

    private static final Logger logger = LoggerFactory.getLogger(AuditAggregator.class);

    public static final String TOPIC_OUTCOMES = "audit.outcomes";
    private static final String STORE_NAME = "pending-batches";

    static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    enum LifecycleState {
        PENDING,
        TIMED_OUT
    }

    static class PendingBatch {
        String eventId;
        String consumerGroup;
        String sourceTopic;
        Instant firstSeenAt;
        Instant lastSeenAt;
        Instant timedOutAt;
        LifecycleState lifecycleState;
        int recordCount;
        int replayCount;
        int timeoutCount;
        List<AuditRecord.PartitionRange> partitionRanges;
    }

    static class AuditOutcome {
        String eventId;
        String outcome;
        String consumerGroup;
        String sourceTopic;
        Instant firstSeenAt;
        Instant lastSeenAt;
        Instant observedAt;
        int recordCount;
        int replayCount;
        int timeoutCount;
        List<AuditRecord.PartitionRange> partitionRanges;
    }

    public static KafkaStreams build(
            String bootstrapServers,
            String auditTopic,
            long transactionTimeoutMs,
            long failureThresholdExtraSeconds,
            long punctuateIntervalSeconds,
            Properties streamsProperties) {

        Duration failureThreshold = Duration.ofMillis(transactionTimeoutMs).plusSeconds(failureThresholdExtraSeconds);
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

        topology.addSink("outcome-sink",
            TOPIC_OUTCOMES,
            Serdes.String().serializer(),
            Serdes.String().serializer(),
            "audit-processor");

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()
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

    static class AuditProcessor implements Processor<String, String, String, String> {

        private static final Logger log = LoggerFactory.getLogger(AuditProcessor.class);

        private final Duration failureThreshold;
        private final Duration punctuateInterval;

        private ProcessorContext<String, String> context;
        private KeyValueStore<String, String> store;

        private long replayObservedCount;
        private long estimatedFailedCount;
        private long lateCommitCount;
        private long committedCount;
        private long unknownCommitCount;

        AuditProcessor(Duration failureThreshold, Duration punctuateInterval) {
            this.failureThreshold = failureThreshold;
            this.punctuateInterval = punctuateInterval;
        }

        @Override
        public void init(ProcessorContext<String, String> context) {
            this.context = context;
            this.store = context.getStateStore(STORE_NAME);
            context.schedule(
                punctuateInterval,
                PunctuationType.WALL_CLOCK_TIME,
                this::punctuate
            );
        }

        @Override
        public void process(Record<String, String> record) {
            AuditRecord auditRecord;
            try {
                auditRecord = AuditRecord.fromJson(record.value());
            } catch (IllegalArgumentException e) {
                log.error("Failed to deserialize AuditRecord: {}", record.value(), e);
                return;
            }

            if (auditRecord.stage == null) {
                return;
            }

            switch (auditRecord.stage) {
                case BATCH_READ -> handleBatchRead(auditRecord);
                case OFFSET_COMMITTED -> handleOffsetCommitted(auditRecord);
            }
        }

        private void handleBatchRead(AuditRecord auditRecord) {
            PendingBatch pending = readPending(auditRecord.eventId);
            boolean replayObserved = pending != null;

            if (pending == null) {
                pending = new PendingBatch();
                pending.eventId = auditRecord.eventId;
                pending.consumerGroup = auditRecord.consumerGroup;
                pending.sourceTopic = auditRecord.sourceTopic;
                pending.firstSeenAt = auditRecord.timestamp;
                pending.replayCount = 0;
                pending.timeoutCount = 0;
            } else {
                pending.replayCount++;
                replayObservedCount++;
                emitOutcome("REPLAY_OBSERVED", pending, auditRecord.timestamp);
            }

            pending.lastSeenAt = auditRecord.timestamp;
            pending.recordCount = auditRecord.recordCount;
            pending.partitionRanges = auditRecord.partitionRanges;
            pending.lifecycleState = LifecycleState.PENDING;
            pending.timedOutAt = null;

            writePending(pending);

            if (replayObserved) {
                log.info("[AUDIT] Replay observed — eventId={} replayCount={}",
                    pending.eventId, pending.replayCount);
            } else {
                log.debug("[AUDIT] Pending — eventId={}", pending.eventId);
            }
        }

        private void handleOffsetCommitted(AuditRecord auditRecord) {
            PendingBatch pending = readPending(auditRecord.eventId);
            if (pending == null) {
                unknownCommitCount++;
                log.warn("[AUDIT] OFFSET_COMMITTED with no pending entry — eventId={}",
                    auditRecord.eventId);
                return;
            }

            pending.lastSeenAt = auditRecord.timestamp;

            if (pending.timeoutCount > 0) {
                lateCommitCount++;
                emitOutcome("LATE_COMMIT", pending, auditRecord.timestamp);
                log.warn("[AUDIT] Late commit reconciled — eventId={} timeoutCount={} replayCount={}",
                    pending.eventId, pending.timeoutCount, pending.replayCount);
            } else {
                committedCount++;
                emitOutcome("COMMITTED", pending, auditRecord.timestamp);
                log.debug("[AUDIT] Committed — eventId={}", pending.eventId);
            }

            store.delete(auditRecord.eventId);
        }

        private PendingBatch readPending(String eventId) {
            String json = store.get(eventId);
            if (json == null) {
                return null;
            }

            try {
                return MAPPER.readValue(json, PendingBatch.class);
            } catch (JsonProcessingException e) {
                log.error("Failed to deserialize PendingBatch: {}", json, e);
                return null;
            }
        }

        private void writePending(PendingBatch pending) {
            try {
                store.put(pending.eventId, MAPPER.writeValueAsString(pending));
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize PendingBatch: {}", pending.eventId, e);
            }
        }

        private void emitOutcome(String outcome, PendingBatch pending, Instant observedAt) {
            AuditOutcome auditOutcome = new AuditOutcome();
            auditOutcome.eventId = pending.eventId;
            auditOutcome.outcome = outcome;
            auditOutcome.consumerGroup = pending.consumerGroup;
            auditOutcome.sourceTopic = pending.sourceTopic;
            auditOutcome.firstSeenAt = pending.firstSeenAt;
            auditOutcome.lastSeenAt = pending.lastSeenAt;
            auditOutcome.observedAt = observedAt;
            auditOutcome.recordCount = pending.recordCount;
            auditOutcome.replayCount = pending.replayCount;
            auditOutcome.timeoutCount = pending.timeoutCount;
            auditOutcome.partitionRanges = pending.partitionRanges;

            try {
                context.forward(
                    new Record<>(auditOutcome.eventId, MAPPER.writeValueAsString(auditOutcome),
                        observedAt == null ? System.currentTimeMillis() : observedAt.toEpochMilli()),
                    "outcome-sink"
                );
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize AuditOutcome: {}", pending.eventId, e);
            }
        }

        private void punctuate(long wallClockMs) {
            Instant cutoff = Instant.ofEpochMilli(wallClockMs).minus(failureThreshold);
            int pendingCount = 0;

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

                    if (pending.lifecycleState == LifecycleState.PENDING) {
                        pendingCount++;
                        if (pending.lastSeenAt != null && pending.lastSeenAt.isBefore(cutoff)) {
                            pending.lifecycleState = LifecycleState.TIMED_OUT;
                            pending.timedOutAt = Instant.ofEpochMilli(wallClockMs);
                            pending.timeoutCount++;
                            estimatedFailedCount++;
                            emitOutcome("ESTIMATED_FAILED", pending, pending.timedOutAt);
                            writePending(pending);
                            log.warn("[AUDIT] Estimated failure — eventId={} timeoutCount={} replayCount={}",
                                pending.eventId, pending.timeoutCount, pending.replayCount);
                        }
                    }
                }
            }

            log.info("[AUDIT] Summary pending={} replays={} estimatedFailed={} lateCommits={} committed={} unknownCommits={}",
                pendingCount, replayObservedCount, estimatedFailedCount, lateCommitCount, committedCount,
                unknownCommitCount);
        }

        @Override
        public void close() {}
    }

    private static Properties streamsConfig(String bootstrapServers, Properties defaultProps) {
        Properties props = new Properties();
        String appId = defaultProps.getProperty("application.id", "audit-aggregator");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

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
            Properties streamsProps = loadStreamsProperties();

            String bootstrapServers = System.getenv().getOrDefault("BOOTSTRAP_SERVERS",
                streamsProps.getProperty("bootstrap.servers", "localhost:9092"));
            String auditTopic = System.getenv().getOrDefault("AUDIT_TOPIC",
                streamsProps.getProperty("audit.topic", "audit-topic"));
            long transactionTimeout = Long.parseLong(
                System.getenv().getOrDefault("TRANSACTION_TIMEOUT_MS",
                    streamsProps.getProperty("transaction.timeout.ms", "60000")));
            long failureThresholdExtra = Long.parseLong(
                streamsProps.getProperty("failure.threshold.extra.seconds", "30"));
            long punctuateInterval = Long.parseLong(
                streamsProps.getProperty("punctuation.interval.seconds", "10"));

            KafkaStreams streams = build(
                bootstrapServers,
                auditTopic,
                transactionTimeout,
                failureThresholdExtra,
                punctuateInterval,
                streamsProps
            );

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down AuditAggregator...");
                streams.close(Duration.ofSeconds(10));
            }));

            streams.start();
            logger.info("AuditAggregator running — writing lifecycle estimates to {}", TOPIC_OUTCOMES);
        } catch (Exception e) {
            logger.error("Failed to start AuditAggregator", e);
            System.exit(1);
        }
    }
}

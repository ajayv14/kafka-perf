package com.kafka.perf.audit;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuditableConsumer — a transparent delegate wrapper around KafkaConsumer that
 * intercepts two points in the consumer pipeline and publishes audit events to
 * the Kafka audit topic.
 *
 * Pointcut 1 — BATCH_READ
 *   Fires after poll() returns a non-empty batch.
 *   Publishes: eventId, stage, consumerGroup, sourceTopic, timestamp,
 *              recordCount, partitionRanges.
 *
 * Pointcut 2 — OFFSET_COMMITTED
 *   Fires after commitSync() returns successfully (not on failure/exception).
 *   Publishes: the SAME eventId as the preceding BATCH_READ, stage, consumerGroup,
 *              sourceTopic, timestamp.
 *   Offset detail is intentionally omitted — join on eventId to the paired
 *   BATCH_READ record to retrieve it.
 *
 * Audit correlation model
 * -----------------------
 *   BATCH_READ(eventId=X)  +  OFFSET_COMMITTED(eventId=X)  → batch committed cleanly
 *   BATCH_READ(eventId=X)  +  no matching OFFSET_COMMITTED  → crash before commit
 *
 * Usage — one construction change in FaultInjectorConsumer, nothing else:
 *
 *   // Before:
 *   KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 *
 *   // After:
 *   KafkaConsumer<String, String> consumer =
 *       new AuditableConsumer<>(new KafkaConsumer<>(props), config.topic, config.groupId);
 *
 * Thread safety
 * -------------
 * KafkaConsumer is not thread-safe and neither is this wrapper.
 * The same single-threaded usage contract applies.
 *
 * Supported execution model
 * -------------------------
 * This wrapper is intentionally narrow in scope. It is designed for consumers
 * that execute the following sequence in a single thread:
 *
 *   1. poll() returns one batch
 *   2. the application processes that batch synchronously
 *   3. commitSync() is called once for that batch
 *
 * commitAsync() is delegated but not audited.
 * Multi-commit flows, overlapping in-flight batches, and more complex offset
 * management patterns are out of scope for this wrapper.
 */
public class AuditableConsumer<K, V> extends KafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(AuditableConsumer.class);

    private final KafkaConsumer<K, V> delegate;
    private final String              sourceTopic;
    private final String              consumerGroup;
    private final AuditProducer       auditProducer;

    // The eventId is generated once per poll() and reused for the subsequent
    // commitSync(). This assumes one polled batch is processed synchronously and
    // acknowledged once before the next audited batch lifecycle begins.
    private String currentBatchEventId = null;

    /**
     * @param delegate      the real KafkaConsumer — all calls are forwarded to it
     * @param sourceTopic   topic being consumed
     * @param consumerGroup consumer group id
     */
    public AuditableConsumer(
            KafkaConsumer<K, V> delegate,
            String sourceTopic,
            String consumerGroup) {

        // KafkaConsumer has no no-arg constructor, so we must pass dummy props to
        // satisfy the superclass. The superclass instance is immediately closed and
        // never used — all real work is performed by the delegate below.
        super(dummyProps());
        super.close();

        this.delegate      = delegate;
        this.sourceTopic   = sourceTopic;
        this.consumerGroup = consumerGroup;
        this.auditProducer = AuditProducer.instance();
    }

    // -------------------------------------------------------------------------
    // Pointcut 1 — poll()
    // -------------------------------------------------------------------------

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<K, V> records = delegate.poll(timeout);

        if (!records.isEmpty()) {
            Map<Integer, PartitionAccumulator> partitions = new HashMap<>();
            for (var record : records) {
                partitions
                    .computeIfAbsent(record.partition(), ignored -> new PartitionAccumulator(record.partition()))
                    .accept(record.offset());
            }

            List<AuditRecord.PartitionRange> partitionRanges = new ArrayList<>(partitions.size());
            for (PartitionAccumulator accumulator : partitions.values()) {
                partitionRanges.add(accumulator.toRange());
            }
            partitionRanges.sort(Comparator.comparingInt(range -> range.partition));

            currentBatchEventId = fingerprintBatch(partitionRanges, records.count());

            AuditRecord auditRecord = AuditRecord.builder(AuditStage.BATCH_READ)
                .eventId(currentBatchEventId)
                .consumerGroup(consumerGroup)
                .sourceTopic(sourceTopic)
                .recordCount(records.count())
                .partitionRanges(partitionRanges)
                .build();

            auditProducer.send(auditRecord);
            logger.debug("[AUDIT] BATCH_READ eventId={} count={} partitions={}",
                currentBatchEventId, records.count(), partitionRanges.size());
        }

        return records;
    }

    // -------------------------------------------------------------------------
    // Pointcut 2 — commitSync() (all overloads)
    // Audit fires only if commit succeeds — if commitSync() throws, no audit
    // record is produced since the commit did not actually happen.
    // -------------------------------------------------------------------------

    @Override
    public void commitSync() {
        delegate.commitSync();
        publishOffsetCommittedAudit();
    }

    @Override
    public void commitSync(Duration timeout) {
        delegate.commitSync(timeout);
        publishOffsetCommittedAudit();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);
        publishOffsetCommittedAudit();
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        delegate.commitSync(offsets, timeout);
        publishOffsetCommittedAudit();
    }

    /**
     * Publishes an OFFSET_COMMITTED audit record using the eventId from the
     * most recent poll(). The eventId is the only link needed — consumers of
     * the audit topic join on it to get offset detail from the BATCH_READ record.
     */
    private void publishOffsetCommittedAudit() {
        if (currentBatchEventId == null) {
            // commitSync() called before any poll() — nothing to correlate against
            logger.warn("[AUDIT] commitSync() fired before any batch was polled, skipping audit");
            return;
        }

        AuditRecord auditRecord = AuditRecord.builder(AuditStage.OFFSET_COMMITTED)
            .eventId(currentBatchEventId)   // same id as the paired BATCH_READ
            .consumerGroup(consumerGroup)
            .sourceTopic(sourceTopic)
            .build();

        auditProducer.send(auditRecord);
        logger.debug("[AUDIT] OFFSET_COMMITTED eventId={}", currentBatchEventId);

        // Reset so a stale eventId is never reused if commitSync() is called twice
        // without an intervening poll()
        currentBatchEventId = null;
    }

    // -------------------------------------------------------------------------
    // All remaining KafkaConsumer methods — pure delegation, no interception
    // -------------------------------------------------------------------------

    @Override
    public void subscribe(Collection<String> topics) {
        delegate.subscribe(topics);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        delegate.subscribe(topics, listener);
    }

    @Override
    public void commitAsync() {
        logger.debug("[AUDIT] commitAsync() delegated without audit; commitAsync is out of scope");
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        logger.debug("[AUDIT] commitAsync(callback) delegated without audit; commitAsync is out of scope");
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        logger.debug("[AUDIT] commitAsync(offsets, callback) delegated without audit; commitAsync is out of scope");
        delegate.commitAsync(offsets, callback);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        delegate.seekToBeginning(partitions);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        delegate.seekToEnd(partitions);
    }

    @Override
    public Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    @Override
    public Set<String> subscription() {
        return delegate.subscription();
    }

    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    @Override
    public void close() {
        if (delegate == null) {
            super.close();
            return;
        }
        delegate.close();
    }

    @Override
    public void close(Duration timeout) {
        if (delegate == null) {
            super.close(timeout);
            return;
        }
        delegate.close(timeout);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private static Properties dummyProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers",  "localhost:9092");
        p.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id",           "__audit_dummy__");
        return p;
    }

    private String fingerprintBatch(List<AuditRecord.PartitionRange> partitionRanges, int recordCount) {
        StringBuilder builder = new StringBuilder();
        builder.append(consumerGroup).append('|')
            .append(sourceTopic).append('|')
            .append(recordCount);

        for (AuditRecord.PartitionRange range : partitionRanges) {
            builder.append('|')
                .append(range.partition).append(':')
                .append(range.offsetMin).append(':')
                .append(range.offsetMax).append(':')
                .append(range.recordCount);
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(builder.toString().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xF, 16));
                hex.append(Character.forDigit(b & 0xF, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static final class PartitionAccumulator {
        private final int partition;
        private long offsetMin = Long.MAX_VALUE;
        private long offsetMax = Long.MIN_VALUE;
        private int recordCount = 0;

        private PartitionAccumulator(int partition) {
            this.partition = partition;
        }

        private void accept(long offset) {
            offsetMin = Math.min(offsetMin, offset);
            offsetMax = Math.max(offsetMax, offset);
            recordCount++;
        }

        private AuditRecord.PartitionRange toRange() {
            return new AuditRecord.PartitionRange(partition, offsetMin, offsetMax, recordCount);
        }
    }
}

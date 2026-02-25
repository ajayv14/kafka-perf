package com.kafka.perf.baseline;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kafka.perf.audit.AuditProducer;
import com.kafka.perf.audit.AuditRecord;
import com.kafka.perf.audit.AuditStage;

/**
 * AuditableConsumer — a transparent delegate wrapper around KafkaConsumer that
 * adds audit event production at two "pointcuts" without any framework or agent.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  Pointcut 1 — after poll() returns a non-empty batch                │
 * │    → publishes AuditRecord(stage=BATCH_READ) to audit-topic         │
 * │                                                                     │
 * │  Pointcut 2 — after commitSync() returns successfully               │
 * │    → publishes AuditRecord(stage=OFFSET_COMMITTED) to audit-topic   │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * Usage — only the construction line in FaultInjectorConsumer changes:
 *
 *   // Before:
 *   KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
 *
 *   // After:
 *   KafkaConsumer<String, String> consumer =
 *       new AuditableConsumer<>(new KafkaConsumer<>(props), config.topic, config.groupId);
 *
 * All other code in runConsumer() is unchanged — subscribe(), poll(),
 * commitSync(), and close() are called exactly as before.
 *
 * Thread safety
 * -------------
 * KafkaConsumer is not thread-safe and neither is this wrapper. The same
 * single-threaded usage contract applies.
 */
public class AuditableConsumer<K, V> extends KafkaConsumer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(AuditableConsumer.class);

    private final KafkaConsumer<K, V> delegate;
    private final String              sourceTopic;
    private final String              consumerGroup;
    private final AuditProducer       auditProducer;

    // Tracks the most recent batch so commitSync() can reference offset range
    private volatile long lastBatchOffsetMin = -1;
    private volatile long lastBatchOffsetMax = -1;
    private volatile int  lastBatchCount     = 0;

    /**
     * @param delegate      the real KafkaConsumer — all calls are forwarded to it
     * @param sourceTopic   topic being consumed (used in audit record)
     * @param consumerGroup consumer group id (used in audit record + as audit partition key)
     */
    public AuditableConsumer(
            KafkaConsumer<K, V> delegate,
            String sourceTopic,
            String consumerGroup) {

        // We never use the superclass instance directly — KafkaConsumer requires
        // a Properties argument to construct even as a base class, so we pass a
        // dummy. All real work is delegated to the wrapped instance below.
        super(dummyProps());

        // Immediately close the superclass's internal resources so we don't leak
        // a second real consumer. The delegate does all the actual work.
        super.close();

        this.delegate      = delegate;
        this.sourceTopic   = sourceTopic;
        this.consumerGroup = consumerGroup;
        this.auditProducer = AuditProducer.instance();
    }

    // ------------------------------------------------------------------
    // Pointcut 1 — poll()
    // Fires AFTER poll returns; audit only on non-empty batches.
    // ------------------------------------------------------------------

    @Override
    public ConsumerRecords<K, V> poll(Duration timeout) {
        
        ConsumerRecords<K, V> records = delegate.poll(timeout);

        if (!records.isEmpty()) {
            // Capture offset range across all partitions in this batch
            long min = Long.MAX_VALUE;
            long max = Long.MIN_VALUE;
            for (var record : records) {
                if (record.offset() < min) min = record.offset();
                if (record.offset() > max) max = record.offset();
            }
            lastBatchOffsetMin = min;
            lastBatchOffsetMax = max;
            lastBatchCount     = records.count();

            // ── AUDIT POINTCUT 1 ──────────────────────────────────────
            AuditRecord auditRecord = AuditRecord.builder(AuditStage.BATCH_READ)
                .consumerGroup(consumerGroup)
                .sourceTopic(sourceTopic)
                .recordCount(records.count())
                .offsetMin(min)
                .offsetMax(max)
                .build();

            auditProducer.send(auditRecord);
            logger.debug("[AUDIT-PC1] BATCH_READ count={} offsets=[{}-{}]",
                records.count(), min, max);
            // ─────────────────────────────────────────────────────────
        }

        return records;
    }

    // ------------------------------------------------------------------
    // Pointcut 2 — commitSync()
    // Both overloads are intercepted. Fires AFTER commit returns normally.
    // If commitSync() throws, no audit record is published — the commit
    // did not succeed, so auditing it would be misleading.
    // ------------------------------------------------------------------

    @Override
    public void commitSync() {
        delegate.commitSync();   // throws on failure — audit fires only if this returns

        // ── AUDIT POINTCUT 2 ──────────────────────────────────────────
        publishOffsetCommittedAudit();
        // ──────────────────────────────────────────────────────────────
    }

    @Override
    public void commitSync(Duration timeout) {
        delegate.commitSync(timeout);

        // AUDIT POINTCUT 2
        publishOffsetCommittedAudit();
       
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        delegate.commitSync(offsets);

        // AUDIT POINTCUT 2 
        publishOffsetCommittedAudit();
       
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        delegate.commitSync(offsets, timeout);

        // AUDIT POINTCUT 2 
        publishOffsetCommittedAudit();
    
    }

    /** Shared implementation for all commitSync() overloads. */
    private void publishOffsetCommittedAudit() {
        AuditRecord auditRecord = AuditRecord.builder(AuditStage.OFFSET_COMMITTED)
            .consumerGroup(consumerGroup)
            .sourceTopic(sourceTopic)
            .recordCount(lastBatchCount)      // batch that was just committed
            .offsetMin(lastBatchOffsetMin)
            .offsetMax(lastBatchOffsetMax)
            .build();

        auditProducer.send(auditRecord);
        logger.debug("[AUDIT-PC2] OFFSET_COMMITTED offsets=[{}-{}]",
            lastBatchOffsetMin, lastBatchOffsetMax);
    }

    // ------------------------------------------------------------------
    // All remaining KafkaConsumer methods delegated transparently
    // ------------------------------------------------------------------

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
        delegate.commitAsync();
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        delegate.commitAsync(callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
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
    public java.util.Set<TopicPartition> assignment() {
        return delegate.assignment();
    }

    @Override
    public java.util.Set<String> subscription() {
        return delegate.subscription();
    }

    @Override
    public void unsubscribe() {
        delegate.unsubscribe();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public void close(Duration timeout) {
        delegate.close(timeout);
    }

    @Override
    public void wakeup() {
        delegate.wakeup();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /**
     * KafkaConsumer requires a non-empty Properties to construct as a superclass.
     * We pass the bare minimum so the super() call compiles; the superclass
     * instance is immediately closed and never used — all work is on delegate.
     */
    private static java.util.Properties dummyProps() {
        java.util.Properties p = new java.util.Properties();
        p.put("bootstrap.servers",  "localhost:9092");
        p.put("key.deserializer",   "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id",           "__audit_dummy__");
        return p;
    }
}
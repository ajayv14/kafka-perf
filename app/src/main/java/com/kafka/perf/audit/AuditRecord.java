package com.kafka.perf.audit;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable payload published to the Kafka audit topic.
 *
 *
 * Fields
 * ------
 * eventId       — UUID, deduplication key for the audit record itself
 * stage         — BATCH_READ | OFFSET_COMMITTED
 * consumerGroup — Kafka consumer group id
 * sourceTopic   — topic being consumed
 * recordCount   — number of records in the batch (0 for OFFSET_COMMITTED)
 * offsetMin     — lowest offset seen in the batch  (-1 if not applicable)
 * offsetMax     — highest offset seen in the batch (-1 if not applicable)
 * timestamp     — time the event was captured (ISO-8601)
 */
public final class AuditRecord {

    public final String     eventId;
    public final AuditStage stage;
    public final String     consumerGroup;
    public final String     sourceTopic;
    public final int        recordCount;
    public final long       offsetMin;
    public final long       offsetMax;
    public final Instant    timestamp;

    private AuditRecord(Builder b) {
        this.eventId       = b.eventId;
        this.stage         = b.stage;
        this.consumerGroup = b.consumerGroup;
        this.sourceTopic   = b.sourceTopic;
        this.recordCount   = b.recordCount;
        this.offsetMin     = b.offsetMin;
        this.offsetMax     = b.offsetMax;
        this.timestamp     = b.timestamp;
    }

    public static Builder builder(AuditStage stage) {
        return new Builder(stage);
    }

    public static final class Builder {
        private final AuditStage stage;
        private String  eventId       = UUID.randomUUID().toString();
        private String  consumerGroup = "unknown";
        private String  sourceTopic   = "unknown";
        private int     recordCount   = 0;
        private long    offsetMin     = -1;
        private long    offsetMax     = -1;
        private Instant timestamp     = Instant.now();

        private Builder(AuditStage stage) { this.stage = stage; }

        public Builder consumerGroup(String v) { this.consumerGroup = v; return this; }
        public Builder sourceTopic(String v)   { this.sourceTopic = v;   return this; }
        public Builder recordCount(int v)      { this.recordCount = v;   return this; }
        public Builder offsetMin(long v)       { this.offsetMin = v;     return this; }
        public Builder offsetMax(long v)       { this.offsetMax = v;     return this; }

        public AuditRecord build()             { return new AuditRecord(this); }
    }

    // ------------------------------------------------------------------
    // Zero-dependency JSON serialization
    // ------------------------------------------------------------------

    /**
     * Produces a JSON string without any external library.
     * Example output:
     * {
     *   "eventId":"abc-123",
     *   "stage":"BATCH_READ",
     *   "consumerGroup":"perf-group",
     *   "sourceTopic":"events",
     *   "recordCount":500,
     *   "offsetMin":1000,
     *   "offsetMax":1499,
     *   "timestamp":"2025-01-15T10:30:00Z"
     * }
     */
    public String toJson() {
        return "{"
            + jsonStr("eventId",       eventId)       + ","
            + jsonStr("stage",         stage.name())  + ","
            + jsonStr("consumerGroup", consumerGroup) + ","
            + jsonStr("sourceTopic",   sourceTopic)   + ","
            + jsonNum("recordCount",   recordCount)   + ","
            + jsonNum("offsetMin",     offsetMin)     + ","
            + jsonNum("offsetMax",     offsetMax)     + ","
            + jsonStr("timestamp",     timestamp.toString())
            + "}";
    }

    private static String jsonStr(String key, String value) {
        return "\"" + key + "\":\"" + escape(value) + "\"";
    }

    private static String jsonNum(String key, long value) {
        return "\"" + key + "\":" + value;
    }

    /** Escape characters that would break JSON string values. */
    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    @Override
    public String toString() {
        return toJson();
    }
}
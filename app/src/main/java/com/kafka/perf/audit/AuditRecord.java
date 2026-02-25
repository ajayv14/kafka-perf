package com.kafka.perf.audit;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable payload published to the Kafka audit topic for each intercepted event.
 *
 * Two event types are produced (see AuditStage):
 *
 *   BATCH_READ       — emitted after poll() returns a non-empty batch.
 *                      Carries recordCount, offsetMin, offsetMax for that batch.
 *
 *   OFFSET_COMMITTED — emitted after commitSync() returns successfully.
 *                      Carries only the correlation eventId and metadata.
 *                      Offset detail is intentionally omitted: the matching
 *                      BATCH_READ record (joined on eventId) already holds it.
 *
 * Audit correlation
 * -----------------
 * A successful commit is confirmed by finding a BATCH_READ and OFFSET_COMMITTED
 * pair sharing the same eventId in the audit topic. A BATCH_READ with no matching
 * OFFSET_COMMITTED indicates the consumer crashed between poll and commit.
 *
 * No external JSON library required — serialization is handled internally.
 */
public final class AuditRecord {

    public final String     eventId;       // shared key linking BATCH_READ <-> OFFSET_COMMITTED
    public final AuditStage stage;
    public final String     consumerGroup;
    public final String     sourceTopic;
    public final Instant    timestamp;

    // Populated for BATCH_READ only; -1 / 0 for OFFSET_COMMITTED
    public final int        recordCount;
    public final long       offsetMin;
    public final long       offsetMax;

    private AuditRecord(Builder b) {
        this.eventId       = b.eventId;
        this.stage         = b.stage;
        this.consumerGroup = b.consumerGroup;
        this.sourceTopic   = b.sourceTopic;
        this.timestamp     = b.timestamp;
        this.recordCount   = b.recordCount;
        this.offsetMin     = b.offsetMin;
        this.offsetMax     = b.offsetMax;
    }

    // -------------------------------------------------------------------------
    // Builder
    // -------------------------------------------------------------------------

    public static Builder builder(AuditStage stage) {
        return new Builder(stage);
    }

    public static final class Builder {
        private final AuditStage stage;
        private String  eventId       = UUID.randomUUID().toString();
        private String  consumerGroup = "unknown";
        private String  sourceTopic   = "unknown";
        private Instant timestamp     = Instant.now();
        private int     recordCount   = 0;
        private long    offsetMin     = -1;
        private long    offsetMax     = -1;

        private Builder(AuditStage stage) { this.stage = stage; }

        public Builder eventId(String v)       { this.eventId = v;       return this; }
        public Builder consumerGroup(String v) { this.consumerGroup = v; return this; }
        public Builder sourceTopic(String v)   { this.sourceTopic = v;   return this; }
        public Builder recordCount(int v)      { this.recordCount = v;   return this; }
        public Builder offsetMin(long v)       { this.offsetMin = v;     return this; }
        public Builder offsetMax(long v)       { this.offsetMax = v;     return this; }

        public AuditRecord build() { return new AuditRecord(this); }
    }

    // -------------------------------------------------------------------------
    // Serialization — zero external dependencies
    // -------------------------------------------------------------------------

    /**
     * Serializes to JSON. BATCH_READ includes offset fields; OFFSET_COMMITTED omits
     * them since the consumer joins on eventId to retrieve them from the paired record.
     *
     * BATCH_READ example:
     *   {"eventId":"abc-123","stage":"BATCH_READ","consumerGroup":"perf-group",
     *    "sourceTopic":"events","timestamp":"2025-01-15T10:30:00Z",
     *    "recordCount":500,"offsetMin":1000,"offsetMax":1499}
     *
     * OFFSET_COMMITTED example:
     *   {"eventId":"abc-123","stage":"OFFSET_COMMITTED","consumerGroup":"perf-group",
     *    "sourceTopic":"events","timestamp":"2025-01-15T10:30:05Z"}
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append(jsonStr("eventId",       eventId))       .append(",")
            .append(jsonStr("stage",         stage.name()))  .append(",")
            .append(jsonStr("consumerGroup", consumerGroup)) .append(",")
            .append(jsonStr("sourceTopic",   sourceTopic))   .append(",")
            .append(jsonStr("timestamp",     timestamp.toString()));

        // Offset fields are only meaningful for BATCH_READ
        if (stage == AuditStage.BATCH_READ) {
            sb.append(",").append(jsonNum("recordCount", recordCount))
              .append(",").append(jsonNum("offsetMin",   offsetMin))
              .append(",").append(jsonNum("offsetMax",   offsetMax));
        }

        return sb.append("}").toString();
    }

    private static String jsonStr(String key, String value) {
        return "\"" + key + "\":\"" + escape(value) + "\"";
    }

    private static String jsonNum(String key, long value) {
        return "\"" + key + "\":" + value;
    }

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
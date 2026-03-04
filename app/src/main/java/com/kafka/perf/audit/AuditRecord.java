package com.kafka.perf.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

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
 * JSON serialization/deserialization is handled via Jackson.
 */
public final class AuditRecord {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

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
        public Builder timestamp(Instant v)    { this.timestamp = v;     return this; }
        public Builder recordCount(int v)      { this.recordCount = v;   return this; }
        public Builder offsetMin(long v)       { this.offsetMin = v;     return this; }
        public Builder offsetMax(long v)       { this.offsetMax = v;     return this; }

        public AuditRecord build() { return new AuditRecord(this); }
    }

    // -------------------------------------------------------------------------
    // Serialization / Deserialization — Jackson
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
        try {
            ObjectNode root = OBJECT_MAPPER.createObjectNode();
            root.put("eventId", eventId);
            root.put("stage", stage.name());
            root.put("consumerGroup", consumerGroup);
            root.put("sourceTopic", sourceTopic);
            root.put("timestamp", timestamp.toString());

            // Offset fields are only meaningful for BATCH_READ
            if (stage == AuditStage.BATCH_READ) {
                root.put("recordCount", recordCount);
                root.put("offsetMin", offsetMin);
                root.put("offsetMax", offsetMax);
            }

            return OBJECT_MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize AuditRecord", e);
        }
    }

    public static AuditRecord fromJson(String json) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(json);

            AuditStage stage = AuditStage.valueOf(requiredText(root, "stage"));
            Builder builder = AuditRecord.builder(stage)
                .eventId(requiredText(root, "eventId"))
                .consumerGroup(requiredText(root, "consumerGroup"))
                .sourceTopic(requiredText(root, "sourceTopic"))
                .timestamp(Instant.parse(requiredText(root, "timestamp")));

            if (stage == AuditStage.BATCH_READ) {
                builder.recordCount(requiredInt(root, "recordCount"))
                       .offsetMin(requiredLong(root, "offsetMin"))
                       .offsetMax(requiredLong(root, "offsetMax"));
            }

            return builder.build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize AuditRecord", e);
        }
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return child.asText();
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return child.asInt();
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            throw new IllegalArgumentException("Missing required field: " + field);
        }
        return child.asLong();
    }

    @Override
    public String toString() {
        return toJson();
    }
}
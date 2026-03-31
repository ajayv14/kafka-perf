package com.kafka.perf.audit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Immutable payload published to the Kafka audit topic for each intercepted
 * event in the consumer pipeline.
 *
 * BATCH_READ carries deterministic batch identity and per-partition offset
 * ranges so replayed batches can be recognized across retries and restarts.
 * OFFSET_COMMITTED reuses the same eventId and metadata.
 */
public final class AuditRecord {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public final String eventId;
    public final AuditStage stage;
    public final String consumerGroup;
    public final String sourceTopic;
    public final Instant timestamp;
    public final int recordCount;
    public final List<PartitionRange> partitionRanges;

    private AuditRecord(Builder b) {
        this.eventId = b.eventId;
        this.stage = b.stage;
        this.consumerGroup = b.consumerGroup;
        this.sourceTopic = b.sourceTopic;
        this.timestamp = b.timestamp;
        this.recordCount = b.recordCount;
        this.partitionRanges = List.copyOf(b.partitionRanges);
    }

    public static Builder builder(AuditStage stage) {
        return new Builder(stage);
    }

    public static final class PartitionRange {
        public final int partition;
        public final long offsetMin;
        public final long offsetMax;
        public final int recordCount;

        @JsonCreator
        public PartitionRange(
                @JsonProperty("partition") int partition,
                @JsonProperty("offsetMin") long offsetMin,
                @JsonProperty("offsetMax") long offsetMax,
                @JsonProperty("recordCount") int recordCount) {
            this.partition = partition;
            this.offsetMin = offsetMin;
            this.offsetMax = offsetMax;
            this.recordCount = recordCount;
        }
    }

    public static final class Builder {
        private final AuditStage stage;
        private String eventId = "";
        private String consumerGroup = "unknown";
        private String sourceTopic = "unknown";
        private Instant timestamp = Instant.now();
        private int recordCount = 0;
        private List<PartitionRange> partitionRanges = List.of();

        private Builder(AuditStage stage) {
            this.stage = stage;
        }

        public Builder eventId(String v) {
            this.eventId = v;
            return this;
        }

        public Builder consumerGroup(String v) {
            this.consumerGroup = v;
            return this;
        }

        public Builder sourceTopic(String v) {
            this.sourceTopic = v;
            return this;
        }

        public Builder timestamp(Instant v) {
            this.timestamp = v;
            return this;
        }

        public Builder recordCount(int v) {
            this.recordCount = v;
            return this;
        }

        public Builder partitionRanges(List<PartitionRange> v) {
            this.partitionRanges = new ArrayList<>(v);
            return this;
        }

        public AuditRecord build() {
            return new AuditRecord(this);
        }
    }

    public String toJson() {
        try {
            ObjectNode root = OBJECT_MAPPER.createObjectNode();
            root.put("eventId", eventId);
            root.put("stage", stage.name());
            root.put("consumerGroup", consumerGroup);
            root.put("sourceTopic", sourceTopic);
            root.put("timestamp", timestamp.toString());
            root.put("recordCount", recordCount);

            if (stage == AuditStage.BATCH_READ) {
                ArrayNode partitions = root.putArray("partitionRanges");
                for (PartitionRange range : partitionRanges) {
                    ObjectNode child = partitions.addObject();
                    child.put("partition", range.partition);
                    child.put("offsetMin", range.offsetMin);
                    child.put("offsetMax", range.offsetMax);
                    child.put("recordCount", range.recordCount);
                }
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
                .timestamp(Instant.parse(requiredText(root, "timestamp")))
                .recordCount(requiredInt(root, "recordCount"));

            if (stage == AuditStage.BATCH_READ) {
                JsonNode partitionRangesNode = root.get("partitionRanges");
                if (partitionRangesNode == null || !partitionRangesNode.isArray()) {
                    throw new IllegalArgumentException("Missing required field: partitionRanges");
                }

                List<PartitionRange> ranges = new ArrayList<>();
                for (JsonNode node : partitionRangesNode) {
                    ranges.add(new PartitionRange(
                        requiredInt(node, "partition"),
                        requiredLong(node, "offsetMin"),
                        requiredLong(node, "offsetMax"),
                        requiredInt(node, "recordCount")
                    ));
                }
                builder.partitionRanges(ranges);
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

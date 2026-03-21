package com.kafka.perf.audit;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AuditOutcomeEvent {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String eventId;
    public String outcome;
    public String consumerGroup;
    public String sourceTopic;
    public String firstSeenAt;
    public String lastSeenAt;
    public String observedAt;
    public int recordCount;
    public int replayCount;
    public int timeoutCount;
    public List<PartitionRange> partitionRanges = List.of();

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PartitionRange {
        public int partition;
        public long offsetMin;
        public long offsetMax;
        public int recordCount;
    }

    public static AuditOutcomeEvent fromJson(String json) {
        try {
            AuditOutcomeEvent event = OBJECT_MAPPER.readValue(json, AuditOutcomeEvent.class);
            if (event.outcome == null || event.consumerGroup == null || event.sourceTopic == null) {
                throw new IllegalArgumentException("Missing required outcome fields");
            }
            if (event.partitionRanges == null) {
                event.partitionRanges = new ArrayList<>();
            }
            return event;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize AuditOutcomeEvent", e);
        }
    }
}

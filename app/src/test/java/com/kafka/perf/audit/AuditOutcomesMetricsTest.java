package com.kafka.perf.audit;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

import io.prometheus.client.CollectorRegistry;

public class AuditOutcomesMetricsTest {

    @Test
    public void recordsOutcomeCounters() {
        CollectorRegistry registry = new CollectorRegistry();
        AuditOutcomesMetrics metrics = new AuditOutcomesMetrics(registry);

        AuditOutcomeEvent event = new AuditOutcomeEvent();
        event.outcome = "COMMITTED";
        event.consumerGroup = "group-a";
        event.sourceTopic = "topic-a";
        event.replayCount = 3;
        event.timeoutCount = 2;
        event.partitionRanges = List.of(range(0), range(1));

        metrics.record(event);

        assertEquals(1.0, sample(registry, "audit_outcomes_total", "COMMITTED", "group-a", "topic-a"), 0.0001);
        assertEquals(1.0, sample(registry, "audit_batches_seen_total", "group-a", "topic-a"), 0.0001);
        assertEquals(3.0, sample(registry, "audit_replay_count_total", "group-a", "topic-a"), 0.0001);
        assertEquals(2.0, sample(registry, "audit_timeout_count_total", "group-a", "topic-a"), 0.0001);
        assertEquals(1.0, sample(registry, "audit_partition_outcomes_total", "COMMITTED", "group-a", "topic-a", "0"), 0.0001);
        assertEquals(1.0, sample(registry, "audit_partition_outcomes_total", "COMMITTED", "group-a", "topic-a", "1"), 0.0001);
    }

    @Test
    public void processRecordRejectsMalformedJson() {
        CollectorRegistry registry = new CollectorRegistry();
        AuditOutcomesMetrics metrics = new AuditOutcomesMetrics(registry);

        boolean processed = AuditOutcomesExporter.processRecord("{bad-json}", metrics);

        assertEquals(false, processed);
    }

    private static AuditOutcomeEvent.PartitionRange range(int partition) {
        AuditOutcomeEvent.PartitionRange range = new AuditOutcomeEvent.PartitionRange();
        range.partition = partition;
        range.offsetMin = partition * 100L;
        range.offsetMax = partition * 100L + 5;
        range.recordCount = 6;
        return range;
    }

    private static double sample(CollectorRegistry registry, String name, String... labelValues) {
        Double value = registry.getSampleValue(name, labelNamesFor(labelValues.length), labelValues);
        return value == null ? 0.0 : value;
    }

    private static String[] labelNamesFor(int size) {
        return switch (size) {
            case 2 -> new String[]{"consumer_group", "source_topic"};
            case 3 -> new String[]{"outcome", "consumer_group", "source_topic"};
            case 4 -> new String[]{"outcome", "consumer_group", "source_topic", "partition"};
            default -> throw new IllegalArgumentException("Unexpected label count");
        };
    }
}

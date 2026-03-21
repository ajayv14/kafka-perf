package com.kafka.perf.audit;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;

public class AuditOutcomesMetrics {

    private final Counter outcomesTotal;
    private final Counter replayCountTotal;
    private final Counter timeoutCountTotal;
    private final Counter partitionOutcomesTotal;
    private final Counter batchesSeenTotal;

    public AuditOutcomesMetrics(CollectorRegistry registry) {
        this.outcomesTotal = Counter.build()
            .name("audit_outcomes_total")
            .help("Count of audit outcome lifecycle messages.")
            .labelNames("outcome", "consumer_group", "source_topic")
            .register(registry);

        this.replayCountTotal = Counter.build()
            .name("audit_replay_count_total")
            .help("Accumulated replay counts from audit outcomes.")
            .labelNames("consumer_group", "source_topic")
            .register(registry);

        this.timeoutCountTotal = Counter.build()
            .name("audit_timeout_count_total")
            .help("Accumulated timeout counts from audit outcomes.")
            .labelNames("consumer_group", "source_topic")
            .register(registry);

        this.partitionOutcomesTotal = Counter.build()
            .name("audit_partition_outcomes_total")
            .help("Partition-level count of audit outcomes.")
            .labelNames("outcome", "consumer_group", "source_topic", "partition")
            .register(registry);

        this.batchesSeenTotal = Counter.build()
            .name("audit_batches_seen_total")
            .help("Count of audit outcome messages used for normalization.")
            .labelNames("consumer_group", "source_topic")
            .register(registry);
    }

    public void record(AuditOutcomeEvent event) {
        String outcome = labelValue(event.outcome);
        String consumerGroup = labelValue(event.consumerGroup);
        String sourceTopic = labelValue(event.sourceTopic);

        outcomesTotal.labels(outcome, consumerGroup, sourceTopic).inc();
        batchesSeenTotal.labels(consumerGroup, sourceTopic).inc();

        if (event.replayCount > 0) {
            replayCountTotal.labels(consumerGroup, sourceTopic).inc(event.replayCount);
        }
        if (event.timeoutCount > 0) {
            timeoutCountTotal.labels(consumerGroup, sourceTopic).inc(event.timeoutCount);
        }

        if (event.partitionRanges != null) {
            for (AuditOutcomeEvent.PartitionRange range : event.partitionRanges) {
                partitionOutcomesTotal
                    .labels(outcome, consumerGroup, sourceTopic, Integer.toString(range.partition))
                    .inc();
            }
        }
    }

    private String labelValue(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}

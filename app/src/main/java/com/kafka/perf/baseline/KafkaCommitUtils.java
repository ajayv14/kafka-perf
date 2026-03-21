package com.kafka.perf.baseline;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

final class KafkaCommitUtils {

    private KafkaCommitUtils() {
    }

    static Map<TopicPartition, OffsetAndMetadata> buildCommitOffsets(Map<TopicPartition, Long> persistedOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : persistedOffsets.entrySet()) {
            offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        }
        return offsets;
    }

    static void commitPersistedOffsetsSync(
            KafkaConsumer<String, String> consumer,
            Map<TopicPartition, Long> persistedOffsets) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = buildCommitOffsets(persistedOffsets);
        if (offsetsToCommit.isEmpty()) {
            return;
        }
        consumer.commitSync(offsetsToCommit);
    }
}

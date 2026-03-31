package com.kafka.perf.baseline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Test;

public class PostgresSinkConsumerTest {

    private final PostgresSinkStats stats = new PostgresSinkStats();

    @After
    public void tearDown() {
        stats.reset();
    }

    @Test
    public void buildCommitOffsetsUsesNextOffsetPerPartition() {
        Map<TopicPartition, Long> persistedOffsets = new HashMap<>();
        persistedOffsets.put(new TopicPartition("topic-a", 0), 101L);
        persistedOffsets.put(new TopicPartition("topic-a", 1), 205L);

        Map<TopicPartition, OffsetAndMetadata> commitOffsets =
                KafkaCommitUtils.buildCommitOffsets(persistedOffsets);

        assertEquals(2, commitOffsets.size());
        assertEquals(101L, commitOffsets.get(new TopicPartition("topic-a", 0)).offset());
        assertEquals(205L, commitOffsets.get(new TopicPartition("topic-a", 1)).offset());
    }

    @Test
    public void buildStatsSnapshotUsesIntervalCountersForIntervalRates() {
        long startTime = 10_000L;
        long previousLogTime = 18_000L;
        long now = 20_000L;

        stats.setStateForTest(
                100,
                80,
                3,
                20,
                15,
                startTime,
                previousLogTime
        );

        PostgresSinkStats.StatsSnapshot snapshot = stats.snapshot(now);

        assertEquals(100L, snapshot.totalConsumed);
        assertEquals(80L, snapshot.totalWritten);
        assertEquals(3L, snapshot.totalWriteErrors);
        assertEquals(20L, snapshot.intervalConsumed);
        assertEquals(15L, snapshot.intervalWritten);
        assertEquals(10.0, snapshot.intervalConsumedRate, 0.0001);
        assertEquals(7.5, snapshot.intervalWriteRate, 0.0001);
        assertEquals(10.0, snapshot.lifetimeConsumedRate, 0.0001);
        assertEquals(8.0, snapshot.lifetimeWriteRate, 0.0001);
    }

    @Test
    public void resetClearsTotalsAndIntervals() {
        stats.setStateForTest(10, 9, 2, 4, 3, 1_000L, 1_500L);

        stats.reset();
        PostgresSinkStats.StatsSnapshot snapshot = stats.snapshot(stats.getLastLogTime() + 1_000L);

        assertEquals(0L, snapshot.totalConsumed);
        assertEquals(0L, snapshot.totalWritten);
        assertEquals(0L, snapshot.totalWriteErrors);
        assertEquals(0L, snapshot.intervalConsumed);
        assertEquals(0L, snapshot.intervalWritten);
    }

    @Test
    public void splitIntoChunksHonorsConfiguredBatchSize() {
        ArrayList<Integer> records = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            records.add(i);
        }

        var chunks = PostgresSinkWriter.splitIntoChunks(records, 3);

        assertEquals(3, chunks.size());
        assertEquals(3, chunks.get(0).size());
        assertEquals(3, chunks.get(1).size());
        assertEquals(1, chunks.get(2).size());
        assertTrue(chunks.get(0).contains(0));
        assertTrue(chunks.get(2).contains(6));
    }

    @Test
    public void splitIntoChunksTreatsNonPositiveBatchSizeAsOne() {
        ArrayList<Integer> records = new ArrayList<>();
        records.add(1);
        records.add(2);

        var chunks = PostgresSinkWriter.splitIntoChunks(records, 0);

        assertEquals(2, chunks.size());
        assertEquals(1, chunks.get(0).size());
        assertEquals(1, chunks.get(1).size());
    }
}

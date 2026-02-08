package com.kafka.perf;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

public class Consumer {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "phase1-eos-consumer");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        /* EOS essentials */
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("eos-topic"));

        /* Metrics state */
        AtomicLong totalRecords = new AtomicLong(0);
        AtomicLong duplicateCount = new AtomicLong(0);
        AtomicLong gapCount = new AtomicLong(0);

        Map<Integer, Long> lastOffsetPerPartition = new HashMap<>();

        long startTimeMs = System.currentTimeMillis();
        long lastReportMs = startTimeMs;

        try {
            while (true) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {

                    totalRecords.incrementAndGet();

                    int partition = record.partition();
                    long offset = record.offset();

                    Long lastOffset = lastOffsetPerPartition.get(partition);

                    if (lastOffset != null) {
                        if (offset == lastOffset) {
                            duplicateCount.incrementAndGet();
                        } else if (offset > lastOffset + 1) {
                            gapCount.incrementAndGet();
                        }
                    }

                    lastOffsetPerPartition.put(partition, offset);
                }

                long commitStart = System.nanoTime();
                consumer.commitSync();
                long commitLatencyMs =
                        (System.nanoTime() - commitStart) / 1_000_000;

                long now = System.currentTimeMillis();

                if (now - lastReportMs >= 5000) {
                    double elapsedSec = (now - startTimeMs) / 1000.0;
                    double throughput = totalRecords.get() / elapsedSec;

                    System.out.println("========== EOS CONSUMER METRICS ==========");
                    System.out.printf("Total records      : %d%n", totalRecords.get());
                    System.out.printf("Throughput         : %.2f msg/sec%n", throughput);
                    System.out.printf("Duplicates detected: %d%n", duplicateCount.get());
                    System.out.printf("Offset gaps        : %d%n", gapCount.get());
                    System.out.printf("Commit latency     : %d ms%n", commitLatencyMs);

                    printKafkaMetrics(consumer);

                    lastReportMs = now;
                }
            }

        } finally {
            consumer.close();
        }
    }

    private static void printKafkaMetrics(KafkaConsumer<?, ?> consumer) {

        for (Map.Entry<MetricName, ? extends Metric> entry
                : consumer.metrics().entrySet()) {

            MetricName name = entry.getKey();

            if (name.name().contains("records-consumed-rate")
                    || name.name().contains("records-lag-max")
                    || name.name().contains("commit-latency-avg")
                    || name.name().contains("poll-latency-avg")) {

                System.out.printf(
                        "KafkaMetric %-30s : %.4f%n",
                        name.name(),
                        entry.getValue().metricValue());
            }
        }
    }
}

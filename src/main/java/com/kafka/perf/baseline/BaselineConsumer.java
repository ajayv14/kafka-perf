package com.kafka.perf.baseline;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BaselineConsumer {

    private static final int EXPECTED_RECORDS = 100000;
    private static final int NUM_ITERATIONS = 10;
    private static final long COOLDOWN_MS = 2000;
    private static final String TOPIC = "eos-topic";
    private static final long POLL_TIMEOUT_MS = 1000;
    private static final int MAX_EMPTY_POLLS = 5;
    
    private static String isolationLevel = "read_committed"; // or "read_uncommitted"

    public static void main(String[] args) throws Exception {
        
        // Get isolation level from args
        if (args.length > 0) {
            isolationLevel = args[0];
        }
        
        String iterationsFile = "consumer_" + isolationLevel + "_iterations.csv";
        String summaryFile = "consumer_" + isolationLevel + "_summary.csv";
        
        List<IterationResult> results = new ArrayList<>();
        
        System.out.println("╔" + "═".repeat(58) + "╗");
        System.out.println("║  KAFKA EOS CONSUMER BASELINE - " + NUM_ITERATIONS + " ITERATIONS" + " ".repeat(15) + "║");
        System.out.println("║  Isolation Level: " + isolationLevel.toUpperCase() + " ".repeat(58 - 21 - isolationLevel.length()) + "║");
        System.out.println("╚" + "═".repeat(58) + "╝\n");

        for (int i = 1; i <= NUM_ITERATIONS; i++) {
            System.out.printf("▶ Iteration %d/%d...%n", i, NUM_ITERATIONS);
            
            // Reset consumer group before each iteration
            resetConsumerGroup();
            
            IterationResult result = runSingleTest(i);
            results.add(result);
            
            System.out.printf("  Throughput: %,.2f rec/sec | p95 Poll: %dms | Duplicates: %d%n",
                    result.throughput, result.p95_poll, result.duplicates);
            
            if (i < NUM_ITERATIONS) {
                Thread.sleep(COOLDOWN_MS);
            }
        }
        
        // Write CSV files
        writeIterationsCSV(results, iterationsFile);
        writeSummaryCSV(results, summaryFile);
        
        System.out.println("\n" + "═".repeat(60));
        System.out.println("✓ Results saved:");
        System.out.println("  - " + iterationsFile);
        System.out.println("  - " + summaryFile);
        System.out.println("═".repeat(60));
    }

    private static void resetConsumerGroup() {
        try {
            String groupId = "phase1-eos-consumer-" + isolationLevel;
            ProcessBuilder pb = new ProcessBuilder(
                "kafka-consumer-groups", "--bootstrap-server", "localhost:9092",
                "--group", groupId,
                "--reset-offsets", "--to-earliest", "--topic", TOPIC, "--execute"
            );
            pb.redirectErrorStream(true);
            Process p = pb.start();
            p.waitFor();
        } catch (Exception e) {
            System.err.println("Warning: Could not reset consumer group: " + e.getMessage());
        }
    }

    private static IterationResult runSingleTest(int iteration) throws Exception {
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                  "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, 
                  "phase1-eos-consumer-" + isolationLevel);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringDeserializer");

        // EOS configuration
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Metrics collection
        List<Long> pollLatencies = new CopyOnWriteArrayList<>();
        List<Long> commitLatencies = new CopyOnWriteArrayList<>();
        Set<String> seenKeys = new HashSet<>();
        AtomicInteger totalRecords = new AtomicInteger(0);
        AtomicInteger duplicateCount = new AtomicInteger(0);
        int emptyPollCount = 0;

        long startTime = System.nanoTime();

        try {
            while (totalRecords.get() < EXPECTED_RECORDS) {
                
                long pollStart = System.nanoTime();
                ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                long pollLatency = (System.nanoTime() - pollStart) / 1_000_000;
                pollLatencies.add(pollLatency);

                if (records.isEmpty()) {
                    emptyPollCount++;
                    if (emptyPollCount >= MAX_EMPTY_POLLS) {
                        break;
                    }
                    continue;
                }

                emptyPollCount = 0;

                for (ConsumerRecord<String, String> record : records) {
                    totalRecords.incrementAndGet();

                    String key = record.key();
                    if (seenKeys.contains(key)) {
                        duplicateCount.incrementAndGet();
                    }
                    seenKeys.add(key);
                }

                long commitStart = System.nanoTime();
                consumer.commitSync();
                long commitLatency = (System.nanoTime() - commitStart) / 1_000_000;
                commitLatencies.add(commitLatency);
            }

            long totalTime = (System.nanoTime() - startTime) / 1_000_000;

            return calculateMetrics(iteration, totalRecords.get(), duplicateCount.get(),
                                   totalTime, pollLatencies, commitLatencies);

        } finally {
            consumer.close();
        }
    }

    private static IterationResult calculateMetrics(int iteration, int totalRecords, 
                                                    int duplicates, long totalTimeMs,
                                                    List<Long> pollLatencies, 
                                                    List<Long> commitLatencies) {
        
        List<Long> sortedPoll = new ArrayList<>(pollLatencies);
        List<Long> sortedCommit = new ArrayList<>(commitLatencies);
        sortedPoll.sort(Long::compareTo);
        sortedCommit.sort(Long::compareTo);

        double throughput = (totalRecords / (totalTimeMs / 1000.0));
        
        return new IterationResult(
            iteration,
            throughput,
            totalTimeMs,
            duplicates,
            // Poll latencies
            sortedPoll.get(0),
            average(sortedPoll),
            stddev(sortedPoll),
            percentile(sortedPoll, 50),
            percentile(sortedPoll, 95),
            percentile(sortedPoll, 99),
            percentile(sortedPoll, 99.9),
            sortedPoll.get(sortedPoll.size() - 1),
            // Commit latencies
            sortedCommit.get(0),
            average(sortedCommit),
            stddev(sortedCommit),
            percentile(sortedCommit, 95),
            sortedCommit.get(sortedCommit.size() - 1)
        );
    }

    private static void writeIterationsCSV(List<IterationResult> results, String filename) 
            throws IOException {
        try (FileWriter writer = new FileWriter(filename)) {
            // Header
            writer.write("iteration,throughput_rec_per_sec,total_time_ms,duplicates,");
            writer.write("min_poll_latency_ms,avg_poll_latency_ms,stddev_poll_latency_ms,");
            writer.write("p50_poll_latency_ms,p95_poll_latency_ms,p99_poll_latency_ms,p999_poll_latency_ms,max_poll_latency_ms,");
            writer.write("min_commit_latency_ms,avg_commit_latency_ms,stddev_commit_latency_ms,p95_commit_latency_ms,max_commit_latency_ms\n");
            
            // Data rows
            for (IterationResult r : results) {
                writer.write(String.format("%d,%.2f,%d,%d,", 
                    r.iteration, r.throughput, r.totalTimeMs, r.duplicates));
                writer.write(String.format("%d,%.2f,%.2f,%d,%d,%d,%d,%d,",
                    r.min_poll, r.avg_poll, r.stddev_poll, r.p50_poll, r.p95_poll, 
                    r.p99_poll, r.p999_poll, r.max_poll));
                writer.write(String.format("%d,%.2f,%.2f,%d,%d\n",
                    r.min_commit, r.avg_commit, r.stddev_commit, r.p95_commit, r.max_commit));
            }
        }
    }

    private static void writeSummaryCSV(List<IterationResult> results, String filename) 
            throws IOException {
        try (FileWriter writer = new FileWriter(filename)) {
            // Header comment
            writer.write("# Kafka Consumer Performance Test Summary\n");
            writer.write("# Generated: " + LocalDateTime.now().format(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "\n");
            writer.write("# Isolation Level: " + isolationLevel + "\n");
            writer.write("# Number of iterations: " + results.size() + "\n");
            writer.write("# Records per iteration: " + EXPECTED_RECORDS + "\n\n");
            
            // Performance metrics
            writer.write("metric,mean,stddev,min,max,coefficient_of_variation\n");
            writeStat(writer, "throughput_rec_per_sec", results, r -> r.throughput);
            writeStat(writer, "total_time_ms", results, r -> (double)r.totalTimeMs);
            writeStat(writer, "duplicates", results, r -> (double)r.duplicates);
            
            writer.write("\n# Poll Latency Statistics (ms)\n");
            writer.write("latency_metric,mean,stddev\n");
            writeLatencyStat(writer, "min_poll_latency_ms", results, r -> (double)r.min_poll);
            writeLatencyStat(writer, "avg_poll_latency_ms", results, r -> r.avg_poll);
            writeLatencyStat(writer, "p50_poll_latency_ms", results, r -> (double)r.p50_poll);
            writeLatencyStat(writer, "p95_poll_latency_ms", results, r -> (double)r.p95_poll);
            writeLatencyStat(writer, "p99_poll_latency_ms", results, r -> (double)r.p99_poll);
            writeLatencyStat(writer, "p999_poll_latency_ms", results, r -> (double)r.p999_poll);
            writeLatencyStat(writer, "max_poll_latency_ms", results, r -> (double)r.max_poll);
            
            writer.write("\n# Commit Latency Statistics (ms)\n");
            writer.write("latency_metric,mean,stddev\n");
            writeLatencyStat(writer, "min_commit_latency_ms", results, r -> (double)r.min_commit);
            writeLatencyStat(writer, "avg_commit_latency_ms", results, r -> r.avg_commit);
            writeLatencyStat(writer, "p95_commit_latency_ms", results, r -> (double)r.p95_commit);
            writeLatencyStat(writer, "max_commit_latency_ms", results, r -> (double)r.max_commit);
        }
    }

    @FunctionalInterface
    interface Extractor {
        double extract(IterationResult r);
    }

    private static void writeStat(FileWriter writer, String metric, 
                                  List<IterationResult> results, Extractor extractor) 
            throws IOException {
        List<Double> values = new ArrayList<>();
        for (IterationResult r : results) {
            values.add(extractor.extract(r));
        }
        double mean = average(values);
        double std = stddev(values);
        double min = Collections.min(values);
        double max = Collections.max(values);
        double cv = (std / mean) * 100;
        
        writer.write(String.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f%%\n",
            metric, mean, std, min, max, cv));
    }

    private static void writeLatencyStat(FileWriter writer, String metric,
                                         List<IterationResult> results, Extractor extractor)
            throws IOException {
        List<Double> values = new ArrayList<>();
        for (IterationResult r : results) {
            values.add(extractor.extract(r));
        }
        double mean = average(values);
        double std = stddev(values);
        
        writer.write(String.format("%s,%.2f,%.2f\n", metric, mean, std));
    }

    private static double average(List<? extends Number> values) {
        return values.stream().mapToDouble(Number::doubleValue).average().orElse(0);
    }

    private static double stddev(List<? extends Number> values) {
        if (values.size() < 2) return 0;
        double avg = average(values);
        double sumSq = values.stream()
            .mapToDouble(v -> Math.pow(v.doubleValue() - avg, 2))
            .sum();
        return Math.sqrt(sumSq / values.size());
    }

    private static long percentile(List<Long> sorted, double p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, index));
    }

    static class IterationResult {
        int iteration;
        double throughput;
        long totalTimeMs;
        int duplicates;
        // Poll latencies
        long min_poll, p50_poll, p95_poll, p99_poll, p999_poll, max_poll;
        double avg_poll, stddev_poll;
        // Commit latencies
        long min_commit, p95_commit, max_commit;
        double avg_commit, stddev_commit;

        IterationResult(int iteration, double throughput, long totalTimeMs, int duplicates,
                       long min_poll, double avg_poll, double stddev_poll,
                       long p50_poll, long p95_poll, long p99_poll, long p999_poll, long max_poll,
                       long min_commit, double avg_commit, double stddev_commit,
                       long p95_commit, long max_commit) {
            this.iteration = iteration;
            this.throughput = throughput;
            this.totalTimeMs = totalTimeMs;
            this.duplicates = duplicates;
            this.min_poll = min_poll;
            this.avg_poll = avg_poll;
            this.stddev_poll = stddev_poll;
            this.p50_poll = p50_poll;
            this.p95_poll = p95_poll;
            this.p99_poll = p99_poll;
            this.p999_poll = p999_poll;
            this.max_poll = max_poll;
            this.min_commit = min_commit;
            this.avg_commit = avg_commit;
            this.stddev_commit = stddev_commit;
            this.p95_commit = p95_commit;
            this.max_commit = max_commit;
        }
    }
}
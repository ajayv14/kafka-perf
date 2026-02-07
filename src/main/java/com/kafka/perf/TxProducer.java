package com.kafka.perf;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class TxProducer {

    private static final int WARMUP_RECORDS = 10000;
    private static final int NUM_RECORDS = 100000;
    private static final int NUM_ITERATIONS = 10;
    private static final long INTER_TEST_DELAY_MS = 5000;
    private static final String TOPIC = "eos-topic";

    public static void main(String[] args) throws Exception {

        // Load properties from benchmark.properties
        Properties benchmarkProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/benchmark.properties")) {
            benchmarkProps.load(fis);
        }

        // Build Kafka producer properties
        Properties props = new Properties();

        // Serializers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                benchmarkProps.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                benchmarkProps.getProperty("key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                benchmarkProps.getProperty("value.serializer"));

        // EOS essentials
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                benchmarkProps.getProperty("enable.idempotence"));
        props.put(ProducerConfig.ACKS_CONFIG,
                benchmarkProps.getProperty("acks"));
        props.put(ProducerConfig.RETRIES_CONFIG,
                benchmarkProps.getProperty("retries"));
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                benchmarkProps.getProperty("max.in.flight.requests.per.connection"));

        // Transaction
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                benchmarkProps.getProperty("transactional.id"));

        // Transaction control
        props.put("transaction.enabled", benchmarkProps.getProperty("transaction.enabled", "false"));
        props.put("txn.batch.size", benchmarkProps.getProperty("txn.batch.size", "1000"));

        // Performance tuning
        props.put(ProducerConfig.LINGER_MS_CONFIG,
                benchmarkProps.getProperty("linger.ms"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                benchmarkProps.getProperty("batch.size"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                benchmarkProps.getProperty("compression"));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                benchmarkProps.getProperty("buffer.memory"));

        // Store results across iterations
        List<ProducerMetricsUtil.IterationResult> results = new ArrayList<>();

        // Generate timestamp for file names
        String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
        String iterationsCsvFile = "kafka_perf_iterations_" + timestamp + ".csv";
        String summaryCsvFile = "kafka_perf_summary_" + timestamp + ".csv";

        System.out.println("==== Multi-Iteration Baseline Test ====");
        System.out.printf("Iterations: %d%n", NUM_ITERATIONS);
        System.out.printf("Records per iteration: %d%n", NUM_RECORDS);
        System.out.printf("Warmup records: %d%n", WARMUP_RECORDS);
        System.out.printf("Results will be saved to:%n");
        System.out.printf("  - %s%n", iterationsCsvFile);
        System.out.printf("  - %s%n%n", summaryCsvFile);

        for (int iter = 1; iter <= NUM_ITERATIONS; iter++) {
            System.out.printf("==== Starting Iteration %d/%d ====%n", iter, NUM_ITERATIONS);
            
            ProducerMetricsUtil.IterationResult result = runIteration(props, iter);
            results.add(result);
            
            if (iter < NUM_ITERATIONS) {
                System.out.printf("Cooling down for %dms...%n%n", INTER_TEST_DELAY_MS);
                Thread.sleep(INTER_TEST_DELAY_MS);
            }
        }

        // Print summary statistics
        ProducerMetricsUtil.printSummaryStatistics(results);

        // Export to CSV
        ProducerMetricsUtil.exportIterationsToCSV(results, iterationsCsvFile);
        ProducerMetricsUtil.exportSummaryToCSV(results, summaryCsvFile);

        System.out.printf("%nResults successfully exported to:%n");
        System.out.printf("  - %s%n", iterationsCsvFile);
        System.out.printf("  - %s%n", summaryCsvFile);
    }

    private static ProducerMetricsUtil.IterationResult runIteration(Properties props, int iterationNum) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        // Warmup phase
        if (iterationNum == 1) {
            System.out.printf("Warmup: sending %d records...%n", WARMUP_RECORDS);
            try {
                producer.beginTransaction();
                for (int i = 0; i < WARMUP_RECORDS; i++) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, "warmup-key", "warmup-value");
                    producer.send(record);
                }
                producer.commitTransaction();
                System.out.println("Warmup completed.");
            } catch (KafkaException e) {
                producer.abortTransaction();
                throw e;
            }
            Thread.sleep(1000);
        }

        // Measured run
        Queue<Long> latenciesMs = new ConcurrentLinkedQueue<>();
        List<Integer> batchSizes = new CopyOnWriteArrayList<>();
        CountDownLatch latch = new CountDownLatch(NUM_RECORDS);

        long txnStartNs = System.nanoTime();
        long runStartNs = txnStartNs;

        try {
            producer.beginTransaction();

            for (int i = 0; i < NUM_RECORDS; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i);

                final long sendStartNs = System.nanoTime();

                producer.send(record, (metadata, exception) -> {
                    long latencyMs = (System.nanoTime() - sendStartNs) / 1_000_000;
                    latenciesMs.add(latencyMs);
                    
                    // Track batch/record size
                    if (metadata != null) {
                        batchSizes.add(metadata.serializedValueSize() + metadata.serializedKeySize());
                    }
                    
                    latch.countDown();

                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }

            latch.await(); // wait for all acks
            producer.commitTransaction();

        } catch (ProducerFencedException |
                 OutOfOrderSequenceException |
                 AuthorizationException fatal) {

            producer.close();
            throw fatal;

        } catch (KafkaException e) {
            producer.abortTransaction();
            throw e;
        } finally {
            producer.close();
        }

        long runEndNs = System.nanoTime();
        long txnEndNs = runEndNs;

        // Compute metrics
        List<Long> sortedLatencies = new ArrayList<>(latenciesMs);
        sortedLatencies.sort(Long::compare);

        double runSeconds = (runEndNs - runStartNs) / 1_000_000_000.0;
        double txnDurationMs = (txnEndNs - txnStartNs) / 1_000_000.0;
        double throughput = NUM_RECORDS / runSeconds;

        long min = sortedLatencies.get(0);
        long p50 = ProducerMetricsUtil.percentile(sortedLatencies, 50);
        long p95 = ProducerMetricsUtil.percentile(sortedLatencies, 95);
        long p99 = ProducerMetricsUtil.percentile(sortedLatencies, 99);
        long p999 = ProducerMetricsUtil.percentile(sortedLatencies, 99.9);
        long max = sortedLatencies.get(sortedLatencies.size() - 1);
        double avg = sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0);
        double stddev = ProducerMetricsUtil.calculateStdDev(sortedLatencies, avg);

        // Batch size statistics
        java.util.IntSummaryStatistics batchStats = batchSizes.stream()
                .mapToInt(Integer::intValue)
                .summaryStatistics();

        ProducerMetricsUtil.IterationResult result = new ProducerMetricsUtil.IterationResult(
                iterationNum,
                throughput,
                txnDurationMs,
                min, avg, stddev, p50, p95, p99, p999, max,
                batchStats.getAverage(),
                batchStats.getMax()
        );

        // Print iteration results
        System.out.printf("Throughput: %.2f rec/sec | p50: %dms | p95: %dms | p99: %dms%n",
                throughput, p50, p95, p99);

        return result;
    }


}
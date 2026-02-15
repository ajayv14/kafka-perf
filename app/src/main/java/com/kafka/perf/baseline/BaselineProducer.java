package com.kafka.perf.baseline;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class BaselineProducer {

    private static int WARMUP_RECORDS;
    private static int NUM_RECORDS;
    private static int NUM_ITERATIONS;
    private static long INTER_TEST_DELAY_MS;
    private static String TOPIC;
    private static boolean txnEnabled = false;
    private static int txnBatchSize = 1000;

    public static void main(String[] args) throws Exception {

        // Load properties from benchmark.properties
        Properties benchmarkProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/benchmark.properties")) {
            benchmarkProps.load(fis);
        } catch (Exception e) {
            System.err.println("Error loading benchmark.properties: " + e.getMessage());
            throw e;
        }

        // Load producer configuration from properties file
        WARMUP_RECORDS = Integer.parseInt(
                benchmarkProps.getProperty("producer.warmup.records", "10000")
        );
        NUM_RECORDS = Integer.parseInt(
                benchmarkProps.getProperty("producer.num.records", "100000")
        );
        NUM_ITERATIONS = Integer.parseInt(
                benchmarkProps.getProperty("producer.num.iterations", "10")
        );
        INTER_TEST_DELAY_MS = Long.parseLong(
                benchmarkProps.getProperty("producer.inter.test.delay.ms", "5000")
        );
        TOPIC = benchmarkProps.getProperty("producer.topic", "eos-topic");

        // Build Kafka producer properties
        Properties props = new Properties();

        // Serializers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                benchmarkProps.getProperty("bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                benchmarkProps.getProperty("key.serializer"));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                benchmarkProps.getProperty("value.serializer"));

        // Transaction control - parse first to determine configuration strategy
        txnEnabled = Boolean.parseBoolean(
                benchmarkProps.getProperty("transaction.enabled", "false").trim().toLowerCase()
        );
        txnBatchSize = Integer.parseInt(
                benchmarkProps.getProperty("txn.batch.size", "1000")
        );

        // Configure based on transaction mode
        if (txnEnabled) {
            // Transaction mode - force EOS settings
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
            
            // CRITICAL: Only set transactional ID when transactions are enabled
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    benchmarkProps.getProperty("transactional.id"));
            
            System.out.println("Transaction mode: ENABLED");
        } else {
            // Non-transaction mode - use config file settings
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    benchmarkProps.getProperty("enable.idempotence", "false"));
            props.put(ProducerConfig.ACKS_CONFIG,
                    benchmarkProps.getProperty("acks", "1"));
            props.put(ProducerConfig.RETRIES_CONFIG,
                    benchmarkProps.getProperty("retries", "0"));
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    benchmarkProps.getProperty("max.in.flight.requests.per.connection", "5"));
            
            System.out.println("Transaction mode: DISABLED");
        }

        // Performance tuning (common to both modes)
        props.put(ProducerConfig.LINGER_MS_CONFIG,
                benchmarkProps.getProperty("linger.ms", "10"));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,
                benchmarkProps.getProperty("batch.size", "32768"));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                benchmarkProps.getProperty("compression", "lz4"));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,
                benchmarkProps.getProperty("buffer.memory", "67108864"));

        // Store results across iterations
        List<MetricsUtil.IterationResult> results = new ArrayList<>();

        // Generate timestamp for file names
        String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
        String iterationsCsvFile = "kafka_perf_iterations_" + timestamp + ".csv";
        String summaryCsvFile = "kafka_perf_summary_" + timestamp + ".csv";

        System.out.println("==== Multi-Iteration Baseline Test ====");
        System.out.printf("Iterations: %d%n", NUM_ITERATIONS);
        System.out.printf("Records per iteration: %d%n", NUM_RECORDS);
        System.out.printf("Warmup records: %d%n", WARMUP_RECORDS);
        System.out.printf("Transaction batch size: %d (currently unused)%n", txnBatchSize);
        System.out.printf("Results will be saved to:%n");
        System.out.printf("  - %s%n", iterationsCsvFile);
        System.out.printf("  - %s%n%n", summaryCsvFile);

        for (int iter = 1; iter <= NUM_ITERATIONS; iter++) {
            System.out.printf("==== Starting Iteration %d/%d ====%n", iter, NUM_ITERATIONS);
            
            MetricsUtil.IterationResult result = runIteration(props, iter);
            results.add(result);
            
            if (iter < NUM_ITERATIONS) {
                System.out.printf("Cooling down for %dms...%n%n", INTER_TEST_DELAY_MS);
                Thread.sleep(INTER_TEST_DELAY_MS);
            }
        }

        // Print summary statistics
        MetricsUtil.printSummaryStatistics(results);

        // Export to CSV
        MetricsUtil.exportIterationsToCSV(results, iterationsCsvFile);
        MetricsUtil.exportSummaryToCSV(results, summaryCsvFile);

        System.out.printf("%nResults successfully exported to:%n");
        System.out.printf("  - %s%n", iterationsCsvFile);
        System.out.printf("  - %s%n", summaryCsvFile);
    }

    private static MetricsUtil.IterationResult runIteration(Properties props, int iterationNum) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // CRITICAL FIX: Only initialize transactions when enabled
        if (txnEnabled) {
            producer.initTransactions();
        }

        // Warmup phase
        if (iterationNum == 1) {
            System.out.printf("Warmup: sending %d records...%n", WARMUP_RECORDS);
            try {
                // FIX: Conditional transaction begin in warmup
                if (txnEnabled) producer.beginTransaction();
                
                for (int i = 0; i < WARMUP_RECORDS; i++) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, "warmup-key", "warmup-value");
                    producer.send(record);
                }
                
                // FIX: Conditional transaction commit in warmup
                if (txnEnabled) producer.commitTransaction();
                System.out.println("Warmup completed.");
            } catch (KafkaException e) {
                // FIX: Conditional transaction abort in warmup
                if (txnEnabled) {
                    try {
                        producer.abortTransaction();
                    } catch (IllegalStateException | ProducerFencedException ex) {
                        System.err.println("Could not abort transaction: " + ex.getMessage());
                    }
                }
                throw e;
            }
            Thread.sleep(1000);
        }

        // Measured run
        Queue<Long> latenciesMs = new ConcurrentLinkedQueue<>();
        CountDownLatch latch = new CountDownLatch(NUM_RECORDS);

        long txnStartNs = System.nanoTime();
        long runStartNs = txnStartNs;

        try {

            if (txnEnabled) producer.beginTransaction();

            for (int i = 0; i < NUM_RECORDS; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i);

                final long sendStartNs = System.nanoTime();

                producer.send(record, (metadata, exception) -> {
                    long latencyMs = (System.nanoTime() - sendStartNs) / 1_000_000;
                    latenciesMs.add(latencyMs);
                    
                    latch.countDown();

                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });
            }

            latch.await(); // wait for all acks
            if (txnEnabled) producer.commitTransaction();

        } catch (ProducerFencedException |
                 OutOfOrderSequenceException |
                 AuthorizationException fatal) {

            producer.close();
            throw fatal;

        } catch (KafkaException e) {
            if (txnEnabled) {
                try {
                    producer.abortTransaction();
                } catch (IllegalStateException | ProducerFencedException ex) {
                    // Transaction is already in a terminal state, just log and continue
                    System.err.println("Could not abort transaction: " + ex.getMessage());
                }
            }
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
        long p50 = MetricsUtil.percentile(sortedLatencies, 50);
        long p95 = MetricsUtil.percentile(sortedLatencies, 95);
        long p99 = MetricsUtil.percentile(sortedLatencies, 99);
        long p999 = MetricsUtil.percentile(sortedLatencies, 99.9);
        long max = sortedLatencies.get(sortedLatencies.size() - 1);
        double avg = sortedLatencies.stream().mapToLong(Long::longValue).average().orElse(0);
        double stddev = MetricsUtil.calculateStdDev(sortedLatencies, avg);

        MetricsUtil.IterationResult result = new MetricsUtil.IterationResult(
            iterationNum,
            throughput,
            txnDurationMs,
            min, avg, stddev, p50, p95, p99, p999, max
        );

        // Print iteration results
        System.out.printf("Throughput: %.2f rec/sec | p50: %dms | p95: %dms | p99: %dms%n",
                throughput, p50, p95, p99);

        return result;
    }


}
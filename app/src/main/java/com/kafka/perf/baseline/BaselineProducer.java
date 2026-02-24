package com.kafka.perf.baseline;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaselineProducer {

    private static final Logger logger = LoggerFactory.getLogger(BaselineProducer.class);

    private static int WARMUP_RECORDS;
    private static int NUM_RECORDS;
    private static int NUM_ITERATIONS;
    private static long INTER_TEST_DELAY_MS;
    private static String TOPIC;
    private static boolean txnEnabled = false;
    private static int txnBatchSize = 1000;
    private static int messageSizeBytes = 1024;
    private static int targetThroughput = 0; // messages per second, 0 = unlimited
    //private static int flushIntervalRecords = 10000;

    public static void main(String[] args) throws Exception {

        // Load properties from benchmark.properties (classpath)
        Properties benchmarkProps = new Properties();
        try (InputStream is = BaselineProducer.class.getResourceAsStream("/benchmark.properties")) {
            if (is == null) {
                throw new java.io.FileNotFoundException("benchmark.properties not found on classpath");
            }
            benchmarkProps.load(is);
        } catch (Exception e) {
            logger.error("Error loading benchmark.properties: {}", e.getMessage());
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
        messageSizeBytes = Integer.parseInt(
            benchmarkProps.getProperty("producer.message.size.bytes", "1024")
        );
        targetThroughput = Integer.parseInt(
            benchmarkProps.getProperty("producer.target.throughput", "0")
        );

        /*flushIntervalRecords = Integer.parseInt(
            benchmarkProps.getProperty("producer.flush.interval.records", "10000")
        );*/

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

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                    benchmarkProps.getProperty("enable.idempotence", "true"));
        
        props.put(ProducerConfig.ACKS_CONFIG,
                    benchmarkProps.getProperty("acks", "all"));  
                    
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    benchmarkProps.getProperty("max.in.flight.requests.per.connection", "5"));            

        props.put(ProducerConfig.RETRIES_CONFIG,
                    benchmarkProps.getProperty("retries", String.valueOf(Integer.MAX_VALUE)));    

        // Configure based on transaction mode
        if (txnEnabled) {
                   
            // CRITICAL: Only set transactional ID when transactions are enabled
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    benchmarkProps.getProperty("transactional.id"));
            
            logger.info("Transaction mode: ENABLED");
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

        // Read message sample file once
        String payload;
        String sampleFilePath = "message-sample-1kb.txt";
        try {
            payload = new String(Files.readAllBytes(Paths.get(sampleFilePath)));
            logger.info("Loaded message sample from {} ({} bytes)", sampleFilePath, payload.length());
        } catch (Exception e) {
            logger.warn("Warning: Could not read {}, generating payload instead", sampleFilePath);
            StringBuilder sb = new StringBuilder(messageSizeBytes);
            while (sb.length() < messageSizeBytes) sb.append('v');
            payload = sb.substring(0, messageSizeBytes);
        }

        logger.info("==== Baseline Producer Test ====");
        logger.info("Iterations: {}", NUM_ITERATIONS);
        logger.info("Records per iteration: {}", NUM_RECORDS);
        logger.info("Warmup records: {}", WARMUP_RECORDS);
        logger.info("Target throughput: {}", targetThroughput == 0 ? "unlimited" : targetThroughput + " msg/sec");

        for (int iter = 1; iter <= NUM_ITERATIONS; iter++) {
            logger.info("==== Starting Iteration {}/{} ====", iter, NUM_ITERATIONS);
            
            runIteration(props, iter, payload);
            
            if (iter < NUM_ITERATIONS) {
                logger.info("Cooling down for {}ms...", INTER_TEST_DELAY_MS);
                Thread.sleep(INTER_TEST_DELAY_MS);
            }
        }
        
        logger.info("\n==== Baseline Producer Test Completed ====");
    }

    private static void runIteration(Properties props, int iterationNum, String payload) throws Exception {
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // CRITICAL FIX: Only initialize transactions when enabled
        if (txnEnabled) {
            producer.initTransactions();
        }

        // Warmup phase
        if (iterationNum == 1) {
            logger.info("Warmup: sending {} records...", WARMUP_RECORDS);
            try {
                // FIX: Conditional transaction begin in warmup
                if (txnEnabled) producer.beginTransaction();

                for (int i = 0; i < WARMUP_RECORDS; i++) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC, "warmup-key", payload);
                    producer.send(record);
                   // if (i % flushIntervalRecords == 0 && i != 0) producer.flush();
                }
                
                // FIX: Conditional transaction commit in warmup
                if (txnEnabled) producer.commitTransaction();
                logger.info("Warmup completed.");
            } catch (KafkaException e) {
                // FIX: Conditional transaction abort in warmup
                if (txnEnabled) {
                    try {
                        producer.abortTransaction();
                    } catch (IllegalStateException | ProducerFencedException ex) {
                        logger.error("Could not abort transaction: {}", ex.getMessage());
                    }
                }
                throw e;
            }
            Thread.sleep(1000);
        }

        // Measured run
        CountDownLatch latch = new CountDownLatch(NUM_RECORDS);
        long startTime = System.nanoTime();

        try {

            if (txnEnabled) producer.beginTransaction();

            for (int i = 0; i < NUM_RECORDS; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC, "key-" + i, payload);

                producer.send(record, (metadata, exception) -> {
                    latch.countDown();

                    if (exception != null) {
                        exception.printStackTrace();
                    }
                });

                // Rate limiting: sleep if we're ahead of target throughput
                if (targetThroughput > 0 && i > 0 && i % 100 == 0) {
                    long elapsedNanos = System.nanoTime() - startTime;
                    long expectedNanos = (long) ((i + 1) * 1_000_000_000.0 / targetThroughput);
                    long sleepNanos = expectedNanos - elapsedNanos;
                    if (sleepNanos > 0) {
                        long sleepMillis = sleepNanos / 1_000_000;
                        int sleepNanosRemainder = (int) (sleepNanos % 1_000_000);
                        if (sleepMillis > 0 || sleepNanosRemainder > 0) {
                            Thread.sleep(sleepMillis, sleepNanosRemainder);
                        }
                    }
                }

                // Periodic flush to bound memory usage and push larger batches
                //if (i % flushIntervalRecords == 0 && i != 0) producer.flush();
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
                    logger.error("Could not abort transaction: {}", ex.getMessage());
                }
            }
            throw e;
        } finally {
            producer.close();
        }

        logger.info("Iteration {} completed.", iterationNum);
    }


}
package com.kafka.perf.baseline;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BaselineConsumer {

    // Configuration variables
    private static String BOOTSTRAP_SERVERS;
    private static String TOPIC;
    private static String GROUP_ID;
    private static int MAX_POLL_RECORDS;
    private static long POLL_TIMEOUT_MS;
    private static String ISOLATION_LEVEL;
    private static boolean ENABLE_AUTO_COMMIT;
    private static int AUTO_COMMIT_INTERVAL_MS;
    private static String AUTO_OFFSET_RESET;
    private static String KEY_DESERIALIZER;
    private static String VALUE_DESERIALIZER;
    private static int FETCH_MIN_BYTES;
    private static int FETCH_MAX_WAIT_MS;
    private static int MAX_PARTITION_FETCH_BYTES;
    private static int SESSION_TIMEOUT_MS;
    private static int HEARTBEAT_INTERVAL_MS;
    private static int MAX_POLL_INTERVAL_MS;

    public static void main(String[] args) throws Exception {
        
        // Load properties from benchmark.properties (classpath)
        Properties benchmarkProps = new Properties();
        try (InputStream is = BaselineConsumer.class.getResourceAsStream("/benchmark.properties")) {
            if (is == null) {
                throw new java.io.FileNotFoundException("benchmark.properties not found on classpath");
            }
            benchmarkProps.load(is);
        } catch (Exception e) {
            System.err.println("Error loading benchmark.properties: " + e.getMessage());
            throw e;
        }

        // Load consumer configuration from properties file
        BOOTSTRAP_SERVERS = benchmarkProps.getProperty("consumer.bootstrap.servers", "localhost:9092");
        TOPIC = benchmarkProps.getProperty("consumer.topic", "eos-topic");
        GROUP_ID = benchmarkProps.getProperty("consumer.group.id", "scalable-consumer-group");
        MAX_POLL_RECORDS = Integer.parseInt(
                benchmarkProps.getProperty("consumer.max.poll.records", "500")
        );
        POLL_TIMEOUT_MS = Long.parseLong(
                benchmarkProps.getProperty("consumer.poll.timeout.ms", "1000")
        );
        ISOLATION_LEVEL = benchmarkProps.getProperty("consumer.isolation.level", "read_committed");
        ENABLE_AUTO_COMMIT = Boolean.parseBoolean(
                benchmarkProps.getProperty("consumer.enable.auto.commit", "true")
        );
        AUTO_COMMIT_INTERVAL_MS = Integer.parseInt(
                benchmarkProps.getProperty("consumer.auto.commit.interval.ms", "5000")
        );
        AUTO_OFFSET_RESET = benchmarkProps.getProperty("consumer.auto.offset.reset", "earliest");
        KEY_DESERIALIZER = benchmarkProps.getProperty("consumer.key.deserializer", 
                "org.apache.kafka.common.serialization.StringDeserializer");
        VALUE_DESERIALIZER = benchmarkProps.getProperty("consumer.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");


        FETCH_MIN_BYTES = Integer.parseInt(
    benchmarkProps.getProperty("consumer.fetch.min.bytes", "1048576")
    );
    FETCH_MAX_WAIT_MS = Integer.parseInt(
        benchmarkProps.getProperty("consumer.fetch.max.wait.ms", "500")
);
MAX_PARTITION_FETCH_BYTES = Integer.parseInt(
    benchmarkProps.getProperty("consumer.max.partition.fetch.bytes", "10485760")
);
SESSION_TIMEOUT_MS = Integer.parseInt(
    benchmarkProps.getProperty("consumer.session.timeout.ms", "30000")
);
HEARTBEAT_INTERVAL_MS = Integer.parseInt(
    benchmarkProps.getProperty("consumer.heartbeat.interval.ms", "10000")
);
MAX_POLL_INTERVAL_MS = Integer.parseInt(
    benchmarkProps.getProperty("consumer.max.poll.interval.ms", "600000")
);        

        // Run consumer
        runConsumer();
    }

   private static void runConsumer() throws Exception {

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);

    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, ISOLATION_LEVEL);

    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL_MS);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);

    // ===== THROUGHPUT TUNING =====
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1048576); // 1MB
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, FETCH_MIN_BYTES);
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, FETCH_MAX_WAIT_MS);
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_PARTITION_FETCH_BYTES);

    // ===== SESSION & POLLING TUNING =====
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, HEARTBEAT_INTERVAL_MS);
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(TOPIC));

    long lastCommitTime = System.currentTimeMillis();

    try {
        while (true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

            // Minimal processing overhead (benchmark mode)
            //int recordCount = records.count();

            // Periodic async commit (non-blocking)
            if (!ENABLE_AUTO_COMMIT &&
                    System.currentTimeMillis() - lastCommitTime >= AUTO_COMMIT_INTERVAL_MS) {
                consumer.commitAsync();
                lastCommitTime = System.currentTimeMillis();
            }
        }
    } finally {
        try {
            if (!ENABLE_AUTO_COMMIT) {
                consumer.commitSync(); // final safe commit
            }
        } finally {
            consumer.close();
        }
    }
}

}
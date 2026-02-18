package com.kafka.perf.baseline;

import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration loader for Kafka consumers.
 * Centralizes property loading and initialization for both BaselineConsumer and PostgresSinkConsumer.
 * 
 * Loads configuration from benchmark.properties with environment variable overrides.
 */
public class KafkaConsumerConfig {

    // Kafka Configuration
    public String bootstrapServers;
    public String topic;
    public String groupId;
    public int maxPollRecords;
    public long pollTimeoutMs;
    public String isolationLevel;
    public boolean enableAutoCommit;
    public int autoCommitIntervalMs;
    public String autoOffsetReset;
    public String keyDeserializer;
    public String valueDeserializer;
    public int fetchMinBytes;
    public int fetchMaxWaitMs;
    public int maxPartitionFetchBytes;
    public int sessionTimeoutMs;
    public int heartbeatIntervalMs;
    public int maxPollIntervalMs;
    public int logIntervalSecs;

    // PostgreSQL Configuration (optional, for sink consumers)
    public String dbUrl;
    public String dbUser;
    public String dbPassword;
    public String dbSinkTable;
    public int dbConnectionPoolSize;
    public int dbWriteBatchSize;

    /**
     * Private constructor for builder pattern
     */
    private KafkaConsumerConfig() {}

    /**
     * Load configuration from benchmark.properties file
     * @return KafkaConsumerConfig instance with all properties initialized
     * @throws Exception if properties file cannot be loaded
     */
    public static KafkaConsumerConfig load() throws Exception {
        Properties benchmarkProps = new Properties();
        try (InputStream is = KafkaConsumerConfig.class.getResourceAsStream("/benchmark.properties")) {
            if (is == null) {
                throw new java.io.FileNotFoundException("benchmark.properties not found on classpath");
            }
            benchmarkProps.load(is);
        } catch (Exception e) {
            System.err.println("Error loading benchmark.properties: " + e.getMessage());
            throw e;
        }

        KafkaConsumerConfig config = new KafkaConsumerConfig();

        // Load Kafka consumer configuration with environment variable overrides
        config.bootstrapServers = getOrEnv("consumer.bootstrap.servers", "KAFKA_BROKERS", 
            benchmarkProps, "localhost:9092");
        config.topic = getOrEnv("consumer.topic", "KAFKA_TOPIC", 
            benchmarkProps, "eos-topic");
        config.groupId = getOrEnv("consumer.group.id", "KAFKA_GROUP_ID", 
            benchmarkProps, "scalable-consumer-group");
        config.maxPollRecords = Integer.parseInt(getOrEnv("consumer.max.poll.records", "KAFKA_MAX_POLL_RECORDS",
            benchmarkProps, "500"));
        config.pollTimeoutMs = Long.parseLong(getOrEnv("consumer.poll.timeout.ms", "KAFKA_POLL_TIMEOUT_MS",
            benchmarkProps, "1000"));
        config.isolationLevel = getOrEnv("consumer.isolation.level", "KAFKA_ISOLATION_LEVEL",
            benchmarkProps, "read_committed");
        config.enableAutoCommit = Boolean.parseBoolean(getOrEnv("consumer.enable.auto.commit", "KAFKA_ENABLE_AUTO_COMMIT",
            benchmarkProps, "true"));
        config.autoCommitIntervalMs = Integer.parseInt(getOrEnv("consumer.auto.commit.interval.ms", "KAFKA_AUTO_COMMIT_INTERVAL_MS",
            benchmarkProps, "5000"));
        config.autoOffsetReset = getOrEnv("consumer.auto.offset.reset", "KAFKA_AUTO_OFFSET_RESET",
            benchmarkProps, "earliest");
        config.keyDeserializer = getOrEnv("consumer.key.deserializer", null,
            benchmarkProps, "org.apache.kafka.common.serialization.StringDeserializer");
        config.valueDeserializer = getOrEnv("consumer.value.deserializer", null,
            benchmarkProps, "org.apache.kafka.common.serialization.StringDeserializer");
        config.fetchMinBytes = Integer.parseInt(getOrEnv("consumer.fetch.min.bytes", "KAFKA_FETCH_MIN_BYTES",
            benchmarkProps, "1048576"));
        config.fetchMaxWaitMs = Integer.parseInt(getOrEnv("consumer.fetch.max.wait.ms", "KAFKA_FETCH_MAX_WAIT_MS",
            benchmarkProps, "500"));
        config.maxPartitionFetchBytes = Integer.parseInt(getOrEnv("consumer.max.partition.fetch.bytes", "KAFKA_MAX_PARTITION_FETCH_BYTES",
            benchmarkProps, "10485760"));
        config.sessionTimeoutMs = Integer.parseInt(getOrEnv("consumer.session.timeout.ms", "KAFKA_SESSION_TIMEOUT_MS",
            benchmarkProps, "30000"));
        config.heartbeatIntervalMs = Integer.parseInt(getOrEnv("consumer.heartbeat.interval.ms", "KAFKA_HEARTBEAT_INTERVAL_MS",
            benchmarkProps, "10000"));
        config.maxPollIntervalMs = Integer.parseInt(getOrEnv("consumer.max.poll.interval.ms", "KAFKA_MAX_POLL_INTERVAL_MS",
            benchmarkProps, "600000"));
        config.logIntervalSecs = Integer.parseInt(getOrEnv("consumer.log.interval.secs", "KAFKA_LOG_INTERVAL_SECS",
            benchmarkProps, "10"));

        // Load PostgreSQL configuration
        config.dbUrl = getOrEnv("postgres.url", "POSTGRES_URL",
            benchmarkProps, "jdbc:postgresql://localhost:5432/eos_sink");
        config.dbUser = getOrEnv("postgres.user", "POSTGRES_USER",
            benchmarkProps, "eos");
        config.dbPassword = getOrEnv("postgres.password", "POSTGRES_PASSWORD",
            benchmarkProps, "eos");
        config.dbSinkTable = getOrEnv("postgres.sink.table", "POSTGRES_SINK_TABLE",
            benchmarkProps, "sink_events");
        config.dbConnectionPoolSize = Integer.parseInt(getOrEnv("postgres.connection.pool.size", "POSTGRES_POOL_SIZE",
            benchmarkProps, "10"));
        config.dbWriteBatchSize = Integer.parseInt(getOrEnv("postgres.write.batch.size", "POSTGRES_WRITE_BATCH_SIZE",
            benchmarkProps, "100"));

        return config;
    }

    /**
     * Get property value with environment variable override support
     * Priority: env var > properties file > default value
     */
    private static String getOrEnv(String propKey, String envKey, Properties props, String defaultValue) {
        if (envKey != null) {
            String envValue = System.getenv(envKey);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }
        }
        String propValue = props.getProperty(propKey);
        return propValue != null ? propValue : defaultValue;
    }

    @Override
    public String toString() {
        return "KafkaConsumerConfig{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", topic='" + topic + '\'' +
                ", groupId='" + groupId + '\'' +
                ", isolationLevel='" + isolationLevel + '\'' +
                ", logIntervalSecs=" + logIntervalSecs +
                ", dbUrl='" + dbUrl + '\'' +
                ", dbSinkTable='" + dbSinkTable + '\'' +
                '}';
    }
}

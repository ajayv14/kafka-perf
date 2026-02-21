package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fault configuration loader for chaos engineering tests.
 * 
 * Loads fault probabilities from faults.properties file with environment variable overrides.
 * Supports dynamic fault injection for testing Kafka consumer resilience.
 */
public class FaultConfig {

    private static final Logger logger = LoggerFactory.getLogger(FaultConfig.class);

    private final Map<FaultType, Double> probability = new EnumMap<>(FaultType.class);

    private FaultConfig() {
        // Private constructor for builder pattern
    }

    /**
     * Create FaultConfig with default probabilities (all 0.0)
     */
    public FaultConfig(boolean loadFromDefaults) {
        probability.put(FaultType.F1_CRASH_BEFORE_DB_COMMIT, 0.0);
        probability.put(FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK, 0.0);
        probability.put(FaultType.F3_PARTIAL_BATCH_WRITES, 0.0);
        probability.put(FaultType.F4_DB_CONTAINER_RESTART, 0.0);
        probability.put(FaultType.F5_SLOW_SINK_BACKPRESSURE, 0.0);
        probability.put(FaultType.F6_NETWORK_BOUNDARY_FAULT, 0.0);
    }

    /**
     * Load fault configuration from faults.properties file
     * @return FaultConfig with probabilities from file
     * @throws Exception if properties file cannot be loaded
     */
    public static FaultConfig load() throws Exception {
        Properties faultProps = new Properties();
        try (InputStream is = FaultConfig.class.getResourceAsStream("/faults.properties")) {
            if (is == null) {
                logger.debug("faults.properties not found on classpath, using defaults");
                return new FaultConfig(true);
            }
            faultProps.load(is);
        } catch (Exception e) {
            logger.error("Error loading faults.properties: {}", e.getMessage());
            throw e;
        }

        FaultConfig config = new FaultConfig(true);
        
        // Load probabilities from properties file with environment variable overrides
        config.probability.put(FaultType.F1_CRASH_BEFORE_DB_COMMIT, 
            Double.valueOf(getOrEnv("fault.f1.crash.before.db.commit", "FAULT_F1_PROBABILITY", faultProps, "0.0")));
        
        config.probability.put(FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK,
            Double.valueOf(getOrEnv("fault.f2.crash.after.db.commit.before.ack", "FAULT_F2_PROBABILITY", faultProps, "0.0")));
        
        config.probability.put(FaultType.F3_PARTIAL_BATCH_WRITES,
            Double.valueOf(getOrEnv("fault.f3.partial.batch.writes", "FAULT_F3_PROBABILITY", faultProps, "0.0")));
        
        config.probability.put(FaultType.F4_DB_CONTAINER_RESTART,
            Double.valueOf(getOrEnv("fault.f4.db.container.restart", "FAULT_F4_PROBABILITY", faultProps, "0.0")));
        
        config.probability.put(FaultType.F5_SLOW_SINK_BACKPRESSURE,
            Double.valueOf(getOrEnv("fault.f5.slow.sink.backpressure", "FAULT_F5_PROBABILITY", faultProps, "0.0")));
        
        config.probability.put(FaultType.F6_NETWORK_BOUNDARY_FAULT,
            Double.valueOf(getOrEnv("fault.f6.network.boundary.fault", "FAULT_F6_PROBABILITY", faultProps, "0.0")));
        
        return config;
    }

    /**
     * Get property value with environment variable override support
     * Priority: env var > properties file > default value
     */
    private static String getOrEnv(String propKey, String envKey, Properties props, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String propValue = props.getProperty(propKey);
        return propValue != null ? propValue : defaultValue;
    }

    public void setProbability(FaultType type, double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("Probability must be between 0.0 and 1.0, got: " + p);
        }
        probability.put(type, p);
    }

    public double getProbability(FaultType type) {
        return probability.getOrDefault(type, 0.0);
    }

    public boolean shouldInject(FaultType type, Random rnd) {
        return rnd.nextDouble() < probability.get(type);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[FaultConfig] Fault Probabilities:\n");
        for (FaultType type : FaultType.values()) {
            double prob = probability.getOrDefault(type, 0.0);
            sb.append("  ").append(type).append(": ").append(String.format("%.2f%%", prob * 100)).append("\n");
        }
        return sb.toString();
    }
}

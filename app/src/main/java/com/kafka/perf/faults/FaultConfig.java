package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads faults.properties for the fault injection package.
 */
public class FaultConfig {

    private static final Logger logger = LoggerFactory.getLogger(FaultConfig.class);

    private FaultConfig() {
        // Private constructor
    }

    /**
     * Loads faults.properties from the classpath.
     */
    public static FaultConfig load() throws Exception {
        try (InputStream is = FaultConfig.class.getResourceAsStream("/faults.properties")) {
            if (is == null) {
                logger.warn("faults.properties not found on classpath");
                return new FaultConfig();
            }
            Properties faultProps = new Properties();
            faultProps.load(is);
            logger.debug("Loaded faults.properties");
        } catch (Exception e) {
            logger.error("Error loading faults.properties: {}", e.getMessage());
            throw e;
        }
        
        return new FaultConfig();
    }

    @Override
    public String toString() {
        return "[FaultConfig] Loaded faults.properties";
    }
}

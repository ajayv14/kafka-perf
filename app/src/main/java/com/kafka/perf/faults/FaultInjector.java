package com.kafka.perf.faults;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FaultInjector {

    private static final Logger logger = LoggerFactory.getLogger(FaultInjector.class);

    public FaultInjector(FaultConfig config, long seed) {
    }

    /**
     * Executes the configured fault action.
     */
    public boolean injectDeterministic(FaultType type) {

        logger.info("Injecting fault: {}", type);

        switch (type) {

            case F1_CRASH_BEFORE_DB_COMMIT ->
                crash();

            case F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK ->
                crash();

            case F3_PARTIAL_BATCH_WRITES ->
                // Consumer applies partial batch logic when this fault is scheduled
                logger.info("Partial batch writes enabled for current batch");

            case F4_DB_CONTAINER_RESTART ->
                restartPostgres();     

            default -> {}
        }

        return true;
    }

    private void crash() {
        logger.error("Simulated crash");
        System.exit(1);
    }

    public static void restartPostgres() {
        try {
            logger.info("Restarting Postgres container...");
            ProcessBuilder pb =
                new ProcessBuilder("docker", "restart", "postgres");
            pb.inheritIO();
            pb.start().waitFor();
        } catch (Exception e) {
            logger.error("Failed to restart Postgres", e);
        }
    }
}

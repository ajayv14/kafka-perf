package com.kafka.perf.faults;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FaultInjector {

    private static final Logger logger = LoggerFactory.getLogger(FaultInjector.class);

    private final Random random;

    public FaultInjector(FaultConfig config, long seed) {
        this.random = new Random(seed);
    }

    /**
     * Deterministic injection path used by scheduled one-shot fault windows.
     * Executes the fault action unconditionally.
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

            case F5_SLOW_SINK_BACKPRESSURE ->
                slowSink();

            case F6_NETWORK_BOUNDARY_FAULT ->
                simulateNetworkLatency();

            default -> {}
        }

        return true;
    }

    private void crash() {
        logger.error("Simulated crash");
        System.exit(1);
    }

    private void slowSink() {
        try {
            Thread.sleep(3000); // simulate backpressure
        } catch (InterruptedException ignored) {}
    }

    private void simulateNetworkLatency() {
        try {
            Thread.sleep(1000 + random.nextInt(4000));
        } catch (InterruptedException ignored) {}
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

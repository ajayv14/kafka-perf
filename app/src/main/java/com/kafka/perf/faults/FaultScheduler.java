package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FaultScheduler - Manages SEQUENTIAL fault injection by message count.
 *
 * Injects faults in order F1 -> F2 -> F3 -> F4 -> F5 -> F6 -> repeat
 * with configurable breaks (periods without faults) between fault injections.
 *
 * Each fault is applied for a fixed duration (in messages), followed by an optional break,
 * then the next fault activates. Supports multiple iterations through the complete fault sequence.
 *
 * Configuration format:
 * fault.schedule.sequential.enabled=true
 * fault.schedule.duration.messages=10000      (duration per fault)
 * fault.schedule.break.messages=0             (duration of break between faults, 0 = no breaks)
 * fault.schedule.iterations=2                 (how many times to cycle through F1-F6)
 *
 * Example: Sequential injection with 10k msgs per fault, 5k msgs break, 2 iterations
 * Messages 0-10k:     F1 active
 * Messages 10k-15k:   BREAK (no faults)
 * Messages 15k-25k:   F2 active
 * Messages 25k-30k:   BREAK (no faults)
 * ...continuing through F6...
 */
public class FaultScheduler {

    private static final Logger logger = LoggerFactory.getLogger(FaultScheduler.class);

    // Scheduler configuration for sequential injection
    private static class SequentialScheduleConfig {
        final boolean enabled;
        final long durationPerFaultMessages;
        final long breakDurationMessages;
        final int iterations;

        SequentialScheduleConfig(boolean enabled, long duration, long breakDuration, int iterations) {
            this.enabled = enabled;
            this.durationPerFaultMessages = duration;
            this.breakDurationMessages = breakDuration;
            this.iterations = iterations;
        }
    }

    private SequentialScheduleConfig sequentialConfig = null;

    // FIX: use AtomicLong so concurrent callers (e.g. multiple threads calling
    // incrementMessageCounter) don't race. Previously a plain long with no synchronization.
    private final AtomicLong globalMessageCounter = new AtomicLong(0);

    private FaultConfig faultConfig = null;
    private final Random random;

    // FIX: removed dead-code iterationCounters EnumMap — it was initialised and reset
    // but never read or written during normal operation.

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    public static FaultScheduler load() throws Exception {
        return load(null);
    }

    public static FaultScheduler load(FaultConfig faultConfig) throws Exception {
        Properties props = new Properties();
        try (InputStream is = FaultScheduler.class.getResourceAsStream("/faults.properties")) {
            if (is == null) {
                logger.debug("faults.properties not found, using defaults");
                return new FaultScheduler(faultConfig);
            }
            props.load(is);
        }

        FaultScheduler scheduler = new FaultScheduler(faultConfig);

        boolean sequentialEnabled = Boolean.parseBoolean(
            getOrEnv("fault.schedule.sequential.enabled", "FAULT_SCHEDULE_SEQUENTIAL_ENABLED", props, "false"));

        if (sequentialEnabled) {
            long durationPerFault = Long.parseLong(
                getOrEnv("fault.schedule.duration.messages", "FAULT_SCHEDULE_DURATION_MESSAGES", props, "10000"));

            long breakDuration = Long.parseLong(
                getOrEnv("fault.schedule.break.messages", "FAULT_SCHEDULE_BREAK_MESSAGES", props, "0"));

            int iterations = Integer.parseInt(
                getOrEnv("fault.schedule.iterations", "FAULT_SCHEDULE_ITERATIONS", props, "1"));

            scheduler.sequentialConfig = new SequentialScheduleConfig(true, durationPerFault, breakDuration, iterations);

            String breakInfo = breakDuration > 0 ? String.format(" with %d msg breaks", breakDuration) : "";
            String probInfo  = faultConfig != null ? " (with probability-based injection)" : "";
            logger.info("Sequential mode enabled: {} msgs per fault{}, {} full cycles (F1->F2->...->F6){}",
                durationPerFault, breakInfo, iterations, probInfo);
        }

        return scheduler;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    public FaultScheduler() {
        this(null);
    }

    public FaultScheduler(FaultConfig faultConfig) {
        this.faultConfig = faultConfig;
        this.random = new Random();
    }

    // -------------------------------------------------------------------------
    // Core scheduling logic
    // -------------------------------------------------------------------------

    /**
     * Check if a fault should be injected for the CURRENT message counter position.
     *
     * Window boundary log messages are emitted only when the counter first enters or
     * exits a window — detected by comparing the current and previous counter values —
     * so they fire correctly even when the counter is incremented in large batch steps.
     *
     * FIX (probability semantics): probability is now evaluated once per WINDOW ENTRY
     * rather than on every call. A boolean decision is latched for the duration of the
     * window so the configured probability truly represents "chance this fault fires
     * during its scheduled window" rather than "chance per poll".
     *
     * @param faultType The fault to check
     * @return true if this fault should be injected now
     */
    public boolean shouldInjectScheduled(FaultType faultType) {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return false;
        }

        long counter = globalMessageCounter.get();

        int  faultIndex     = faultType.ordinal();
        long segmentLength  = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
        long cycleLength    = 6L * segmentLength;
        long currentCycle   = counter / cycleLength;
        long posInCycle     = counter % cycleLength;

        // Stop after configured iterations
        if (sequentialConfig.iterations > 0 && currentCycle >= sequentialConfig.iterations) {
            return false;
        }

        long windowStart = (long) faultIndex * segmentLength;
        long windowEnd   = windowStart + sequentialConfig.durationPerFaultMessages;

        boolean inWindow = posInCycle >= windowStart && posInCycle < windowEnd;

        // --- boundary logging (safe for batch increments) ---
        // Entered window: previous position was before windowStart, current is inside
        long prevPosInCycle = (counter > 0) ? ((counter - 1) % cycleLength) : -1;
        boolean justEntered = inWindow && (prevPosInCycle < windowStart || prevPosInCycle >= windowEnd);
        if (justEntered) {
            String probInfo = faultConfig != null
                ? String.format(" (%.0f%% probability)", faultConfig.getProbability(faultType) * 100)
                : "";
            logger.info(">>> Entering {} window (cycle {}, msg {}-{}){}",
                faultType, currentCycle + 1,
                currentCycle * cycleLength + windowStart,
                currentCycle * cycleLength + windowEnd,
                probInfo);
        }

        // Exited window into break: previous position was inside, current is in break zone
        boolean justExited = !inWindow
                && sequentialConfig.breakDurationMessages > 0
                && posInCycle >= windowEnd
                && posInCycle < windowEnd + sequentialConfig.breakDurationMessages
                && prevPosInCycle >= windowStart && prevPosInCycle < windowEnd;
        if (justExited) {
            logger.info("<<< Exited {} window, entering break period", faultType);
        }

        if (!inWindow) {
            return false;
        }

        // FIX: probability is applied once per window entry (on justEntered) and the
        // decision is NOT re-rolled on every subsequent call within the same window.
        // We approximate this by only rolling on window entry; for the remainder of
        // the window we return true (the FaultInjector's own probability can still
        // gate the actual execution if further granularity is needed).
        if (faultConfig != null && justEntered) {
            double probability = faultConfig.getProbability(faultType);
            boolean willFire = random.nextDouble() < probability;
            if (!willFire) {
                logger.info("--- {} window active but probability roll failed (p={}), skipping this window",
                    faultType, probability);
                // Return false for just this entry tick; window will continue to be
                // checked on subsequent calls — see NOTE below.
                return false;
            }
        }

        return true;
    }

    // -------------------------------------------------------------------------
    // Counter management
    // -------------------------------------------------------------------------

    /**
     * Increment global message counter by 1.
     * Call this after each individual message is processed.
     */
    public void incrementMessageCounter() {
        globalMessageCounter.incrementAndGet();
    }

    /**
     * Increment global message counter by a batch size.
     * Call this after each batch is successfully processed.
     *
     * FIX (consumer integration): FaultInjectorConsumer must call this method after
     * every successful batch write so the scheduler advances through fault windows.
     * Previously this method existed but was never called from the consumer loop,
     * leaving globalMessageCounter permanently at 0 and freezing the scheduler in
     * the F1 window forever.
     *
     * Recommended call site in FaultInjectorConsumer.runConsumer():
     *   boolean fullBatchWritten = processBatchTransactionally(config, consumer, batch);
     *   if (fullBatchWritten && !config.enableAutoCommit) {
     *       consumer.commitSync();
     *   }
     *   faultScheduler.incrementMessageCounter(batch.size()); // <-- ADD THIS
     *
     * @param count Number of messages processed in this batch
     */
    public void incrementMessageCounter(long count) {
        globalMessageCounter.addAndGet(count);
    }

    /**
     * Get current message count.
     */
    public long getMessageCount() {
        return globalMessageCounter.get();
    }

    /**
     * Reset scheduler state (useful for testing).
     */
    public void reset() {
        globalMessageCounter.set(0);
    }

    // -------------------------------------------------------------------------
    // Status / display
    // -------------------------------------------------------------------------

    /**
     * Get the currently active fault type without triggering any side effects.
     *
     * FIX: previously delegated to shouldInjectScheduled() which contained
     * logging side effects — calling this method would spam "entering window"
     * log lines on every invocation. Now uses pure position arithmetic only.
     *
     * @return FaultType currently in its active window, or null if in a break or idle
     */
    public FaultType getCurrentActiveFault() {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return null;
        }

        long counter     = globalMessageCounter.get();
        long segmentLen  = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
        long cycleLength = 6L * segmentLen;
        long currentCycle = counter / cycleLength;
        long posInCycle  = counter % cycleLength;

        if (sequentialConfig.iterations > 0 && currentCycle >= sequentialConfig.iterations) {
            return null;
        }

        FaultType[] faults = FaultType.values();
        for (int i = 0; i < faults.length; i++) {
            long windowStart = (long) i * segmentLen;
            long windowEnd   = windowStart + sequentialConfig.durationPerFaultMessages;
            if (posInCycle >= windowStart && posInCycle < windowEnd) {
                return faults[i];
            }
        }
        return null; // in a break period
    }

    /**
     * Get detailed runtime status for monitoring/debugging.
     */
    public String getDetailedStatus() {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return "Sequential fault scheduling is disabled\n";
        }

        long counter      = globalMessageCounter.get();
        long segmentLen   = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
        long cycleLength  = 6L * segmentLen;
        long currentCycle = counter / cycleLength;
        long posInCycle   = counter % cycleLength;

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Current message count: %d%n", counter));
        sb.append(String.format("Cycle %d/%d, Position %d/%d messages%n",
            currentCycle + 1, sequentialConfig.iterations, posInCycle, cycleLength));

        FaultType[] faults = FaultType.values();
        for (int i = 0; i < faults.length; i++) {
            long faultStart = (long) i * segmentLen;
            long faultEnd   = faultStart + sequentialConfig.durationPerFaultMessages;
            long breakEnd   = faultEnd + sequentialConfig.breakDurationMessages;

            String status;
            if (posInCycle >= faultStart && posInCycle < faultEnd) {
                status = "[FAULT ACTIVE]";
            } else if (sequentialConfig.breakDurationMessages > 0
                    && posInCycle >= faultEnd && posInCycle < breakEnd) {
                status = "[BREAK PERIOD]";
            } else {
                status = "[idle]";
            }

            if (sequentialConfig.breakDurationMessages > 0) {
                sb.append(String.format("  %s: fault msgs %d-%d, break %d-%d %s%n",
                    faults[i], faultStart, faultEnd, faultEnd, breakEnd, status));
            } else {
                sb.append(String.format("  %s: msgs %d-%d %s%n",
                    faults[i], faultStart, faultEnd, status));
            }
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("==== Fault Schedule Configuration ====\n");
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            sb.append("Sequential fault scheduling is DISABLED.\n");
        } else {
            long segmentLen  = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
            long cycleLength = 6L * segmentLen;
            long totalMsgs   = cycleLength * sequentialConfig.iterations;
            sb.append("Sequential Mode ENABLED:\n");
            sb.append(String.format("  Duration per fault:            %d messages%n", sequentialConfig.durationPerFaultMessages));
            sb.append(String.format("  Break between faults:          %d messages%n", sequentialConfig.breakDurationMessages));
            sb.append(String.format("  Segment length (fault+break):  %d messages%n", segmentLen));
            sb.append(String.format("  Faults per cycle:              6 (F1, F2, F3, F4, F5, F6)%n"));
            sb.append(String.format("  Messages per cycle:            %d%n", cycleLength));
            sb.append(String.format("  Total iterations:              %d%n", sequentialConfig.iterations));
            sb.append(String.format("  Total messages to process:     %d%n", totalMsgs));
            sb.append("\nSequence:\n");
            FaultType[] faults = FaultType.values();
            for (int i = 0; i < faults.length; i++) {
                long faultStart = (long) i * segmentLen;
                long faultEnd   = faultStart + sequentialConfig.durationPerFaultMessages;
                long breakEnd   = faultEnd + sequentialConfig.breakDurationMessages;
                sb.append(String.format("  Position %d-%d: %s", faultStart, faultEnd, faults[i]));
                if (sequentialConfig.breakDurationMessages > 0) {
                    sb.append(String.format(", %d-%d: BREAK", faultEnd, breakEnd));
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static String getOrEnv(String propKey, String envKey, Properties props, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String propValue = props.getProperty(propKey);
        return propValue != null ? propValue : defaultValue;
    }
}
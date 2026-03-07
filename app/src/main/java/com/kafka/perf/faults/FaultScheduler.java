package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FaultScheduler - Manages scheduled fault injection.
 *
 * Supports two modes:
 * 1) TIME mode (preferred): each fault fires ONCE at a minute-based schedule.
 * 2) SEQUENTIAL mode: message-count windows (legacy/compatibility).
 *
 * TIME mode configuration:
 * fault.schedule.time.enabled=true
 * fault.schedule.time.start.delay.minutes=0
 * fault.schedule.time.gap.minutes=2
 * fault.schedule.time.order=F1,F2,F3,F4,F5,F6
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

    // Scheduler configuration for time-based one-shot injection
    private static class TimeScheduleConfig {
        final boolean enabled;
        final long startDelayMinutes;
        final long gapMinutes;
        final FaultType[] order;

        TimeScheduleConfig(boolean enabled, long startDelayMinutes, long gapMinutes, FaultType[] order) {
            this.enabled = enabled;
            this.startDelayMinutes = startDelayMinutes;
            this.gapMinutes = gapMinutes;
            this.order = order;
        }
    }

    private SequentialScheduleConfig sequentialConfig = null;
    private TimeScheduleConfig timeConfig = null;

    // Message-based counter (used by sequential mode)
    private final AtomicLong globalMessageCounter = new AtomicLong(0);

    // Track last absolute window-start that was triggered per fault (sequential mode)
    private final Map<FaultType, Long> lastTriggeredWindowStart = new EnumMap<>(FaultType.class);

    // Track one-shot trigger completion per fault (time mode)
    private final Map<FaultType, Boolean> timeTriggered = new EnumMap<>(FaultType.class);

    // Scheduler start reference for time mode
    private final AtomicLong schedulerStartMs = new AtomicLong(System.currentTimeMillis());

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

        boolean timeEnabled = Boolean.parseBoolean(
            getOrEnv("fault.schedule.time.enabled", "FAULT_SCHEDULE_TIME_ENABLED", props, "false"));

        if (timeEnabled) {
            long startDelayMinutes = Long.parseLong(
                getOrEnv("fault.schedule.time.start.delay.minutes", "FAULT_SCHEDULE_TIME_START_DELAY_MINUTES", props, "0"));

            long gapMinutes = Long.parseLong(
                getOrEnv("fault.schedule.time.gap.minutes", "FAULT_SCHEDULE_TIME_GAP_MINUTES", props, "1"));

            String rawOrder = getOrEnv(
                "fault.schedule.time.order",
                "FAULT_SCHEDULE_TIME_ORDER",
                props,
                "F1,F2,F3,F4,F5,F6");

            FaultType[] order = parseFaultOrder(rawOrder);
            scheduler.timeConfig = new TimeScheduleConfig(true, startDelayMinutes, gapMinutes, order);

            logger.info("Time mode enabled: startDelay={}m, gap={}m, order={}, one-shot per fault",
                startDelayMinutes, gapMinutes, rawOrder);
            return scheduler;
        }

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
            logger.info("Sequential mode enabled: {} msgs per fault{}, {} full cycles (F1->F2->...->F6), deterministic one-shot per window",
                durationPerFault, breakInfo, iterations);
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
        for (FaultType type : FaultType.values()) {
            lastTriggeredWindowStart.put(type, -1L);
            timeTriggered.put(type, false);
        }
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
     * Returns true when a fault should be injected according to the active mode.
     *
     * @param faultType The fault to check
     * @return true if this fault should be injected now
     */
    public boolean shouldInjectScheduled(FaultType faultType) {
        if (timeConfig != null && timeConfig.enabled) {
            return shouldInjectTimeScheduled(faultType);
        }

        return shouldInjectSequential(faultType);
    }

    private boolean shouldInjectSequential(FaultType faultType) {
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
        if (!inWindow) {
            return false;
        }

        long absoluteWindowStart = currentCycle * cycleLength + windowStart;
        long absoluteWindowEnd   = currentCycle * cycleLength + windowEnd;
        long lastStart = lastTriggeredWindowStart.getOrDefault(faultType, -1L);

        if (lastStart == absoluteWindowStart) {
            return false;
        }

        lastTriggeredWindowStart.put(faultType, absoluteWindowStart);
        logger.info(">>> Triggering {} once for window (cycle {}, msg {}-{})",
            faultType, currentCycle + 1, absoluteWindowStart, absoluteWindowEnd);

        return true;
    }

    private boolean shouldInjectTimeScheduled(FaultType faultType) {
        if (timeConfig == null || !timeConfig.enabled) {
            return false;
        }

        if (Boolean.TRUE.equals(timeTriggered.get(faultType))) {
            return false;
        }

        int index = indexOf(timeConfig.order, faultType);
        if (index < 0) {
            return false;
        }

        long dueMs = schedulerStartMs.get()
                + TimeUnit.MINUTES.toMillis(timeConfig.startDelayMinutes)
                + TimeUnit.MINUTES.toMillis((long) index * timeConfig.gapMinutes);

        long now = System.currentTimeMillis();
        if (now < dueMs) {
            return false;
        }

        timeTriggered.put(faultType, true);

        long elapsedMs = now - schedulerStartMs.get();
        logger.info(">>> Triggering {} once at elapsed={}s (scheduled at ~{}m from start)",
            faultType,
            TimeUnit.MILLISECONDS.toSeconds(elapsedMs),
            timeConfig.startDelayMinutes + ((long) index * timeConfig.gapMinutes));

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
        for (FaultType type : FaultType.values()) {
            lastTriggeredWindowStart.put(type, -1L);
            timeTriggered.put(type, false);
        }
        schedulerStartMs.set(System.currentTimeMillis());
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
        if (timeConfig != null && timeConfig.enabled) {
            return null;
        }

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
        if (timeConfig != null && timeConfig.enabled) {
            StringBuilder sb = new StringBuilder();
            long elapsedMs = System.currentTimeMillis() - schedulerStartMs.get();
            sb.append("Time-based fault scheduling is enabled\n");
            sb.append(String.format("Elapsed since scheduler start: %d s%n", TimeUnit.MILLISECONDS.toSeconds(elapsedMs)));
            sb.append(String.format("Start delay: %d min, Gap: %d min%n", timeConfig.startDelayMinutes, timeConfig.gapMinutes));
            for (int i = 0; i < timeConfig.order.length; i++) {
                FaultType ft = timeConfig.order[i];
                long dueMin = timeConfig.startDelayMinutes + ((long) i * timeConfig.gapMinutes);
                String state = Boolean.TRUE.equals(timeTriggered.get(ft)) ? "[TRIGGERED]" : "[PENDING]";
                sb.append(String.format("  %s at ~%d min %s%n", ft, dueMin, state));
            }
            return sb.toString();
        }

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
        if (timeConfig != null && timeConfig.enabled) {
            sb.append("Time Mode ENABLED (one-shot):\n");
            sb.append(String.format("  Start delay:                  %d minutes%n", timeConfig.startDelayMinutes));
            sb.append(String.format("  Gap between faults:           %d minutes%n", timeConfig.gapMinutes));
            sb.append("  Order:                        ");
            for (int i = 0; i < timeConfig.order.length; i++) {
                sb.append(timeConfig.order[i]);
                if (i < timeConfig.order.length - 1) {
                    sb.append(" -> ");
                }
            }
            sb.append("\n");
            return sb.toString();
        }

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

    private static FaultType[] parseFaultOrder(String rawOrder) {
        if (rawOrder == null || rawOrder.isBlank()) {
            return FaultType.values();
        }

        String[] parts = rawOrder.split(",");
        Set<FaultType> ordered = new LinkedHashSet<>();
        for (String part : parts) {
            String token = part.trim().toUpperCase();
            if (token.isEmpty()) {
                continue;
            }
            try {
                ordered.add(toFaultType(token));
            } catch (IllegalArgumentException e) {
                logger.warn("Ignoring unknown fault token in order: {}", token);
            }
        }

        if (ordered.isEmpty()) {
            return FaultType.values();
        }

        return ordered.toArray(FaultType[]::new);
    }

    private static FaultType toFaultType(String token) {
        return switch (token) {
            case "F1" -> FaultType.F1_CRASH_BEFORE_DB_COMMIT;
            case "F2" -> FaultType.F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK;
            case "F3" -> FaultType.F3_PARTIAL_BATCH_WRITES;
            case "F4" -> FaultType.F4_DB_CONTAINER_RESTART;
            case "F5" -> FaultType.F5_SLOW_SINK_BACKPRESSURE;
            case "F6" -> FaultType.F6_NETWORK_BOUNDARY_FAULT;
            default -> throw new IllegalArgumentException("Unknown fault token: " + token);
        };
    }

    private static int indexOf(FaultType[] order, FaultType target) {
        for (int i = 0; i < order.length; i++) {
            if (order[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
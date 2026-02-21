package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.EnumMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

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
 * etc.
 */
public class FaultScheduler {

    // Scheduler configuration for sequential injection
    private static class SequentialScheduleConfig {
        boolean enabled;              // Is sequential scheduling enabled
        long durationPerFaultMessages; // Messages to apply each fault
        long breakDurationMessages;    // Messages of break between faults (no faults active)
        int iterations;               // Times to cycle through all faults
        
        SequentialScheduleConfig(boolean enabled, long duration, long breakDuration, int iterations) {
            this.enabled = enabled;
            this.durationPerFaultMessages = duration;
            this.breakDurationMessages = breakDuration;
            this.iterations = iterations;
        }
    }

    private SequentialScheduleConfig sequentialConfig = null;
    private long globalMessageCounter = 0;
    private final Map<FaultType, Integer> iterationCounters = new EnumMap<>(FaultType.class);
    private FaultConfig faultConfig = null;
    private Random random = null;

    /**
     * Load scheduler configuration from properties
     * @return FaultScheduler instance
     * @throws Exception if properties file cannot be loaded
     */
    public static FaultScheduler load() throws Exception {
        return load(null);
    }

    /**
     * Load scheduler configuration from properties with optional FaultConfig
     * @param faultConfig Optional FaultConfig for probability-based injection within windows
     * @return FaultScheduler instance
     * @throws Exception if properties file cannot be loaded
     */
    public static FaultScheduler load(FaultConfig faultConfig) throws Exception {
        Properties props = new Properties();
        try (InputStream is = FaultScheduler.class.getResourceAsStream("/faults.properties")) {
            if (is == null) {
                System.out.println("[FaultScheduler] faults.properties not found, using defaults");
                return new FaultScheduler(faultConfig);
            }
            props.load(is);
        }

        FaultScheduler scheduler = new FaultScheduler(faultConfig);
        
        // Load sequential scheduling configuration
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
            String probInfo = faultConfig != null ? " (with probability-based injection)" : "";
            System.out.printf("[FaultScheduler] Sequential mode enabled: %d msgs per fault%s, %d full cycles (F1->F2->...->F6)%s%n",
                durationPerFault, breakInfo, iterations, probInfo);
        }
        
        return scheduler;
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

    /**
     * Default constructor - no scheduled faults
     */
    public FaultScheduler() {
        this(null);
    }

    /**
     * Constructor with optional FaultConfig for probability-based injection
     * @param faultConfig FaultConfig for probability-based injection within active windows
     */
    public FaultScheduler(FaultConfig faultConfig) {
        globalMessageCounter = 0;
        this.faultConfig = faultConfig;
        this.random = new Random();
        for (FaultType faultType : FaultType.values()) {
            iterationCounters.put(faultType, 0);
        }
    }

    /**
     * Check if a fault should be injected based on sequential schedule
     * Faults are injected in order: F1, F2, F3, F4, F5, F6, with breaks in between
     * Pattern: [F1: duration msgs] [BREAK: breakDuration msgs] [F2: duration msgs] [BREAK] ... repeat
     * 
     * If FaultConfig is provided, probability is used to determine actual injection within the window.
     * Otherwise, injection is guaranteed within the active window.
     * 
     * @param faultType The fault to check
     * @return true if this fault should be injected now
     */
    public boolean shouldInjectScheduled(FaultType faultType) {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return false;
        }

        // Map fault types to their order (0-5 for F1-F6)
        int faultIndex = faultType.ordinal(); // F1=0, F2=1, ..., F6=5
        
        // Calculate cycle length: each fault + break, for 6 faults
        long segmentLength = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
        long cycleLengthMessages = 6 * segmentLength;
        long currentCycle = globalMessageCounter / cycleLengthMessages;
        long positionInCycle = globalMessageCounter % cycleLengthMessages;
        
        // Check if we've exceeded max iterations
        if (sequentialConfig.iterations > 0 && currentCycle >= sequentialConfig.iterations) {
            return false;
        }
        
        // Calculate the window for this specific fault within the cycle
        // Each fault occupies: [faultIndex * segmentLength] to [faultIndex * segmentLength + durationPerFaultMessages]
        long faultWindowStart = faultIndex * segmentLength;
        long faultWindowEnd = faultWindowStart + sequentialConfig.durationPerFaultMessages;
        
        // Check if current position is within this fault's window
        boolean inWindow = positionInCycle >= faultWindowStart && positionInCycle < faultWindowEnd;
        
        if (!inWindow) {
            return false;
        }
        
        // We are in the fault window. Now check probability if FaultConfig is available
        boolean shouldInject = true;
        if (faultConfig != null) {
            double probability = faultConfig.getProbability(faultType);
            shouldInject = random.nextDouble() < probability;
        }
        
        // Log when entering a new fault window
        if (positionInCycle == faultWindowStart) {
            long breakWindowEnd = faultWindowStart + sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
            String probInfo = faultConfig != null ? String.format(" (%.0f%% probability)", faultConfig.getProbability(faultType) * 100) : "";
            System.out.printf("[FaultScheduler] >>> Starting %s (cycle %d, msgs %d-%d)%s, then break until msg %d%n",
                faultType, currentCycle + 1, 
                currentCycle * cycleLengthMessages + faultWindowStart,
                currentCycle * cycleLengthMessages + faultWindowEnd,
                probInfo,
                currentCycle * cycleLengthMessages + breakWindowEnd);
        }
        
        // Log when exiting a fault window and entering break (if break exists)
        if (positionInCycle == faultWindowEnd && sequentialConfig.breakDurationMessages > 0 && faultIndex < 5) {
            System.out.printf("[FaultScheduler] <<< Completed %s, entering break period%n", faultType);
        }
        
        // Log when exiting break and starting next fault
        if (positionInCycle == faultWindowStart && sequentialConfig.breakDurationMessages > 0 && faultIndex < 5) {
            int prevFaultIndex = (faultIndex + 5) % 6; // Get previous fault (handles F1 -> F6 wrap)
            System.out.printf("[FaultScheduler] <<< Break completed after %s, starting %s%n",
                FaultType.values()[prevFaultIndex], faultType);
        }
        
        return shouldInject;
    }

    /**
     * Increment global message counter
     * Call this after each message is processed
     */
    public void incrementMessageCounter() {
        globalMessageCounter++;
    }

    /**
     * Increment message counter by batch
     * @param count Number of messages to add
     */
    public void incrementMessageCounter(long count) {
        globalMessageCounter += count;
    }

    /**
     * Get current message count
     * @return Total messages processed
     */
    public long getMessageCount() {
        return globalMessageCounter;
    }

    /**
     * Reset scheduler (useful for testing)
     */
    public void reset() {
        globalMessageCounter = 0;
        for (FaultType faultType : FaultType.values()) {
            iterationCounters.put(faultType, 0);
        }
    }

    /**
     * Get schedule info for display
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("==== Fault Schedule Configuration ====\n");
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            sb.append("Sequential fault scheduling is DISABLED.\n");
        } else {
            long segmentLength = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
            long cycleLengthMessages = 6 * segmentLength;
            long totalMessagesForAllIterations = cycleLengthMessages * sequentialConfig.iterations;
            sb.append(String.format("Sequential Mode ENABLED:\n"));
            sb.append(String.format("  Duration per fault: %d messages\n", sequentialConfig.durationPerFaultMessages));
            sb.append(String.format("  Break between faults: %d messages\n", sequentialConfig.breakDurationMessages));
            sb.append(String.format("  Segment length (fault + break): %d messages\n", segmentLength));
            sb.append(String.format("  Faults per cycle: 6 (F1, F2, F3, F4, F5, F6)\n"));
            sb.append(String.format("  Messages per cycle: %d\n", cycleLengthMessages));
            sb.append(String.format("  Total iterations: %d\n", sequentialConfig.iterations));
            sb.append(String.format("  Total messages to process: %d\n", totalMessagesForAllIterations));
            sb.append("\nSequence:\n");
            for (int i = 0; i < FaultType.values().length; i++) {
                long faultStart = i * segmentLength;
                long faultEnd = faultStart + sequentialConfig.durationPerFaultMessages;
                long breakEnd = faultEnd + sequentialConfig.breakDurationMessages;
                sb.append(String.format("  Position %d-%d: %s", faultStart, faultEnd, FaultType.values()[i]));
                if (sequentialConfig.breakDurationMessages > 0) {
                    sb.append(String.format(", %d-%d: BREAK", faultEnd, breakEnd));
                }
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Get detailed status for monitoring
     */
    public String getDetailedStatus() {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return "Sequential fault scheduling is disabled\n";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Current message count: %d\n", globalMessageCounter));
        
        long segmentLength = sequentialConfig.durationPerFaultMessages + sequentialConfig.breakDurationMessages;
        long cycleLengthMessages = 6 * segmentLength;
        long currentCycle = globalMessageCounter / cycleLengthMessages;
        long positionInCycle = globalMessageCounter % cycleLengthMessages;
        
        sb.append(String.format("Cycle %d/%d, Position %d/%d messages\n",
            currentCycle + 1, sequentialConfig.iterations, positionInCycle, cycleLengthMessages));
        
        // Show which fault is currently active or if in break period
        FaultType[] faults = FaultType.values();
        for (int i = 0; i < faults.length; i++) {
            long faultStart = i * segmentLength;
            long faultEnd = faultStart + sequentialConfig.durationPerFaultMessages;
            long breakEnd = faultEnd + sequentialConfig.breakDurationMessages;
            
            String status;
            if (positionInCycle >= faultStart && positionInCycle < faultEnd) {
                status = "[FAULT ACTIVE]";
            } else if (sequentialConfig.breakDurationMessages > 0 && positionInCycle >= faultEnd && positionInCycle < breakEnd) {
                status = "[BREAK PERIOD]";
            } else {
                status = "[idle]";
            }
            
            if (sequentialConfig.breakDurationMessages > 0) {
                sb.append(String.format("  %s: fault msgs %d-%d, break %d-%d %s\n", 
                    faults[i], faultStart, faultEnd, faultEnd, breakEnd, status));
            } else {
                sb.append(String.format("  %s: msgs %d-%d %s\n", faults[i], faultStart, faultEnd, status));
            }
        }
        
        return sb.toString();
    }

    /**
     * Get all active faults for current position
     * In sequential mode, only one fault should be active at a time
     * @return FaultType of currently active fault, or null if none
     */
    public FaultType getCurrentActiveFault() {
        if (sequentialConfig == null || !sequentialConfig.enabled) {
            return null;
        }
        
        for (FaultType faultType : FaultType.values()) {
            if (shouldInjectScheduled(faultType)) {
                return faultType;
            }
        }
        return null;
    }
}

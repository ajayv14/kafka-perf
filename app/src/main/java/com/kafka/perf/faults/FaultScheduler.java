package com.kafka.perf.faults;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Schedules one configured fault to run once after a fixed delay.
 */
public class FaultScheduler {

    private static final Logger logger = LoggerFactory.getLogger(FaultScheduler.class);

    private final FaultType enabledFault;
    private final long injectAfterMinutes;
    private final AtomicBoolean triggered = new AtomicBoolean(false);
    private final AtomicLong schedulerStartMs = new AtomicLong(System.currentTimeMillis());

    public static FaultScheduler load() throws Exception {
        return load(null);
    }

    public static FaultScheduler load(FaultConfig faultConfig) throws Exception {
        Properties props = new Properties();
        try (InputStream is = FaultScheduler.class.getResourceAsStream("/faults.properties")) {
            if (is != null) {
                props.load(is);
            } else {
                logger.debug("faults.properties not found, using defaults");
            }
        }

        FaultType enabledFault = getEnabledFault(props);
        long injectAfterMinutes = Long.parseLong(getOrEnv(
            "fault.inject.after.minutes",
            "FAULT_INJECT_AFTER_MINUTES",
            props,
            "0"));

        if (enabledFault == null) {
            logger.info("Fault injection disabled");
        } else {
            logger.info("Fault {} will be injected once after {} minute(s)", enabledFault, injectAfterMinutes);
        }

        return new FaultScheduler(enabledFault, injectAfterMinutes);
    }

    public FaultScheduler() {
        this(null, 0);
    }

    public FaultScheduler(FaultConfig faultConfig) {
        this(null, 0);
    }

    private FaultScheduler(FaultType enabledFault, long injectAfterMinutes) {
        this.enabledFault = enabledFault;
        this.injectAfterMinutes = injectAfterMinutes;
    }

    public boolean shouldInjectScheduled(FaultType faultType) {
        if (enabledFault == null || enabledFault != faultType) {
            return false;
        }

        if (triggered.get()) {
            return false;
        }

        long dueMs = schedulerStartMs.get() + TimeUnit.MINUTES.toMillis(injectAfterMinutes);
        long now = System.currentTimeMillis();
        if (now < dueMs) {
            return false;
        }

        if (!triggered.compareAndSet(false, true)) {
            return false;
        }

        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(now - schedulerStartMs.get());
        logger.info(">>> Triggering {} once at elapsed={}s", faultType, elapsedSeconds);
        return true;
    }

    public void incrementMessageCounter() {
        // No-op. Message-window scheduling has been removed.
    }

    public void incrementMessageCounter(long count) {
        // No-op. Message-window scheduling has been removed.
    }

    public long getMessageCount() {
        return 0;
    }

    public void reset() {
        triggered.set(false);
        schedulerStartMs.set(System.currentTimeMillis());
    }

    public FaultType getCurrentActiveFault() {
        if (enabledFault == null || triggered.get()) {
            return null;
        }
        return enabledFault;
    }

    public String getDetailedStatus() {
        if (enabledFault == null) {
            return "Fault injection disabled\n";
        }

        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - schedulerStartMs.get());
        long dueSeconds = TimeUnit.MINUTES.toSeconds(injectAfterMinutes);
        String state = triggered.get() ? "[TRIGGERED]" : "[PENDING]";

        return String.format(
            "Fault injection enabled%nElapsed: %d s%nInject after: %d s%nFault: %s %s%n",
            elapsedSeconds,
            dueSeconds,
            enabledFault,
            state);
    }

    @Override
    public String toString() {
        if (enabledFault == null) {
            return "==== Fault Schedule Configuration ====\nFault injection disabled\n";
        }

        return String.format(
            "==== Fault Schedule Configuration ====%nEnabled fault:                %s%nInject after:                %d minute(s)%n",
            enabledFault,
            injectAfterMinutes);
    }

    private static FaultType getEnabledFault(Properties props) {
        FaultType enabledFault = null;
        for (FaultType type : FaultType.values()) {
            if (Boolean.parseBoolean(getOrEnv(shortName(type), shortName(type), props, "false"))) {
                if (enabledFault != null) {
                    throw new IllegalStateException("Only one of F1 to F6 can be set to true");
                }
                enabledFault = type;
            }
        }
        return enabledFault;
    }

    private static String shortName(FaultType type) {
        return switch (type) {
            case F1_CRASH_BEFORE_DB_COMMIT -> "F1";
            case F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK -> "F2";
            case F3_PARTIAL_BATCH_WRITES -> "F3";
            case F4_DB_CONTAINER_RESTART -> "F4";
            case F5_SLOW_SINK_BACKPRESSURE -> "F5";
            case F6_NETWORK_BOUNDARY_FAULT -> "F6";
        };
    }

    private static String getOrEnv(String propKey, String envKey, Properties props, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String propValue = props.getProperty(propKey);
        return propValue != null ? propValue : defaultValue;
    }
}

package com.kafka.perf.baseline;

final class PostgresSinkStats {

    static final class StatsSnapshot {
        final long totalConsumed;
        final long totalWritten;
        final long totalWriteErrors;
        final long intervalConsumed;
        final long intervalWritten;
        final double intervalConsumedRate;
        final double intervalWriteRate;
        final double lifetimeConsumedRate;
        final double lifetimeWriteRate;

        StatsSnapshot(
                long totalConsumed,
                long totalWritten,
                long totalWriteErrors,
                long intervalConsumed,
                long intervalWritten,
                double intervalConsumedRate,
                double intervalWriteRate,
                double lifetimeConsumedRate,
                double lifetimeWriteRate) {
            this.totalConsumed = totalConsumed;
            this.totalWritten = totalWritten;
            this.totalWriteErrors = totalWriteErrors;
            this.intervalConsumed = intervalConsumed;
            this.intervalWritten = intervalWritten;
            this.intervalConsumedRate = intervalConsumedRate;
            this.intervalWriteRate = intervalWriteRate;
            this.lifetimeConsumedRate = lifetimeConsumedRate;
            this.lifetimeWriteRate = lifetimeWriteRate;
        }
    }

    private long totalMessagesConsumed;
    private long totalMessagesWritten;
    private long totalWriteErrors;
    private long intervalMessagesConsumed;
    private long intervalMessagesWritten;
    private long consumerStartTime;
    private long lastLogTime;

    PostgresSinkStats() {
        reset();
    }

    void recordConsumed() {
        totalMessagesConsumed++;
        intervalMessagesConsumed++;
    }

    void recordWriteSuccess() {
        totalMessagesWritten++;
        intervalMessagesWritten++;
    }

    void recordWriteFailure() {
        totalWriteErrors++;
    }

    StatsSnapshot snapshot(long currentTime) {
        double intervalElapsedSecs = Math.max((currentTime - lastLogTime) / 1000.0, 0.001);
        double lifetimeElapsedSecs = Math.max((currentTime - consumerStartTime) / 1000.0, 0.001);

        return new StatsSnapshot(
                totalMessagesConsumed,
                totalMessagesWritten,
                totalWriteErrors,
                intervalMessagesConsumed,
                intervalMessagesWritten,
                intervalMessagesConsumed / intervalElapsedSecs,
                intervalMessagesWritten / intervalElapsedSecs,
                totalMessagesConsumed / lifetimeElapsedSecs,
                totalMessagesWritten / lifetimeElapsedSecs
        );
    }

    void markLogTime(long currentTime) {
        lastLogTime = currentTime;
    }

    long getLastLogTime() {
        return lastLogTime;
    }

    void resetInterval() {
        intervalMessagesConsumed = 0;
        intervalMessagesWritten = 0;
    }

    void reset() {
        totalMessagesConsumed = 0;
        totalMessagesWritten = 0;
        totalWriteErrors = 0;
        intervalMessagesConsumed = 0;
        intervalMessagesWritten = 0;
        consumerStartTime = System.currentTimeMillis();
        lastLogTime = consumerStartTime;
    }

    void setStateForTest(
            long totalConsumed,
            long totalWritten,
            long totalErrors,
            long intervalConsumed,
            long intervalWritten,
            long startTime,
            long previousLogTime) {
        totalMessagesConsumed = totalConsumed;
        totalMessagesWritten = totalWritten;
        totalWriteErrors = totalErrors;
        intervalMessagesConsumed = intervalConsumed;
        intervalMessagesWritten = intervalWritten;
        consumerStartTime = startTime;
        lastLogTime = previousLogTime;
    }
}

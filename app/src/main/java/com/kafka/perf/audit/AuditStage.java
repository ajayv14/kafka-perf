package com.kafka.perf.audit;


public enum AuditStage {
    // A non-empty batch was successfully returned by poll().
    BATCH_READ,
    // Kafka offsets were successfully committed via commitSync().
    OFFSET_COMMITTED
}
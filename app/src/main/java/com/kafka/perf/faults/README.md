# Fault Injection

This package implements deterministic fault injection for the Kafka → PostgreSQL sink consumers.

## What this package does

- Loads fault config from `faults.properties`.
- Selects one fault and injects it once after a delay via `FaultScheduler`.
- Executes the fault behavior via `FaultInjector`.
- Applies faults inside transactional consumer flow (`FaultInjectorConsumer`, `FaultInjectorWithAuditConsumer`).
- Can launch multiple consumer JVMs from one entrypoint (`ConsumerProcessScheduler`).

## Fault types

- `F1_CRASH_BEFORE_DB_COMMIT` — crash before database commit.
- `F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK` — crash after DB commit, before Kafka offset commit.
- `F3_PARTIAL_BATCH_WRITES` — write only part of a batch.
- `F4_DB_CONTAINER_RESTART` — restart PostgreSQL container.

## Configuration

Enable exactly one fault in `faults.properties` and set the delay:

- `F1=false`
- `F2=false`
- `F3=false`
- `F4=false`
- `fault.inject.after.minutes=1`

Example:

- `F3=true`
- `fault.inject.after.minutes=2`

This injects `F3_PARTIAL_BATCH_WRITES` once, two minutes after the consumer starts.

## Injection points in consumer flow

For each poll batch:

1. Poll records.
2. Optional runtime fault before DB write loop:
   - `F4`
3. Transactional DB write path:
   - `F1` before DB commit.
   - `F3` may write subset only (offsets intentionally not committed).
   - DB `commit()`.
   - `F2` after DB commit, before Kafka `commitSync()`.
4. If full batch succeeded and auto-commit is off, commit Kafka offsets.
5. The selected fault runs once after the configured delay.

## Practical note

Only one fault is active at a time.
The consumer executes scheduled events via `faultInjector.injectDeterministic(...)`.

## Single Java scheduler for multiple consumers

Use `ConsumerProcessScheduler` to launch multiple consumer processes from one Java main class.

Example:

- `java -cp target/classes:target/dependency/* com.kafka.perf.faults.ConsumerProcessScheduler 3 com.kafka.perf.faults.FaultInjectorConsumer`

This starts 3 consumer JVM processes in the same group configuration.

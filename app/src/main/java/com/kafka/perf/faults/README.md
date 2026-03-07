# Fault Injection (Sequential with Gap)

This package implements deterministic fault injection for the Kafka → PostgreSQL sink consumers.

## What this package does

- Loads fault config from `faults.properties`.
- Computes *when* a fault is eligible via `FaultScheduler`.
- Executes the fault behavior via `FaultInjector`.
- Applies faults inside transactional consumer flow (`FaultInjectorConsumer`, `FaultInjectorWithAuditConsumer`).
- Can launch multiple consumer JVMs from one entrypoint (`ConsumerProcessScheduler`).

## Fault types

- `F1_CRASH_BEFORE_DB_COMMIT` — crash before database commit.
- `F2_CRASH_AFTER_DB_COMMIT_BEFORE_ACK` — crash after DB commit, before Kafka offset commit.
- `F3_PARTIAL_BATCH_WRITES` — write only part of a batch.
- `F4_DB_CONTAINER_RESTART` — restart PostgreSQL container.
- `F5_SLOW_SINK_BACKPRESSURE` — add sink-side delay.
- `F6_NETWORK_BOUNDARY_FAULT` — add network-style latency.

## Scheduling model

### Time-based one-shot mode (minutes, preferred)

Each fault is injected once at a scheduled minute offset from consumer start.

Properties:

- `fault.schedule.time.enabled=true`
- `fault.schedule.time.start.delay.minutes=0`
- `fault.schedule.time.gap.minutes=1`
- `fault.schedule.time.order=F1,F2,F3,F4,F5,F6`

Example timeline:

- `+0 min` -> `F1` once
- `+1 min` -> `F2` once
- `+2 min` -> `F3` once
- ...

### Sequential message-window mode (legacy)

When `fault.schedule.sequential.enabled=true`, scheduler activates faults in this order:

`F1 -> gap -> F2 -> gap -> F3 -> gap -> F4 -> gap -> F5 -> gap -> F6 -> gap -> repeat`

Key properties:

- `fault.schedule.duration.messages`: messages per fault window.
- `fault.schedule.break.messages`: messages in no-fault gap between windows.
- `fault.schedule.iterations`: number of full F1..F6 cycles.

The scheduler advances by consumed record count using:

- `faultScheduler.incrementMessageCounter(records.count())`

Without this increment, windows do not progress.

## Injection points in consumer flow

For each poll batch:

1. Poll records.
2. Optional runtime faults before DB write loop:
   - `F5`, `F6`
3. Transactional DB write path:
   - `F1` before DB commit.
   - `F3` may write subset only (offsets intentionally not committed).
   - DB `commit()`.
   - `F2` after DB commit, before Kafka `commitSync()`.
4. If full batch succeeded and auto-commit is off, commit Kafka offsets.
5. Advance scheduler message counter by `records.count()`.

## Example

Configuration:

- `fault.schedule.duration.messages=1000`
- `fault.schedule.break.messages=500`
- sequential mode enabled

Resulting windows:

- Messages `1..1000`: `F1`
- Messages `1001..1500`: gap (no faults)
- Messages `1501..2500`: `F2`
- Messages `2501..3000`: gap
- Messages `3001..4000`: `F3`
- ... continues through `F6`, then repeats by iteration count.

## Practical note

Time mode triggers each fault once at minute-based intervals.
Sequential mode triggers once per message window.
The consumer executes scheduled events via `faultInjector.injectDeterministic(...)`.

## Single Java scheduler for multiple consumers

Use `ConsumerProcessScheduler` to launch multiple consumer processes from one Java main class.

Example:

- `java -cp target/classes:target/dependency/* com.kafka.perf.faults.ConsumerProcessScheduler 3 com.kafka.perf.faults.FaultInjectorConsumer`

This starts 3 consumer JVM processes in the same group configuration.

# Metrics Reference

This document is the source of truth for the metrics currently collected by the
Kafka performance monitoring stack. It is written for experiment reporting and
paper interpretation, so it separates raw signals, recording rules, dashboard
queries, and interpretation caveats.

## Scope

The monitoring stack and experiment runner collect five categories of measurements:

1. Kafka broker throughput and health from the JMX exporter.
2. Kafka container CPU and memory from cAdvisor.
3. Host-level CPU and memory from Node Exporter.
4. Audit lifecycle outcomes from the audit outcomes exporter.
5. Producer, sink consumer, and audit aggregator process CPU/RSS from the
   repeated experiment runner.

This document focuses especially on CPU and memory because those metrics are
easy to mislabel in papers.

## Collection Stack

### Kafka JMX Exporter

File:

- `kafka-cluster/jmx-exporter/kafka-config.yml`

Runtime:

- A Prometheus JMX Java agent is attached to each Kafka broker.
- Each broker exposes metrics on internal port `7071`.
- Host mappings are:
  - `kafka-1`: host port `7071`
  - `kafka-2`: host port `7072`
  - `kafka-3`: host port `7073`

Prometheus scrape jobs:

- `kafka-jmx-1`
- `kafka-jmx-2`
- `kafka-jmx-3`

Important JMX metrics:

| Metric | Source | Meaning |
| --- | --- | --- |
| `kafka_broker_bytes_in_total` | Kafka JMX | Cumulative bytes received by a broker. |
| `kafka_broker_bytes_out_total` | Kafka JMX | Cumulative bytes sent by a broker. |
| `kafka_broker_messages_in_total` | Kafka JMX | Cumulative messages received by a broker. |
| `jvm_heap_used_bytes` | JVM JMX | Current JVM heap used by the Kafka process. |
| `jvm_heap_max_bytes` | JVM JMX | Maximum JVM heap configured for the Kafka process. |
| `jvm_non_heap_used_bytes` | JVM JMX | Current JVM non-heap memory, such as metaspace and code cache. |
| `jvm_process_cpu_load_ratio` | JVM JMX | Recent CPU load for the Kafka JVM process, reported as a ratio. |
| `jvm_system_cpu_load_ratio` | JVM JMX | Recent CPU load for the system visible to the JVM, reported as a ratio. |
| `kafka_jvm_threads_current` | JVM JMX | Current JVM thread count. |
| `kafka_request_produce_latency_p99_ms` | Kafka JMX | Kafka produce request p99 latency in milliseconds. |

### cAdvisor

File:

- `monitoring-dash/prometheus/prometheus.yml`

Scrape job:

- `cadvisor`

Kept metrics:

- `container_cpu_usage_seconds_total`
- `container_memory_usage_bytes`
- `container_spec_memory_limit_bytes`

These are container-level measurements. They are not Kafka-specific until the
recording rules filter them down to `kafka-1`, `kafka-2`, and `kafka-3`.

### Node Exporter

File:

- `monitoring-dash/prometheus/prometheus.yml`

Scrape job:

- `node-exporter`

Kept metrics:

- `node_cpu_*`
- `node_memory_*`
- `node_load*`
- `node_context_switches*`
- `node_intr*`

These are host-level measurements. They describe the Docker host, not only the
Kafka brokers.

### Kafka Exporter

Scrape job:

- `kafka-exporter`

Important metric:

- `kafka_consumergroup_lag`

Recording rule:

- `kafka_cluster_consumer_lag_messages`

### Audit Outcomes Exporter

Scrape job:

- `audit-outcomes-exporter`

Important metrics:

| Metric | Meaning |
| --- | --- |
| `audit_outcomes_total` | Count of audit lifecycle outcomes by outcome type, consumer group, and source topic. |
| `audit_batches_seen_total` | Count of audit outcome messages used for rate normalization. |
| `audit_replay_count_total` | Accumulated replay counts from audit outcomes. |
| `audit_timeout_count_total` | Accumulated timeout counts from audit outcomes. |
| `audit_partition_outcomes_total` | Partition-level count of audit outcomes. |

### Experiment Runner Process Sampler

File:

- `experiments/run_repeated_experiments.py`

Output per run:

- `metrics/process_metrics.csv`
- `metrics/process_metrics_summary.csv`
- `metrics/process_metrics_summary.json`

Campaign-level summaries:

- `campaign_prometheus_summary.csv`
- `campaign_process_summary.csv`

The process sampler records CPU percentage and resident set size for each
managed local Java process launched by the runner:

- `producer`
- `consumer-1`, `consumer-2`, `consumer-3`
- `audit-aggregator` during audit runs

This fills the attribution gap left by Prometheus. Prometheus/cAdvisor captures
Docker containers such as Kafka and PostgreSQL, while the benchmark producer and
consumers are host-launched Java processes. Host-level Node Exporter metrics
include these Java processes, but do not identify them individually.

Interpretation:

- `cpu_percent` comes from `ps pcpu` for the managed process.
- `rss_mb` is resident set size from `ps rss`, converted to MB.
- `all-managed-processes` in the summary is the sum across producer, consumers,
  and audit aggregator for each sample timestamp.
- These values are sampled at the runner interval, which defaults to `5s`.

## Prometheus Timing

File:

- `monitoring-dash/prometheus/prometheus.yml`

Current settings:

- Scrape interval: `5s`
- Scrape timeout: `4s`
- Rule evaluation interval: `5s`

Recording rules use a `5m` range window for most rate calculations. This means
the displayed rate is smoothed over five minutes, even though Prometheus scrapes
every five seconds.

For paper reporting, state both:

- collection cadence: `5s`
- rate window: `5m`
- process sampler cadence: `5s`, unless `run_repeated_experiments.py` is
  invoked with a different `--process-sample-secs` value

## Throughput Metrics

File:

- `monitoring-dash/prometheus/recording-rules.yml`

### Broker Throughput

| Recording rule | Formula | Interpretation |
| --- | --- | --- |
| `kafka_broker_bytes_in_kb_per_sec` | `sum by(instance) (rate(kafka_broker_bytes_in_total[5m])) / 1024` | Per-broker ingress throughput in KB/s. |
| `kafka_broker_bytes_out_kb_per_sec` | `sum by(instance) (rate(kafka_broker_bytes_out_total[5m])) / 1024` | Per-broker egress throughput in KB/s. |
| `kafka_broker_throughput_kb_per_sec` | ingress plus egress per broker | Per-broker combined network throughput in KB/s. |

### Cluster Throughput

| Recording rule | Formula | Interpretation |
| --- | --- | --- |
| `kafka_cluster_bytes_in_kb_per_sec` | `sum(rate(kafka_broker_bytes_in_total[5m])) / 1024` | Cluster ingress throughput in KB/s. |
| `kafka_cluster_bytes_out_kb_per_sec` | `sum(rate(kafka_broker_bytes_out_total[5m])) / 1024` | Cluster egress throughput in KB/s. |
| `kafka_cluster_throughput_bytes_per_sec` | `sum(rate(bytes_in[5m]) + rate(bytes_out[5m]))` | Cluster ingress plus egress in bytes/s. |
| `kafka_cluster_messages_per_sec` | `sum(rate(kafka_broker_messages_in_total[5m]))` | Cluster message ingress rate. |

## CPU Metrics

CPU is collected from cAdvisor, JMX, Node Exporter, and the runner process
sampler. They answer different questions and should not be mixed without
explanation.

### Container CPU From cAdvisor

Raw metric:

- `container_cpu_usage_seconds_total`

Recording rule:

- `kafka_broker_cpu_percent`

Current formula:

```promql
rate(container_cpu_usage_seconds_total[5m]) * 100
```

The recording rule also attaches a `broker` label using a selector chain:

1. hard-coded Docker container ID selectors
2. Docker Compose service label selectors
3. container name selectors

Interpretation:

- This is best understood as `CPU cores consumed * 100`.
- A value of `100` means approximately one full CPU core.
- A value of `200` means approximately two full CPU cores.
- It is not normalized to total host CPU capacity.
- It is not normalized to Docker CPU limits.

Cluster rule:

```promql
kafka_cluster_cpu_percent = sum(kafka_broker_cpu_percent)
```

Interpretation:

- This is summed Kafka broker container CPU consumption.
- It can legitimately exceed `100`.
- The name includes `percent`, but the unit is closer to `core-percent`.
- For paper wording, describe it as "Kafka broker container CPU core usage"
  rather than "CPU percentage".

Example:

- `kafka_cluster_cpu_percent = 250`
- Interpretation: the Kafka broker containers together consumed roughly 2.5 CPU
  cores over the five-minute rate window.

Derived core-count rule:

```promql
kafka_cluster_cpu_cores = kafka_cluster_cpu_percent / 100
```

Interpretation:

- This is the same container CPU signal expressed in CPU cores.
- Example: `kafka_cluster_cpu_cores = 2.5` means the Kafka broker containers
  consumed about 2.5 CPU cores.

### Host-Normalized Kafka CPU

The dashboard-facing CPU metric is normalized to the number of CPU cores visible
to Node Exporter:

```promql
kafka_host_cpu_cores = count(count by(cpu) (node_cpu_seconds_total{mode="idle"}))
kafka_cluster_cpu_host_percent = kafka_cluster_cpu_percent / kafka_host_cpu_cores
```

Interpretation:

- This converts Kafka broker container CPU usage into a familiar 0-100% host
  CPU scale.
- Example with 8 host cores: `kafka_cluster_cpu_percent = 250` becomes
  `kafka_cluster_cpu_host_percent = 31.25`.
- This is the recommended CPU metric for paper figures because reviewers can
  read it as "Kafka broker CPU as a percentage of available host cores."

Caveats:

- The denominator is host CPU cores visible to Node Exporter.
- It is not normalized to Docker CPU quota.
- If Prometheus scrapes multiple Node Exporter targets, the denominator should
  be reviewed so it matches the host running the Kafka brokers.

### JVM CPU From JMX

Raw metric:

- `jvm_process_cpu_load_ratio`

Recording rule:

```promql
kafka_cluster_jvm_cpu_percent = avg(jvm_process_cpu_load_ratio) * 100
```

Interpretation:

- This is the average recent Kafka JVM process CPU load ratio across scraped JMX
  targets.
- It is useful as a process-level JVM view.
- It is not the metric currently used by the main Grafana CPU panel.

Caveat:

- The exact denominator for `jvm_process_cpu_load_ratio` depends on JVM and OS
  visibility. In Docker, this may reflect the CPUs visible to the JVM rather
  than an explicitly enforced Compose CPU limit.

### CPU Limits In Docker Compose

The Kafka compose file contains:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 4G
```

Important interpretation:

- These settings are under `deploy.resources`.
- In many non-Swarm `docker compose` runs, `deploy.resources` is not the runtime
  CPU quota used by the container.
- Therefore the current CPU metrics should not be described as "percentage of
  allocated Docker CPU limit" unless runtime quota enforcement is separately
  verified.

For paper reporting, the preferred dashboard metric is:

- `kafka_cluster_cpu_host_percent`

This is the reviewer-friendly 0-100% view of Kafka broker CPU relative to host
cores.

Avoid saying this about `kafka_cluster_cpu_percent`:

- "Kafka used 80% CPU"

Prefer one of these:

- "Kafka broker containers used 31.25% of host CPU cores"
- "Kafka broker containers consumed approximately 0.8 CPU cores"
- or "Kafka broker containers consumed approximately 80 core-percent"

### Recommended CPU Metric For Paper Comparisons

For the three-run overhead comparison, report both infrastructure CPU and
managed workload process CPU:

1. Host-normalized Kafka CPU:

```promql
kafka_cluster_cpu_host_percent
```

This is the preferred paper/dashboard metric because it stays on a familiar
0-100% scale.

2. Container CPU core consumption:

```promql
kafka_cluster_cpu_cores
```

This gives approximate CPU cores consumed by all Kafka broker containers.

3. JVM process CPU load:

```promql
kafka_cluster_jvm_cpu_percent
```

This gives average Kafka JVM process CPU load as a percent-like ratio.

4. Managed Java workload CPU:

```text
metrics/process_metrics_summary.csv
process=all-managed-processes, metric=cpu_percent
```

This captures the local producer, sink consumers, and audit aggregator launched
by the runner. It is the best current signal for application-side CPU overhead.

If comparing across machines or runs, subtract an idle baseline:

```text
marginal_cpu = scenario_cpu - idle_baseline_cpu
```

This is stronger for the paper because it reports the marginal cost of each
fault scenario rather than absolute CPU consumption, which varies with host
hardware and background load.

## Memory Metrics

Memory is collected from cAdvisor, JMX, Node Exporter, and the runner process
sampler.

### Container Memory From cAdvisor

Raw metric:

- `container_memory_usage_bytes`

Recording rules:

| Recording rule | Formula | Interpretation |
| --- | --- | --- |
| `kafka_broker_container_memory_mb` | selected Kafka container memory / `1048576` | Per-broker container memory usage in MB. |
| `kafka_cluster_container_memory_gb` | sum selected Kafka container memory / `1073741824` | Combined Kafka broker container memory usage in GB. |

Interpretation:

- This is container memory usage, not only JVM heap.
- It includes the Kafka JVM, JVM non-heap memory, thread stacks, direct buffers,
  page/cache effects visible to cAdvisor, and other container memory accounting.
- It is the right metric for "how much memory the Kafka containers consumed."
- It is not the right metric for "how full the Kafka JVM heap is."

### JVM Heap And Non-Heap Memory From JMX

Raw metrics:

- `jvm_heap_used_bytes`
- `jvm_heap_max_bytes`
- `jvm_non_heap_used_bytes`

Current recording rules:

- There are no current heap-percent recording rules in
  `monitoring-dash/prometheus/recording-rules.yml`.

Interpretation:

- `jvm_heap_used_bytes` is the Kafka process heap currently used.
- `jvm_heap_max_bytes` is the configured heap maximum.
- `jvm_non_heap_used_bytes` covers memory such as metaspace, code cache, and
  other non-heap JVM areas.

The Kafka compose file sets:

```text
KAFKA_HEAP_OPTS=-Xmx2G -Xms2G
```

per broker. That means each Kafka broker JVM is configured with a 2 GB heap.

### Container Limit Memory

Raw metric:

- `container_spec_memory_limit_bytes`

The Prometheus scrape config keeps this metric, but current recording rules and
the main Grafana dashboard do not use it.

The compose file declares a 4 GB memory limit per Kafka broker under
`deploy.resources`. As with CPU, verify whether this limit is actually enforced
by your Docker runtime before using it as a denominator in the paper.

### Recommended Memory Metrics For Paper Comparisons

Use container, JVM, and managed-process memory, but label them separately:

1. Container memory:

```promql
kafka_cluster_container_memory_gb
```

Use this for total broker container footprint.

2. JVM heap used:

```promql
sum(jvm_heap_used_bytes) / 1073741824
```

Use this for Kafka JVM heap behavior.

3. JVM non-heap used:

```promql
sum(jvm_non_heap_used_bytes) / 1073741824
```

Use this to separate heap from JVM metadata and native/non-heap pressure.

4. Managed Java workload RSS:

```text
metrics/process_metrics_summary.csv
process=all-managed-processes, metric=rss_mb
```

Use this for local producer/consumer/audit application memory. It is separate
from Kafka broker container memory and PostgreSQL container memory.

For fault-scenario comparisons, subtract idle baseline:

```text
marginal_memory = scenario_memory - idle_baseline_memory
```

## Grafana Dashboard Interpretation

File:

- `monitoring-dash/grafana/dashboards/kafka-eos-baseline-phase1.json`

Current CPU panel:

```promql
avg(kafka_cluster_cpu_host_percent{ })
```

Panel title:

- `Kafka Cluster CPU (% of Host Cores)`

Review:

- The query uses Kafka broker container CPU from cAdvisor, normalized by host
  CPU cores from Node Exporter.
- This is the preferred 0-100% CPU view for Grafana and paper figures.
- The raw core-consumption metric remains available as `kafka_cluster_cpu_cores`.

Current memory panel:

```promql
avg(kafka_cluster_container_memory_gb{ })
```

Panel title:

- `Kafka Cluster Container Memory (GB)`

Review:

- This title is accurate.
- It represents combined Kafka broker container memory usage in GB.
- It is not JVM heap-only memory.

## Paper-Ready Language

Recommended wording:

> CPU usage was measured from cAdvisor container CPU counters and converted to
> CPU-core consumption over a five-minute Prometheus rate window. For figures,
> Kafka broker CPU was normalized by the number of host CPU cores visible to
> Node Exporter and reported as a percentage of available host cores. Memory is
> reported as combined Kafka broker container memory usage from cAdvisor, with
> JVM heap and non-heap memory available separately from JMX. Producer,
> consumer, and audit-aggregator process CPU/RSS were sampled by the experiment
> runner during the measured window.

If you use idle-baseline subtraction:

> For fault-scenario comparisons, idle baseline CPU and memory were subtracted
> from scenario measurements to estimate the marginal resource cost of each
> fault condition.

Avoid this wording:

> CPU usage is reported as percent of total hardware CPU.

Avoid unless separately verified:

> CPU usage is reported as percent of Docker CPU limit.

## Current Documentation Review

Removed:

- `docs/CPU_MEMORY_METRICS_SUMMARY.md`

Reason:

- It described `kafka_cluster_cpu_percent = 12.5` as "12.5% combined CPU",
  which is not precise enough for paper reporting. The metric is better
  interpreted as container CPU core consumption multiplied by 100.

Replaced:

- `monitoring-dash/METRICS_GUIDE.md`

Reason:

- The old guide referenced metric names that are not present in the current
  recording rules, such as `kafka_broker_cpu_usage_percent` and
  `kafka_broker_heap_memory_usage_percent`.

## Quick Validation Queries

Use these in Prometheus before exporting experiment data.

Container CPU per broker:

```promql
kafka_broker_cpu_percent
```

Approximate Kafka broker CPU cores consumed by cluster:

```promql
kafka_cluster_cpu_cores
```

Kafka broker CPU normalized to host cores:

```promql
kafka_cluster_cpu_host_percent
```

Host CPU core count used as denominator:

```promql
kafka_host_cpu_cores
```

JVM process CPU ratio as percent-like value:

```promql
kafka_cluster_jvm_cpu_percent
```

Container memory per broker:

```promql
kafka_broker_container_memory_mb
```

Cluster container memory:

```promql
kafka_cluster_container_memory_gb
```

JVM heap used by brokers:

```promql
sum(jvm_heap_used_bytes) / 1073741824
```

JVM non-heap used by brokers:

```promql
sum(jvm_non_heap_used_bytes) / 1073741824
```

Host CPU, if needed for context:

```promql
100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle"}[5m])))
```

Host memory, if needed for context:

```promql
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
```

## Final Interpretation Summary

| Metric | Best interpretation | Not safe to claim |
| --- | --- | --- |
| `kafka_cluster_cpu_percent` | Kafka broker container CPU core consumption multiplied by 100. | Percent of hardware CPU or percent of Docker CPU limit. |
| `kafka_cluster_cpu_cores` | Approximate CPU cores consumed by Kafka broker containers. | Normalized fraction of total host capacity. |
| `kafka_cluster_cpu_host_percent` | Kafka broker container CPU normalized to host CPU cores, on a 0-100% scale. | Percent of Docker CPU limit. |
| `kafka_cluster_jvm_cpu_percent` | Average Kafka JVM process CPU load ratio expressed as percent-like value. | Container CPU quota utilization unless verified. |
| `kafka_cluster_container_memory_gb` | Combined Kafka broker container memory footprint. | JVM heap-only memory. |
| `sum(jvm_heap_used_bytes)` | Kafka JVM heap used. | Total container memory. |
| `sum(jvm_non_heap_used_bytes)` | Kafka JVM non-heap memory used. | Host memory pressure. |

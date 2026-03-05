# Kafka Monitoring Metrics Guide

## Overview
This document describes the metrics collected and exported in the Kafka performance monitoring dashboard.

## Metric Collection Stack

### 1. JMX Exporter (kafka-cluster/jmx-exporter/kafka-config.yml)
Collects JVM and Kafka broker metrics via JMX:
- **Location**: Runs on each broker (port 7071)
- **Metrics Exported**: JVM heap/non-heap memory, CPU, thread count, Kafka throughput

### 2. cAdvisor
Collects container-level metrics:
- **Metrics**: Container CPU and memory usage for Kafka containers

### 3. Node Exporter  
Collects host-level system metrics:
- **Metrics**: Node CPU, memory, load, I/O metrics

### 4. Prometheus
Scrapes metrics from all exporters and applies recording rules:
- **Configuration**: monitoring-dash/prometheus/prometheus.yml
- **Recording Rules**: monitoring-dash/prometheus/recording-rules.yml

### 5. Grafana
Visualizes metrics from Prometheus:
- **Dashboard**: kafka-eos-baseline-phase1.json

---

## Available Metrics

### Throughput Metrics

#### Bytes-based Throughput (NEW)
- `kafka_cluster_bytes_in_bytes_per_sec` - Cluster total ingress throughput in bytes/sec
- `kafka_cluster_bytes_out_bytes_per_sec` - Cluster total egress throughput in bytes/sec
- `kafka_broker_bytes_in_total` - Raw counter from JMX
- `kafka_broker_bytes_out_total` - Raw counter from JMX

#### Kilobyte-based Throughput
- `kafka_cluster_bytes_in_kb_per_sec` - Cluster total ingress throughput in KB/sec
- `kafka_cluster_bytes_out_kb_per_sec` - Cluster total egress throughput in KB/sec
- `kafka_broker_bytes_in_kb_per_sec` - Per-broker ingress throughput in KB/sec
- `kafka_broker_bytes_out_kb_per_sec` - Per-broker egress throughput in KB/sec

#### Message-based Throughput
- `kafka_cluster_messages_in_per_sec` - Cluster total messages received per second
- `kafka_broker_messages_in_per_sec` - Per-broker messages received per second
- `kafka_broker_messages_in_total` - Raw counter from JMX

### CPU Metrics (NEW)

#### JVM CPU Metrics
- `jvm_process_cpu_load_ratio` - Process CPU load ratio (0-1)
- `jvm_process_cpu_time_nanoseconds` - Total CPU time in nanoseconds (counter)
- `jvm_os_system_cpu_load_ratio` - System CPU load ratio (0-1)

#### Recording Rules (Processed)
- `kafka_broker_cpu_usage_percent` - CPU usage per broker in percentage (0-100%)
- `kafka_cluster_cpu_usage_percent` - Average CPU usage across cluster in percentage

### Memory Metrics (NEW)

#### JVM Memory Metrics
- `jvm_heap_used_bytes` - Heap memory currently used
- `jvm_heap_max_bytes` - Maximum heap memory allocated
- `jvm_non_heap_used_bytes` - Non-heap memory used (e.g., metaspace)

#### Recording Rules (Processed)
- `kafka_broker_heap_memory_mb` - Heap memory used per broker in MB
- `kafka_broker_heap_memory_max_mb` - Max heap memory per broker in MB
- `kafka_broker_heap_memory_usage_percent` - Heap memory utilization percentage (0-100%)
- `kafka_broker_non_heap_memory_mb` - Non-heap memory per broker in MB
- `kafka_cluster_heap_memory_usage_percent` - Average heap memory utilization across cluster

### Kafka Broker Metrics

#### Replication & Partitions
- `kafka_under_replicated_partitions` - Number of under-replicated partitions
- `kafka_partition_count` - Total partition count on broker
- `kafka_leader_count` - Number of leader replicas on broker

#### Controller
- `kafka_active_controller` - Indicates if broker is active controller (1 or 0)

### Request Latency Metrics

#### Produce Request Latency
- `kafka_request_produce_latency_p99_ms` - Produce request latency at p99 percentile in milliseconds

### System Metrics

#### Processor Info
- `kafka_available_processors` - Number of available processor cores

---

## Dashboard Panels

The Grafana dashboard (`kafka-eos-baseline-phase1.json`) includes:

1. **Broker Input Throughput** (records/sec)
2. **Producer Output Throughput** (records/sec)
3. **Input vs Output Throughput - Cluster Total** (records/sec)
4. **Consumer Lag**
5. **Broker Request Latency** (ms)
6. **Kafka Network Throughput** (KB/sec) - Per broker bytes in/out
7. **JVM GC Pause Time** (ms/sec)
8. **Kafka Thread Count**
9. **Kafka Container CPU** (%) - From cAdvisor
10. **Kafka Container Memory** (bytes) - From cAdvisor
11. **Throughput per CPU Core** (ops/sec)
12. **Cluster Throughput** (Bytes/sec) - NEW: Total cluster ingress/egress
13. **Broker CPU Usage** (%) - NEW: Per-broker and cluster average CPU
14. **Broker Heap Memory** (MB) - NEW: Heap used and max per broker
15. **Broker Heap Memory Usage** (%) - NEW: Per-broker and cluster average memory usage

---

## Configuration Files

### JMX Exporter Config
**File**: `kafka-cluster/jmx-exporter/kafka-config.yml`

Contains patterns to extract metrics from Kafka broker JMX:
- Throughput metrics (bytes in/out, messages in)
- Memory metrics (heap, non-heap)
- CPU metrics (JVM process CPU, OS system CPU)
- Broker health (under-replicated partitions, leader count)
- Request latency (p99 percentile)

### Prometheus Config
**File**: `monitoring-dash/prometheus/prometheus.yml`

Defines scrape jobs for:
- cAdvisor (container metrics)
- Node Exporter (system metrics)
- Kafka JMX Exporters (broker 1, 2, 3)

### Recording Rules
**File**: `monitoring-dash/prometheus/recording-rules.yml`

Pre-computed metrics updated every 30 seconds:
- **kafka_throughput group**: Network throughput calculations (KB/sec and Bytes/sec)
- **kafka_broker_resources group**: CPU and memory aggregations and calculations

### Grafana Dashboard
**File**: `monitoring-dash/kafka-eos-baseline-phase1.json`

JSON definition of the monitoring dashboard with 15 visualization panels.

---

## Metric Calculation Examples

### Cluster Throughput (Bytes/sec)
```
kafka_cluster_bytes_in_bytes_per_sec = sum(rate(kafka_broker_bytes_in_total[5m]))
kafka_cluster_bytes_out_bytes_per_sec = sum(rate(kafka_broker_bytes_out_total[5m]))
```

### Broker CPU Usage (%)
```
kafka_broker_cpu_usage_percent = jvm_process_cpu_load_ratio * 100
```

### Broker Heap Memory Usage (%)
```
kafka_broker_heap_memory_usage_percent = (jvm_heap_used_bytes / jvm_heap_max_bytes) * 100
```

---

## Troubleshooting

### Missing Metrics
1. **Check JMX config**: Ensure `kafka-cluster/jmx-exporter/kafka-config.yml` has the metric patterns
2. **Verify Prometheus scrape**: Check `prometheus.yml` targets are accessible
3. **Check Prometheus UI**: Visit `localhost:9090` to verify metrics are being scraped

### Metric Not Appearing in Dashboard
1. **Verify recording rule**: Check `recording-rules.yml` for the metric definition
2. **Check Prometheus logs**: Look for evaluation errors
3. **Verify query syntax**: Test queries in Prometheus UI first

### High Memory/CPU in Dashboard
1. Monitor `kafka_broker_heap_memory_usage_percent` for memory pressure
2. Monitor `kafka_broker_cpu_usage_percent` for CPU pressure
3. Check GC pause time in `JVM GC Pause Time` panel

---

## Performance Tuning Metrics

Use these metrics to guide performance tuning:

1. **Throughput Analysis**: Compare `kafka_cluster_bytes_in_bytes_per_sec` with expected workload
2. **CPU Efficiency**: Use `Throughput per CPU Core` to calculate bytes/sec per CPU
3. **Memory Pressure**: Monitor `kafka_broker_heap_memory_usage_percent` to avoid GC storms
4. **Request Latency**: Track `kafka_request_produce_latency_p99_ms` to identify performance degradation

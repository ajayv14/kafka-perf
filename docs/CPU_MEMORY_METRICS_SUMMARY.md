# Kafka Cluster CPU and Memory Metrics Summary

## Goal
Track Kafka container CPU and memory with recording rules that remain valid as cAdvisor label shape changes.

## Selected Metrics
1. `kafka_cluster_cpu_percent`
2. `kafka_cluster_container_memory_gb`

## Current Recording Rules

### Cluster CPU percent
```promql
kafka_cluster_cpu_percent = sum(kafka_broker_cpu_percent)
```

where `kafka_broker_cpu_percent` is computed from cAdvisor container CPU rate using a robust selector chain:
- Per-broker ID selector (`id=~"/docker/<container-id>"`) for `kafka-1/2/3`
- Fallback compose label selector (`container_label_com_docker_compose_service=~"kafka-[0-9]+"`)
- Fallback name selector (`name=~"kafka-[0-9]+"`)

### Cluster container memory (GB)
```promql
kafka_cluster_container_memory_gb =
sum(
  container_memory_usage_bytes{id=~"/docker/(<kafka-1-id>|<kafka-2-id>|<kafka-3-id>)"}
  or
  container_memory_usage_bytes{container_label_com_docker_compose_service=~"kafka-[0-9]+"}
  or
  container_memory_usage_bytes{name=~"kafka-[0-9]+"}
) / 1073741824
```

## Interpretation
- `kafka_cluster_cpu_percent = 12.5` means combined Kafka broker CPU is ~12.5% (sum of broker CPU percentages).
- `kafka_cluster_container_memory_gb = 11.9` means combined Kafka broker container memory usage is ~11.9 GB.

## Notes
- Scrape interval is 30s in `monitoring-dash/prometheus/prometheus.yml`.
- cAdvisor label availability can differ by runtime; this setup prefers container IDs and falls back to labels.
- Container IDs change when containers are recreated.

## Refresh IDs After Container Recreate
Use:

```bash
./monitoring-dash/prometheus/refresh-kafka-cadvisor-ids.sh
```

The script updates Kafka ID selectors in `monitoring-dash/prometheus/recording-rules.yml` and reloads Prometheus.

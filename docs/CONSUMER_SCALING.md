# Scalable Kafka Consumer Deployment

## Overview

The `ScalableConsumer` is a lightweight, Docker-native consumer designed for horizontal scaling. It removes performance measurement overhead and focuses on consuming messages efficiently across multiple instances.

## Key Features

- **No Performance Measurement Overhead**: Simplified code for production consumption
- **Environment Variable Configuration**: All settings configurable via env vars
- **Horizontal Scaling**: Deploy multiple consumer instances easily
- **Optimized JVM Settings**: G1GC with tuned pause times
- **Health Checks**: Built-in container health monitoring
- **Structured Logging**: Periodic progress reporting

## Building the Docker Image

```bash
# From the project root
mvn clean package

# Build Docker image
docker build -f Dockerfile -t kafka-perf:latest .
```

## Configuration

The consumer accepts configuration through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Bootstrap server addresses |
| `KAFKA_TOPIC` | `eos-topic` | Topic to consume from |
| `KAFKA_GROUP_ID` | `scalable-consumer-group` | Consumer group ID |
| `ISOLATION_LEVEL` | `read_committed` | `read_committed` or `read_uncommitted` |
| `AUTO_COMMIT` | `false` | Enable auto-commit offsets |
| `POLL_TIMEOUT_MS` | `1000` | Poll timeout in milliseconds |
| `LOG_INTERVAL` | `10000` | Log stats every N messages |
| `JAVA_HEAP_OPTS` | `-Xms512m -Xmx512m` | JVM heap settings |

## Deployment Examples

### Start Kafka Cluster (if not already running)

```bash
cd kafka-cluster
docker-compose up -d
```

### Single Consumer Instance

```bash
# From the sink directory
docker-compose -f docker-compose-consumer.yml up -d

# View logs
docker-compose -f docker-compose-consumer.yml logs -f consumer
```

### Multiple Consumer Instances (Horizontal Scaling)

```bash
# Start 3 consumer instances
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=3

# Verify running instances
docker-compose -f docker-compose-consumer.yml ps

# View logs from all instances
docker-compose -f docker-compose-consumer.yml logs -f
```

### Custom Configuration

```bash
# With custom broker addresses and topic
KAFKA_BROKERS="broker1:9092,broker2:9092" \
KAFKA_TOPIC="my-topic" \
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=2

# With read_uncommitted isolation level
ISOLATION_LEVEL="read_uncommitted" \
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=5
```

### Scale After Starting

```bash
# Start with 2 instances
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=2

# Later, scale to 5 instances
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=5

# Scale back to 3
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=3
```

## Load Balancing

When multiple instances join the same consumer group (`KAFKA_GROUP_ID`), Kafka automatically distributes partitions among them:

- **2 instances** of `scalable-consumer-group` → Each gets ~50% of partitions
- **3 instances** of `scalable-consumer-group` → Each gets ~33% of partitions
- **4 instances** of `scalable-consumer-group` → Each gets ~25% of partitions

Partition rebalancing happens automatically. Monitor logs for rebalancing messages.

## Monitoring

### Check Consumer Status

```bash
# List running containers
docker-compose -f docker-compose-consumer.yml ps

# View specific container logs
docker-compose -f docker-compose-consumer.yml logs kafka-consumer-1

# Real-time log streaming (all instances)
docker-compose -f docker-compose-consumer.yml logs -f --tail=50
```

### Monitor Consumer Group

```bash
# From inside Kafka cluster
docker exec kafka-1 bash

# List consumer groups
kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe consumer group
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group scalable-consumer-group --describe

# Reset offsets if needed (WARNING: destroys progress)
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group scalable-consumer-group \
  --reset-offsets --to-earliest --all-topics --execute
```

## Performance Tuning

### Increase Message Throughput

```bash
# Use larger batch sizes and poll timeouts
POLL_TIMEOUT_MS=5000 \
LOG_INTERVAL=50000 \
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=8
```

### Reduce GC Pauses

```bash
# Heap settings are in docker-compose-consumer.yml
# Default: -Xms512m -Xmx512m
# Adjust JAVA_HEAP_OPTS for your workload
```

### Commit Strategy

```bash
# Manual commits (default - more control)
# In EOS scenarios, recommended

# Auto-commit (faster but less reliable)
AUTO_COMMIT=true \
docker-compose -f docker-compose-consumer.yml up -d
```

## Cleanup

```bash
# Stop all consumer instances
docker-compose -f docker-compose-consumer.yml down

# Remove all consumer data
docker-compose -f docker-compose-consumer.yml down -v

# Stop and remove everything
docker-compose -f docker-compose-consumer.yml down --remove-orphans
```

## Troubleshooting

### Consumers Not Consuming

Check that:
1. Kafka cluster is running: `docker-compose ps` from kafka-cluster directory
2. Topic exists with data
3. Network connectivity: `docker-compose ps | grep kafka`

### Consumer Lag Issues

1. Increase number of consumers (up to partition count)
2. Check logs for errors: `docker-compose logs consumer`
3. Verify broker health: `docker-compose logs kafka-broker-1`

### Out of Memory

Increase heap size:

```bash
JAVA_HEAP_OPTS="-Xms1g -Xmx1g" \
docker-compose -f docker-compose-consumer.yml up -d
```

### Container Keeps Restarting

1. Check logs: `docker-compose logs consumer`
2. Verify bootstrap servers are accessible
3. Ensure network connectivity between containers

## Performance Characteristics

With default settings (512MB heap, 1 consumer):

- **Throughput**: ~100K-500K messages/sec (depends on message size and cluster)
- **Latency**: <50ms from production to consumption
- **CPU**: 20-40% on single core
- **Memory**: 200-300MB used (out of 512MB limit)

Scales linearly with additional instances.

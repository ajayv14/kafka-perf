# Scalable Kafka Consumer - Docker Deployment Guide

## Overview

The `BaselineConsumer` has been refactored to remove performance measurement overhead and enable horizontal scaling using Docker Compose. The consumer now focuses on efficient message consumption with configurable behavior.

## Key Changes

### Removed
- Performance iteration tracking (NUM_ITERATIONS, test harness)
- CSV file output and metrics calculations
- Latency measurement and percentile calculations
- Duplicate detection overhead
- Consumer group reset logic

### Added
- Environment variable configuration
- Simplified continuous consumption loop
- Real-time throughput logging at configurable intervals
- Graceful shutdown handling
- Docker-optimized container design

## Environment Variables

Configure the consumer using these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Bootstrap servers (comma-separated) |
| `TOPIC` | `test-topic` | Kafka topic to consume from |
| `GROUP_ID` | `scalable-consumer-group` | Consumer group ID (shared for load balancing) |
| `ISOLATION_LEVEL` | `read_committed` | `read_committed` or `read_uncommitted` |
| `LOG_INTERVAL_SECS` | `10` | Stats logging interval in seconds |
| `JAVA_HEAP_OPTS` | `-Xms512m -Xmx512m` | Java heap size |

## Building the Docker Image

```bash
cd /path/to/kafka-perf/app

# Build the Docker image
docker build -t kafka-perf:latest .
```

## Deployment Examples

### Start Single Consumer

```bash
cd docker-compose.yml directory

# Start one consumer connected to Kafka cluster
docker-compose up -d --scale consumer=1

# View logs
docker-compose logs -f consumer
```

### Scale to Multiple Consumers

```bash
# Start with 3 consumer instances
docker-compose up -d --scale consumer=3

# Scale to 5 instances
docker-compose up -d --scale consumer=5

# Scale down to 2 instances
docker-compose up -d --scale consumer=2

# View all consumers
docker-compose ps
```

### View Real-time Logs

```bash
# View all consumer logs
docker-compose logs -f consumer

# Follow specific consumer
docker-compose logs -f consumer_1

# Show last 50 lines and follow
docker-compose logs -f --tail=50 consumer
```

### Environment Variable Customization

#### Single Consumer with Custom Topic

```bash
docker-compose up -d \
  -e TOPIC=my-custom-topic \
  -e GROUP_ID=my-consumer-group \
  --scale consumer=1
```

#### Multiple Consumers with Custom Brokers

```bash
export KAFKA_BROKERS=kafka-1:29092,kafka-2:29092,kafka-3:29092
export TOPIC=performance-test
export GROUP_ID=perf-consumer-group
export ISOLATION_LEVEL=read_uncommitted

docker-compose up -d --scale consumer=4
```

#### Custom via `.env` File

Create a `.env` file:

```env
KAFKA_BROKERS=kafka-1:29092,kafka-2:29092,kafka-3:29092
TOPIC=test-data
GROUP_ID=scaling-test-group
ISOLATION_LEVEL=read_committed
LOG_INTERVAL_SECS=5
JAVA_HEAP_OPTS=-Xms1g -Xmx1g
```

Then run:

```bash
docker-compose up -d --scale consumer=3
```

## Load Balancing & Partitioning

Consumer instances automatically balance across topic partitions:

- Each instance joins the same consumer group
- Kafka broker assigns partitions to instances
- Rebalancing occurs when instances are added/removed
- All instances commit offsets together

**Example with 3 partitions:**
- 1 consumer: processes all 3 partitions
- 2 consumers: each processes ~1.5 partitions (rebalanced)
- 3 consumers: each processes 1 partition (optimal)
- 4+ consumers: some remain idle

## Docker Compose Configuration

The `docker-compose-consumer.yml` provides:

- **Scalable service**: Use `--scale consumer=N`
- **Resource limits**: 1 CPU / 1GB RAM per container (adjustable)
- **Network**: Connects to existing `kafka-network`
- **Logging**: JSON driver with 10MB max file size, 3 rotations
- **Restart policy**: Auto-restart on failure

### Resource Allocation

Adjust per-container limits in `docker-compose-consumer.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '1'        # 1 CPU core
      memory: 1G       # 1GB RAM
    reservations:
      cpus: '0.5'      # Reserve 0.5 CPU
      memory: 512m     # Reserve 512MB
```

## Monitoring

### Check Consumer Status

```bash
# View running consumers
docker-compose ps

# Check resource usage
docker stats

# View consumer group details
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group scalable-consumer-group --describe
```

### Log Output Example

```
[HH:MM:SS] Total: 50,000 messages | Throughput: 5,000.00 msg/sec | Interval: 4,999.99 msg/sec
[HH:MM:SS] Total: 100,000 messages | Throughput: 4,999.99 msg/sec | Interval: 5,000.01 msg/sec
```

## Graceful Shutdown

```bash
# Stop all consumers gracefully (waits up to 10s)
docker-compose down

# Force immediate stop
docker-compose down --force

# View shutdown stats on logs
```

Consumers will:
1. Stop accepting new messages
2. Complete current batch processing
3. Print final statistics
4. Close Kafka connections cleanly

## Performance Tuning

### Java GC Optimization

The Dockerfile includes G1GC settings:

```
-XX:+UseG1GC                           # Use G1 garbage collector
-XX:MaxGCPauseMillis=20                # Target 20ms pause
-XX:InitiatingHeapOccupancyPercent=35  # Start concurrent GC at 35% heap
-XX:G1HeapRegionSize=16M               # Region size (512m heap / 32)
-XX:MetaspaceSize=96m                  # Metaspace allocation
```

### Memory Configuration

For different workloads:

**Light Load (small messages, low throughput):**
```
JAVA_HEAP_OPTS=-Xms256m -Xmx256m
```

**Medium Load (typical workload):**
```
JAVA_HEAP_OPTS=-Xms512m -Xmx512m
```

**Heavy Load (large batches, high throughput):**
```
JAVA_HEAP_OPTS=-Xms1g -Xmx1g
```

### CPU Allocation

Scale based on partition count and throughput:

- **1-2 partitions**: 1 CPU, 1 consumer
- **3-5 partitions**: 0.5-1 CPU per consumer, 3-5 consumers
- **10+ partitions**: 1 CPU per consumer, match partition count

## Troubleshooting

### Consumer Not Receiving Messages

```bash
# Check connectivity
docker-compose exec consumer telnet kafka-1 29092

# View consumer group status
kafka-consumer-groups --bootstrap-server kafka-1:29092 \
  --group scalable-consumer-group --describe

# Check topic details
kafka-topics --bootstrap-server kafka-1:29092 --describe --topic test-topic
```

### High Memory Usage

- Reduce `MAX_POLL_RECORDS_CONFIG` (default: 500)
- Reduce `JAVA_HEAP_OPTS`
- Check for stuck consumers (increase rebalance timeout)

### Messages Processing Slowly

- Increase number of consumer instances
- Check broker disk I/O and CPU
- Verify network connectivity
- Increase consumer poll batch size

### Container Crashes

Check logs:
```bash
docker-compose logs consumer | tail -50
```

Common causes:
- Out of memory: reduce workload or increase heap
- Network issues: verify KAFKA_BROKERS setting
- Topic doesn't exist: create topic first

## Example: Complete Production Setup

```bash
# 1. Create topic with 5 partitions
kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic production-data \
  --partitions 5 \
  --replication-factor 3

# 2. Create .env file
cat > .env << EOF
KAFKA_BROKERS=kafka-1:29092,kafka-2:29092,kafka-3:29092
TOPIC=production-data
GROUP_ID=production-consumers
ISOLATION_LEVEL=read_committed
LOG_INTERVAL_SECS=30
JAVA_HEAP_OPTS=-Xms1g -Xmx1g
EOF

# 3. Start 5 consumer instances (one per partition)
docker-compose up -d --scale consumer=5

# 4. Monitor
watch -n 5 'docker-compose ps && echo "---" && docker stats --no-stream'

# 5. View throughput
docker-compose logs -f --tail=20 consumer | grep "Throughput"
```

## Cleanup

```bash
# Stop and remove all containers and networks
docker-compose down

# Remove Docker image
docker image rm kafka-perf:latest

# Clean up volumes (if using persistent storage)
docker volume prune
```

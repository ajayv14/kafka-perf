# Scalable Kafka Consumer with Retries - Implementation Guide

## Overview

The BaselineConsumer has been successfully converted into a **ScalableConsumer** with the following enhancements:

1. **Retry Logic with Exponential Backoff**: Configurable retry attempts with exponential backoff strategy
2. **Property-based Configuration**: All settings managed through `benchmark.properties` with environment variable overrides
3. **Horizontal Scaling**: Docker Compose configured to scale to 3 instances (or more)
4. **Consumer Group Coordination**: Automatic load balancing across consumer instances

---

## Configuration Files

### 1. benchmark.properties
**Location**: `app/src/main/resources/benchmark.properties`

**New Consumer Configuration Section**:
```properties
########### SCALABLE CONSUMER CONFIGURATION ###########

# Consumer broker connection
consumer.bootstrap.servers=localhost:9092,localhost:9093,localhost:9094
consumer.topic=test-topic
consumer.group.id=scalable-consumer-group

# Consumer retries and reliability
consumer.retries.max.attempts=5
consumer.retries.backoff.ms=100
consumer.retries.backoff.max.ms=10000

# Consumer polling and processing
consumer.max.poll.records=500
consumer.poll.timeout.ms=1000
consumer.session.timeout.ms=30000
consumer.heartbeat.interval.ms=10000
consumer.max.poll.interval.ms=300000

# Isolation level: read_committed or read_uncommitted
consumer.isolation.level=read_committed

# Auto commit settings
consumer.enable.auto.commit=true
consumer.auto.commit.interval.ms=5000
consumer.auto.offset.reset=earliest

# Logging
consumer.log.interval.secs=10

# Deserializers
consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```

**Configuration Properties**:

| Property | Default | Description |
|----------|---------|-------------|
| `consumer.bootstrap.servers` | localhost:9092,localhost:9093,localhost:9094 | Kafka broker addresses |
| `consumer.topic` | test-topic | Topic to consume from |
| `consumer.group.id` | scalable-consumer-group | Consumer group ID for coordination |
| `consumer.retries.max.attempts` | 5 | Maximum connection retry attempts |
| `consumer.retries.backoff.ms` | 100 | Initial backoff time (ms) |
| `consumer.retries.backoff.max.ms` | 10000 | Maximum backoff time (ms) |
| `consumer.max.poll.records` | 500 | Records fetched per poll |
| `consumer.session.timeout.ms` | 30000 | Session timeout (ms) |
| `consumer.isolation.level` | read_committed | read_committed or read_uncommitted |
| `consumer.enable.auto.commit` | true | Enable automatic offset commits |
| `consumer.auto.offset.reset` | earliest | Offset reset strategy |

---

### 2. ScalableConsumer Class
**Location**: `app/src/main/java/com/kafka/perf/baseline/ScalableConsumer.java`

**Key Features**:

- **Automatic Retry Mechanism**: Implements exponential backoff with configurable max attempts
- **Property File Loading**: Loads from `benchmark.properties` resource
- **Environment Variable Override**: Environment variables override property file settings
- **Connection Pool**: Manages consumer lifecycle with proper cleanup
- **Statistics Tracking**: Real-time throughput monitoring and final performance metrics

**Configuration Priority**:
1. Environment Variables (highest priority)
2. benchmark.properties file
3. Hardcoded defaults (lowest priority)

---

### 3. Docker Configuration

#### Dockerfile
**Location**: `app/Dockerfile`

Updated to use `ScalableConsumer` as default main class:
```dockerfile
ENV MAIN_CLASS=com.kafka.perf.baseline.ScalableConsumer
```

#### docker-compose-consumer.yml
**Location**: `app/docker-compose-consumer.yml`

**Key Updates**:
- Added `replicas: 3` for default 3-instance deployment
- New retry configuration environment variables
- Updated container naming for better identification
- Enhanced documentation

---

## Deployment Instructions

### Build the Docker Image

```bash
# From the app directory
docker build -t kafka-perf:latest .
```

### Deploy Scalable Consumer

#### Option 1: Deploy with 3 instances (default)
```bash
cd app
docker-compose -f docker-compose-consumer.yml up -d
```

#### Option 2: Deploy and scale to specific number
```bash
cd app
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=3
```

#### Option 3: Scale up/down existing deployment
```bash
cd app
# Scale from 3 to 5 instances
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=5

# Scale down to 2 instances
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=2
```

### Monitor Consumer Logs

```bash
# View all consumer logs
docker-compose -f docker-compose-consumer.yml logs -f consumer

# View specific consumer instance
docker-compose -f docker-compose-consumer.yml logs -f consumer_1

# Follow logs with timestamps
docker-compose -f docker-compose-consumer.yml logs -f --timestamps consumer
```

### Stop Consumers

```bash
cd app
docker-compose -f docker-compose-consumer.yml down
```

---

## Environment Variable Overrides

Override any configuration using environment variables:

```bash
# Example: Custom Kafka brokers and retry settings
export KAFKA_BROKERS="kafka-1:29092,kafka-2:29092,kafka-3:29092"
export TOPIC="my-custom-topic"
export GROUP_ID="my-consumer-group"
export RETRIES_MAX_ATTEMPTS=10
export RETRIES_BACKOFF_MS=200
export RETRIES_BACKOFF_MAX_MS=15000
export LOG_INTERVAL_SECS=5

docker-compose -f docker-compose-consumer.yml up -d --scale consumer=3
```

---

## Retry Logic Details

The `ScalableConsumer` implements intelligent retry logic:

1. **Initial Connection Attempt**: Tries to establish Kafka consumer connection
2. **On Failure**: 
   - Calculates backoff time: `backoff_ms = min(backoff_ms * 2, max_backoff_ms)`
   - Waits for backoff duration
   - Retries connection
3. **On Success**: 
   - Resets retry counter and backoff time
   - Begins consuming messages
4. **Max Retries Exceeded**: Gracefully shuts down with error

**Example Backoff Sequence** (default settings):
```
Attempt 1: Fail → Wait 100ms
Attempt 2: Fail → Wait 200ms
Attempt 3: Fail → Wait 400ms
Attempt 4: Fail → Wait 800ms
Attempt 5: Fail → Wait 1600ms
Attempt 6: Fail → Wait 3200ms
...continues up to 10000ms (max)
```

---

## Consumer Group Load Balancing

All 3 instances share the same `consumer.group.id` (`scalable-consumer-group`), enabling:

- **Automatic Partition Assignment**: Kafka automatically distributes topic partitions among instances
- **Rebalancing**: When instances are added/removed, Kafka rebalances partition assignments
- **Fault Tolerance**: If one instance fails, others assume its partitions
- **Throughput Scaling**: Proportional increase in message processing as instances scale

**Example**:
- Topic with 6 partitions + 3 consumer instances = 2 partitions per instance
- Scale to 6 instances = 1 partition per instance (linear throughput increase)

---

## Resource Management

### Per-Container Resource Limits
```yaml
resources:
  limits:
    cpus: '1'
    memory: 1G
  reservations:
    cpus: '0.5'
    memory: 512m
```

### Total Cluster Resources (3 instances)
- **CPU Reserved**: 1.5 cores
- **CPU Hard Limit**: 3 cores
- **Memory Reserved**: 1.5GB
- **Memory Hard Limit**: 3GB

Adjust based on your Kafka topic partition count and throughput requirements.

---

## Monitoring and Troubleshooting

### Check Consumer Status
```bash
# View active containers
docker-compose -f docker-compose-consumer.yml ps

# Inspect specific container
docker-compose -f docker-compose-consumer.yml exec consumer ps aux
```

### View Consumer Metrics
Consumer instances log:
- **Total messages consumed**: Cumulative count
- **Throughput (msg/sec)**: Real-time and interval-based
- **Connection status**: Retries and backoff information
- **Offset commits**: Auto-commit activity

### Common Issues

**Issue**: Consumer repeatedly retrying
```
✗ Connection attempt 1 failed: ...
Retrying in 100ms...
```
**Solution**: Verify Kafka brokers are running and accessible

**Issue**: Consumers not balancing partitions
```
Consumer started, listening on topic: test-topic
```
**Solution**: Check consumer group ID is consistent across all instances

**Issue**: High memory usage
**Solution**: Adjust `JAVA_HEAP_OPTS` environment variable in docker-compose.yml

---

## Performance Tuning

### For Higher Throughput
```properties
consumer.max.poll.records=1000          # Increase from 500
consumer.auto.commit.interval.ms=10000  # Less frequent commits
consumer.session.timeout.ms=45000       # More tolerance for processing
```

### For Lower Latency
```properties
consumer.max.poll.records=100           # Reduce from 500
consumer.auto.commit.interval.ms=1000   # More frequent commits
consumer.session.timeout.ms=20000       # Less tolerance for delays
```

### For Reliability
```properties
consumer.retries.max.attempts=10        # More retry attempts
consumer.retries.backoff.max.ms=30000   # Longer max backoff
consumer.session.timeout.ms=45000       # More processing tolerance
```

---

## Scaling Examples

### 3-Instance Production Setup
```bash
export KAFKA_BROKERS="prod-kafka-1:29092,prod-kafka-2:29092,prod-kafka-3:29092"
export TOPIC="prod-events"
export GROUP_ID="prod-consumer-group"
export RETRIES_MAX_ATTEMPTS=10
export RETRIES_BACKOFF_MAX_MS=30000
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=3
```

### Development/Testing Setup
```bash
export KAFKA_BROKERS="localhost:9092"
export TOPIC="test-topic"
export GROUP_ID="test-consumer-group"
export LOG_INTERVAL_SECS=5
docker-compose -f docker-compose-consumer.yml up -d --scale consumer=1
```

---

## Summary of Changes

| Component | Change | Benefit |
|-----------|--------|---------|
| **ScalableConsumer.java** | New class with retry logic | Resilient to Kafka broker failures |
| **benchmark.properties** | Consumer config section added | Centralized, easy configuration |
| **docker-compose-consumer.yml** | Updated with replicas: 3 | Production-ready scaling |
| **Dockerfile** | Default main class updated | Automatic use of ScalableConsumer |

All configuration is now **property-driven** and **environment-variable overridable**, making it ideal for containerized deployments.

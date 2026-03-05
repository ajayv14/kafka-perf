# Docker Compose Master Manager

Master Python script to manage all Docker services for Kafka performance testing.

## Overview

This script simplifies managing three interconnected Docker Compose stacks:
- **Kafka Cluster** - 3-node Kafka cluster with JMX metrics
- **Monitoring Dashboard** - Prometheus + Grafana for metrics visualization
- **Sink** - PostgreSQL database for storing test data

## Features

✅ Start/stop services with proper dependency management  
✅ View service status  
✅ Stream logs from any service  
✅ Restart services  
✅ Clean up all containers and volumes  
✅ Verbose output for debugging  
✅ Smart ordering (Kafka starts first, monitoring/sink depend on it)  

## Installation

The script is ready to use. No additional dependencies beyond Docker and Docker Compose.

```bash
# Already executable, just run it:
./docker-master.py --help
```

Or run it as a Python module:

```bash
python3 docker-master.py --help
```

## Usage

### Start All Services

```bash
./docker-master.py up
```

This starts services in the correct order:
1. Kafka Cluster (no dependencies)
2. Monitoring Dashboard (depends on Kafka)
3. Sink (depends on Kafka)

### Start Specific Service

```bash
# Start only Kafka
./docker-master.py up --service kafka

# Start only Monitoring
./docker-master.py up --service monitoring

# Start only Sink
./docker-master.py up --service sink
```

### Stop Services

```bash
# Stop all services
./docker-master.py down

# Stop only Kafka (and dependent services)
./docker-master.py down --service kafka

# Stop monitoring
./docker-master.py down --service monitoring
```

### Check Service Status

```bash
./docker-master.py status
```

Output shows running containers for each service:
```
📊 Service Status:

🔹 Kafka Cluster (kafka-cluster):
   CONTAINER ID   IMAGE                 STATUS
   abc123def456   apache/kafka:4.1.1    Up 2 minutes
   ...

🔹 Monitoring Dashboard (monitoring-dash):
   ...
```

### View Logs

```bash
# View Kafka logs (last 100 lines)
./docker-master.py logs --service kafka

# View logs with 50 lines
./docker-master.py logs --service kafka --tail 50

# Follow logs in real-time
./docker-master.py logs --service kafka --follow

# Follow monitoring logs
./docker-master.py logs --service monitoring --follow

# Follow sink logs
./docker-master.py logs --service sink --follow
```

### Restart Services

```bash
# Restart all services
./docker-master.py restart

# Restart specific service
./docker-master.py restart --service kafka
```

### Clean Up Everything

```bash
# Remove all containers and volumes
./docker-master.py clean

# Remove only Kafka containers/volumes
./docker-master.py clean --service kafka
```

### Verbose Output

Add `-v` or `--verbose` flag to see detailed command execution:

```bash
./docker-master.py up --verbose
./docker-master.py down -v
```

## Service Details

### Kafka Cluster
- **Location**: `kafka-cluster/docker-compose-kafka-cluster.yml`
- **Services**: 3-node Kafka cluster, Zookeeper
- **Ports**: 
  - Kafka: 9092, 9093, 9094
  - JMX Metrics: 7071, 7072, 7073
- **Network**: `kafka-cluster_kafka-network`

### Monitoring Dashboard
- **Location**: `monitoring-dash/docker-compose-monitoring.yml`
- **Services**: Prometheus, Grafana
- **Ports**:
  - Prometheus: 9090
  - Grafana: 3000
- **Depends On**: Kafka Cluster (for metrics collection)

### Sink
- **Location**: `sink/docker-sink-compose.yml`
- **Services**: PostgreSQL database
- **Ports**: 5432
- **Depends On**: Kafka Cluster (via network)

## Common Workflows

### Full Setup for Testing

```bash
# Start everything
./docker-master.py up

# Check status
./docker-master.py status

# Monitor Kafka in real-time
./docker-master.py logs --service kafka --follow
```

### Tear Down Everything

```bash
# Stop all services
./docker-master.py down

# Clean up all volumes and containers
./docker-master.py clean
```

### Monitor While Running

```bash
# In one terminal, start services
./docker-master.py up

# In another terminal, stream logs
./docker-master.py logs --service kafka --follow
```

### Restart Individual Service

```bash
# Restart just the monitoring dashboard
./docker-master.py restart --service monitoring
```

## Exit Codes

- `0` - Success
- `1` - Error (check output for details)

## Troubleshooting

### Port Already in Use

```bash
# Clean everything and start fresh
./docker-master.py clean
./docker-master.py up
```

### Service Won't Start

```bash
# Check logs for the service
./docker-master.py logs --service kafka

# Restart the problematic service
./docker-master.py restart --service kafka
```

### Network Issues

The script uses a shared Docker network (`kafka-cluster_kafka-network`). If services can't communicate:

```bash
# Stop everything
./docker-master.py down

# Clean up
./docker-master.py clean

# Start fresh
./docker-master.py up
```

## Script Architecture

### Classes

**`Service` Enum**
- Defines available services: KAFKA, MONITORING, SINK, ALL

**`ServiceConfig` Class**
- Encapsulates service configuration (path, compose file, dependencies)

**`DockerComposeManager` Class**
- Main orchestrator class
- Handles all docker-compose operations
- Manages service dependencies and ordering

### Key Methods

- `up()` - Start services in dependency order
- `down()` - Stop services in reverse dependency order
- `status()` - Display running containers
- `logs()` - Stream service logs
- `restart()` - Stop and start services
- `clean()` - Remove containers and volumes

## Customization

To add a new service, edit the `SERVICES` dictionary in the script:

```python
SERVICES = {
    Service.KAFKA: ServiceConfig(...),
    Service.MONITORING: ServiceConfig(...),
    Service.SINK: ServiceConfig(...),
    # Add new service here
    Service.CUSTOM: ServiceConfig(
        name="Custom Service",
        path="/path/to/service",
        compose_file="docker-compose.yml",
        depends_on=["kafka"]  # List services it depends on
    ),
}
```

## Requirements

- Docker (20.10+)
- Docker Compose (1.29+)
- Python 3.8+

## License

Same as parent project

## Notes

- Services are started/stopped in dependency order
- Kafka must be running for Monitoring and Sink to function
- The script uses `docker-compose` v3 format
- All containers run in detached mode by default
- Volumes are preserved unless explicitly cleaned


┌─────────────────────────────────┐         ┌──────────────────────────────┐
│   KAFKA MACHINE                 │         │   MONITORING MACHINE         │
│   (Your Kafka Server)           │         │   (Separate Server)          │
│                                 │         │                              │
│  ┌──────────┐  ┌──────────┐    │         │                              │
│  │ Kafka-1  │  │ Kafka-2  │    │         │  ┌────────────────────────┐  │
│  │ :9092    │  │ :9093    │    │         │  │    Prometheus          │  │
│  │ JMX:7071 │  │ JMX:7072 │    │  HTTP   │  │    :9090               │  │
│  └─────┬────┘  └─────┬────┘    │ ──────> │  │                        │  │
│        │             │          │         │  │  - Scrapes metrics     │  │
│  ┌─────┴─────────────┴──────┐  │         │  │  - Stores time-series  │  │
│  │ Kafka-3                  │  │         │  └────────┬───────────────┘  │
│  │ :9094   JMX:7073         │  │         │           │                  │
│  └──────────────────────────┘  │         │  ┌────────▼───────────────┐  │
│                                 │         │  │    Grafana             │  │
│  ┌──────────────────────────┐  │         │  │    :3000               │  │
│  │ cAdvisor      :8080      │──┼────────>│  │                        │  │
│  │ (Container metrics)      │  │         │  │  - Visualizes data     │  │
│  └──────────────────────────┘  │         │  └────────────────────────┘  │
│                                 │         │                              │
│  ┌──────────────────────────┐  │         │                              │
│  │ Node Exporter :9100      │──┼────────>│                              │
│  │ (Host metrics)           │  │         │                              │
│  └──────────────────────────┘  │         │                              │
│                                 │         │                              │
│  ┌──────────────────────────┐  │         │                              │
│  │ Kafka Exporter :9308     │──┼────────>│                              │
│  │ (Consumer group lag)     │  │         │                              │
│  └──────────────────────────┘  │         │                              │
└─────────────────────────────────┘         └──────────────────────────────┘





PART 2: Monitoring Machine Setup

# Step 1: Create Directory Structure

bash 
mkdir -p prometheus
mkdir -p grafana/provisioning/datasources
mkdir -p grafana/dashboards

# Step 2: Configure Prometheus

Edit prometheus-monitoring-machine.yml and replace <KAFKA_MACHINE_IP> with your Kafka machine's IP:


# Then copy to prometheus directory
cp prometheus-monitoring-machine.yml prometheus/prometheus.yml

# Step 3: Create Grafana Datasource Config
bash 

cat > grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
EOF

# Step 4: Start Kafka exporter, Prometheus and Grafana
bash

docker-compose -f docker-compose-monitoring.yml up -d

# Check containers
docker-compose -f docker-compose-monitoring.yml ps

# Step 5: Verify Prometheus is Scraping

Wait 1-2 minutes, then:

bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, state: .health}'
All targets should show "state": "up"

# Check consumer lag metric is present
curl -G 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=kafka_consumergroup_lag' | jq .

# Step 6: Access Grafana

Open browser: http://localhost:3000
Login: admin / admin

Import dashboard: Upload grafana-dashboard.json


Testing the Setup
From Monitoring Machine:

bash

# Test connectivity to Kafka machine exporters
curl http://<KAFKA_MACHINE_IP>:8080/metrics | head -10
curl http://<KAFKA_MACHINE_IP>:9100/metrics | head -10
curl http://<KAFKA_MACHINE_IP>:7071/metrics | grep kafka_process_cpu
curl http://<KAFKA_MACHINE_IP>:9308/metrics | grep kafka_consumergroup_lag

# Query Prometheus for data
curl -G 'http://localhost:9090/api/v1/query' \
  --data-urlencode 'query=kafka_process_cpu_usage_percent' | jq .
Expected Results:

You should see CPU metrics from all 3 Kafka brokers
Container CPU usage for kafka-1, kafka-2, kafka-3
Host system CPU metrics
Consumer group lag metrics from kafka-exporter
No errors in Prometheus targets page

# Setup docker

## Python Environment Setup

Create and activate virtual environment:
```bash
python3 -m venv kafka-perf-venv
source kafka-perf-venv/bin/activate
pip install kafka-python psycopg2-binary
```

**Note:** If you encounter `ModuleNotFoundError: No module named 'distutils'`, install it with:
```bash
pip install setuptools
```

Step 1 : 
path : ./kafka-cluster
Follow README and Setup kafka cluster in docker

Step 2 :
path : 
Setup postgres



Using master script :



# Start all services
./docker-master.py up

# Check status
./docker-master.py status

# Follow Kafka logs
./docker-master.py logs --service kafka --follow

# Stop everything
./docker-master.py down
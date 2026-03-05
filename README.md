# Setup docker


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
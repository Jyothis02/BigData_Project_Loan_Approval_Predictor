
## Quick Setup for New Users

### Clone the project
git clone https://github.com/YOURUSERNAME/loan-approval-prediction.git
cd loan-approval-prediction

### Install Python dependencies
pip3 install kafka-python flask pymongo

### Install system dependencies
sudo apt install openjdk-11-jdk sbt mongodb-org grafana-server

### Download Kafka and Spark
wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.13-3.5.0.tgz
wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
tar -xzf kafka_2.13-3.5.0.tgz
tar -xzf spark-3.4.1-bin-hadoop3.tgz

### Run the pipeline (in order)
# Terminal 1: Zookeeper
cd kafka_2.13-3.5.0 && bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka
cd kafka_2.13-3.5.0 && bin/kafka-server-start.sh config/server.properties

# Terminal 3: Producer
python3 src/main/python/producer.py

# Terminal 4: Spark Processor
sbt "runMain com.loan.Predictor"

# Terminal 5: Model Trainer (after data flows in)
sbt "runMain com.loan.ModelTrainer"

# Terminal 6: Flask API
python3 src/main/python/grafana_api.py

# Browser: http://localhost:3000 (Grafana)

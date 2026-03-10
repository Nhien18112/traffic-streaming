# 🚦 End-to-End Near Real-Time Traffic & Weather Streaming Pipeline

## 📖 Project Overview
This project is an end-to-end Data Engineering pipeline designed to ingest, process, and visualize urban traffic and transportation systems data in near real-time. By combining live traffic speeds, weather conditions, and AI-driven vehicle counts, this system provides a comprehensive command center for monitoring congestion across Ho Chi Minh City.

The architecture emphasizes enterprise-grade practices: containerization, micro-batching for cost optimization, and stream-stream joining for complex data transformations.

## 🏗️ Architecture & Data Flow
The pipeline follows a robust Near Real-Time (NRT) processing architecture:

1. **Orchestration & Ingestion:** `Apache Airflow` orchestrates Python scripts to fetch data from APIs (Traffic, Weather, Camera) and pushes raw payloads to Kafka.
2. **Message Broker:** `Apache Kafka` acts as the central nervous system, decoupling data producers from consumers via dedicated topics.
3. **Stream Processing:** `Apache Spark (Structured Streaming)` consumes Kafka topics, performs complex Stream-Stream window joins, applies watermarks to handle late data, and writes micro-batches (10s triggers).
4. **Data Storage:** Processed data is pushed to `PostgreSQL` (Neon Cloud) using optimized JDBC batch inserts to prevent connection overhead, while raw data is dumped into `MinIO` (Data Lake).
5. **Visualization:** `Grafana` queries the cloud database to render real-time heatmaps, traffic/weather correlation charts, and vehicle classification metrics.

<img width="1024" height="598" alt="image" src="https://github.com/user-attachments/assets/50aad5e4-a652-41e4-8580-9791d1e105dd" />


🛠️ Technology Stack
Language: Python, SQL, Scala/Java (underlying Spark/Kafka)

Orchestration: Apache Airflow

Message Broker: Apache Kafka, Zookeeper

Stream Processing: Apache Spark (Structured Streaming)

Databases: PostgreSQL (Neon Cloud - Data Warehouse), MinIO (Data Lake)

Visualization: Grafana

Infrastructure: Docker & Docker Compose

🚀 Key Features Highlights
Complex Stream Processing: Implemented multi-stream inner and left-outer joins with a 3-minute time window and 5-minute watermarking to handle out-of-order events.

Cost & Performance Optimization: Configured Spark micro-batching (trigger=10s) and JDBC batchsize to protect the Cloud Database from connection overhead (Anti-DDoS pattern).

AI Integration: Fused traditional API telemetry (speed, temperature) with AI-inferred metrics (motorcycle/car counts from camera feeds) into a single analytical table.

Dynamic Visualization: Built a Command Center dashboard featuring Time-series correlations, Gauge meters, Stacked Bar charts, and Live data feeds.

⚙️ How to Run Locally
1. Clone the repository

Bash
git clone [https://github.com/Nhien18112/traffic-streaming.git](https://github.com/Nhien18112/traffic-streaming.git)
cd traffic-streaming
2. Configure Environment Variables
Create a .env file in the root directory based on the .env.example provided. Add your Neon Database and API credentials.

3. Spin up the Infrastructure
Ensure Docker Desktop is running (allocate at least 8GB RAM), then execute:

Bash
docker-compose up -d
4. Access the Services

Airflow UI: http://localhost:8082 (Trigger the DAGs here)

Kafka UI: http://localhost:8081

MinIO Console: http://localhost:9001

Grafana: http://localhost:3000

📊 Dashboard Preview
[Traffic Command Center Dashboard](https://hominhnhien1805.grafana.net/goto/fffkm44i7jxmob?orgId=stacks-1552630)

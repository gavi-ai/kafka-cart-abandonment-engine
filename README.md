# 🛒 Real-Time E-Commerce Cart Abandonment Engine (Apache Kafka)

## 📌 Executive Summary
A high-throughput, distributed event-streaming infrastructure built to detect e-commerce revenue leaks in real-time. This system mimics enterprise-grade clickstream analytics, tracking active user sessions and instantly triggering recovery alerts when high-value carts are abandoned.

## 📸 Executive Dashboard
![Live Radar](dashboard-preview.png)

## 🏗️ The Architecture Flow
1. **Telemetry Ingestion:** `Faker` generates continuous, randomized user clickstream events (`view_item`, `add_to_cart`, `checkout`).
2. **Distributed Streaming:** A robust `confluent-kafka` Producer asynchronously pushes these events into a Dockerized **Apache Kafka** cluster.
3. **Stateful Processing:** The Consumer engine maintains an in-memory state of active carts, evaluating session timeouts (rolling windows).
4. **Revenue Recovery:** Carts abandoned beyond the TTL threshold automatically trigger a mock Marketing API for discount code dispatch.
5. **Observability:** **Streamlit** consumes the Kafka topic in real-time, providing an executive dashboard of leaked vs. converted revenue.

## ⚙️ Tech Stack
* **Message Broker:** Apache Kafka, Zookeeper
* **Streaming Logic:** Python, `confluent-kafka`
* **Infrastructure:** Docker, Docker Compose
* **Observability:** Streamlit, Pandas

## ⚡ Quick Start
```bash
# 1. Start the distributed Kafka/Zookeeper cluster in the background
docker compose up -d

# 2. Start generating clickstream telemetry
python src/producer.py

# 3. Launch the Real-Time Executive Radar (in a new terminal)
streamlit run src/dashboard.py
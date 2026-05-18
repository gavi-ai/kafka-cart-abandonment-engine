# Kafka Cart Abandonment Engine

Distributed event-streaming pipeline that detects e-commerce cart 
abandonment in real time and triggers automated recovery. Built on 
Apache Kafka + Zookeeper, fully containerised with Docker Compose.

---

## What this solves

E-commerce platforms lose significant revenue when users add items to 
cart and disappear. Batch jobs catch this hours later. This pipeline 
detects abandonment within a configurable TTL window and fires a 
recovery action immediately — while the user is still likely online.

---

## How it works
[Producer]                        [Kafka Topic]              [Consumer]
Faker generates              →    confluent-kafka       →    stateful session engine
clickstream events                async producer             rolling-window per user
view_item                         high-throughput            tracks: view → cart → checkout
add_to_cart                       partitioned topic
checkout                                                      if cart TTL expires
without checkout:
→ mock Marketing API called
→ discount code dispatched
                                                     [Streamlit Dashboard]
                                                     Live: leaked vs converted revenue
                                                     drop-off patterns, session counts

---

## Tech stack

| Component | Tool |
|---|---|
| Message broker | Apache Kafka + Zookeeper |
| Producer | Python, confluent-kafka (async) |
| Consumer | Python, stateful in-memory session store |
| Infrastructure | Docker, Docker Compose |
| Observability | Streamlit, Pandas |
| CI | GitHub Actions + flake8 |

---

## Quick start

```bash
git clone https://github.com/gavi-ai/kafka-cart-abandonment-engine.git
cd kafka-cart-abandonment-engine

# Start Kafka + Zookeeper cluster
docker compose up -d

# Generate clickstream events
python src/producer.py

# Start consumer (new terminal)
python src/consumer.py

# Launch live dashboard (new terminal)
streamlit run src/dashboard.py
```

Dashboard available at `http://localhost:8501`

---

## Repository structure
kafka-cart-abandonment-engine/
├── src/
│   ├── producer.py       # Async confluent-kafka clickstream generator
│   ├── consumer.py       # Stateful session engine + abandonment logic
│   └── dashboard.py      # Streamlit real-time revenue dashboard
├── docker-compose.yml    # Kafka + Zookeeper cluster definition
├── requirements.txt
├── .github/
│   └── workflows/
│       └── ci.yml        # flake8 lint on every push
└── README.md

---

## Session logic

Each user session is tracked in memory with a rolling TTL window:

- Events arrive: `view_item` → `add_to_cart` → `checkout`
- If `checkout` is not received within the TTL threshold after 
  `add_to_cart`, the cart is marked abandoned
- Consumer calls mock Marketing API with user ID + cart value
- Event is published to the dashboard topic for live visibility

TTL threshold is configurable in `consumer.py`.

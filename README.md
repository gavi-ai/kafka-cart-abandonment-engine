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

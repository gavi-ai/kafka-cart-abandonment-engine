from confluent_kafka import Producer
import json
import time
import random
from faker import Faker

fake = Faker()

# ⚙️ Kafka Configuration
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
topic = 'ecommerce_events'

def delivery_report(err, msg):
    """Callback for delivery confirmation (Senior Engineer Move)"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Event safely parked in {msg.topic()} partition [{msg.partition()}]")

print("🚀 [STARTING] E-Commerce Clickstream Simulator...")
print("=====================================================")

# Realistic Event Distribution: Lots of views, fewer carts, very few checkouts
event_types = ['view_item', 'add_to_cart', 'checkout']
weights = [0.70, 0.20, 0.10] 

try:
    while True:
        # 1. Generate fake user activity
        event_data = {
            "user_id": fake.uuid4()[:8],
            "event_type": random.choices(event_types, weights=weights)[0],
            "item_id": f"ITEM_{random.randint(100, 999)}",
            "price": round(random.uniform(500.0, 15000.0), 2),
            "timestamp": int(time.time())
        }

        # 2. Push to Kafka Topic
        # We use user_id as the 'key' so events from the same user go to the same partition
        producer.produce(
            topic, 
            key=event_data["user_id"], 
            value=json.dumps(event_data), 
            callback=delivery_report
        )
        
        # 3. Serve the callback queue
        producer.poll(0) 

        # Fire 2 events every second
        time.sleep(0.5) 

except KeyboardInterrupt:
    print("\n🛑 [STOPPING] Shutting down simulator...")
finally:
    # 4. Flush remaining messages before exiting
    print("🧹 Flushing pending events...")
    producer.flush()
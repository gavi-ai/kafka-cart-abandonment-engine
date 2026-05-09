from confluent_kafka import Consumer
import json
import time

# ⚙️ Kafka Consumer Configuration (The Watcher)
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'cart_abandonment_engine',
    'auto.offset.reset': 'latest'  # Only read NEW events, ignore the past
}

consumer = Consumer(conf)
consumer.subscribe(['ecommerce_events'])

print("🕵️‍♂️ [LISTENING] Cart Abandonment Engine Started...")
print("====================================================")

# In-Memory State Store (Tracking Users in Real-Time)
active_carts = {}
ABANDONMENT_TIMEOUT = 10  # Seconds (Set low for demo, usually 15 mins in prod)

try:
    while True:
        # 1. Poll for new events every 1 second
        msg = consumer.poll(1.0)

        # 2. 🕰️ Time-Check Logic (Have any carts expired?)
        current_time = int(time.time())
        abandoned_users = []
        
        for user_id, cart_data in list(active_carts.items()):
            if current_time - cart_data['timestamp'] > ABANDONMENT_TIMEOUT:
                print(f"🚨 [REVENUE LEAK DETECTED] User {user_id} left {cart_data['item']} in cart! Triggering 10% Discount Email...")
                abandoned_users.append(user_id)
        
        # Clean up abandoned carts so we don't spam them
        for user_id in abandoned_users:
            del active_carts[user_id]

        # 3. Handle the incoming message
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        # 4. Parse the Event
        event = json.loads(msg.value().decode('utf-8'))
        user_id = event['user_id']
        event_type = event['event_type']

        # 5. The Core Business Logic
        if event_type == 'add_to_cart':
            print(f"🛒 [CART ADD] User {user_id} is thinking about buying {event['item_id']}.")
            # Add to watchlist
            active_carts[user_id] = {'item': event['item_id'], 'timestamp': event['timestamp']}
            
        elif event_type == 'checkout':
            if user_id in active_carts:
                print(f"💰 [KA-CHING!] User {user_id} checked out {active_carts[user_id]['item']}! $$$")
                # Remove from watchlist, transaction complete
                del active_carts[user_id]

except KeyboardInterrupt:
    print("\n🛑 [STOPPING] Shutting down consumer...")
finally:
    consumer.close()
import streamlit as st
from confluent_kafka import Consumer
import json
import time
import pandas as pd

# 🎨 UI Configuration
st.set_page_config(page_title="Cart Abandonment Radar", layout='wide')
st.title("🛒 Live Cart Abandonment Radar")
st.markdown("Monitoring high-throughput Kafka `ecommerce_events` stream for real-time revenue leaks.")

# 💾 Initialize Session State for rolling metrics
if 'total_revenue_lost' not in st.session_state:
    st.session_state.total_revenue_lost = 0.0
if 'total_checkouts' not in st.session_state:
    st.session_state.total_checkouts = 0.0

@st.cache_resource
def get_consumer():
    """Connect Streamlit directly to the Kafka Cluster"""
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'streamlit_executive_dashboard',
        'auto.offset.reset': 'latest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['ecommerce_events'])
    return consumer

consumer = get_consumer()

# 📊 Dashboard Layout (Top Metrics)
col1, col2, col3 = st.columns(3)
metric_lost = col1.empty()
metric_saved = col2.empty()
metric_active = col3.empty()

st.divider()
st.subheader("🚨 Live Revenue Leaks (Actionable Alerts)")
alert_placeholder = st.empty()

# ⚙️ Business Logic State
active_carts = {}
ABANDONMENT_TIMEOUT = 10  # Seconds
leak_log = []

# 🚀 The Real-Time Engine Loop
while True:
    msg = consumer.poll(0.1) # Fast polling
    current_time = int(time.time())
    
    # 1. Check for abandoned carts (Revenue Leaks)
    abandoned_users = []
    for uid, cdata in list(active_carts.items()):
        if current_time - cdata['timestamp'] > ABANDONMENT_TIMEOUT:
            st.session_state.total_revenue_lost += cdata['price']
            leak_log.insert(0, {
                "User ID": uid, 
                "Item": cdata['item'], 
                "Lost Revenue": f"₹ {cdata['price']:,.2f}", 
                "Status": "Marketing API Triggered 🚨"
            })
            abandoned_users.append(uid)
            
    # Clean up state
    for uid in abandoned_users:
        del active_carts[uid]
        
    # Keep the UI table clean (Show last 10 leaks only)
    if len(leak_log) > 10:
        leak_log = leak_log[:10]

    # 2. Process incoming Kafka messages
    if msg is not None and not msg.error():
        event = json.loads(msg.value().decode('utf-8'))
        uid = event['user_id']
        etype = event['event_type']
        price = event.get('price', 0)
        
        if etype == 'add_to_cart':
            active_carts[uid] = {'item': event['item_id'], 'price': price, 'timestamp': event['timestamp']}
        elif etype == 'checkout':
            if uid in active_carts:
                st.session_state.total_checkouts += active_carts[uid]['price']
                del active_carts[uid] # Transaction successful!
                
    # 3. Update the UI live
    metric_lost.metric(label="Total Revenue Leaked", value=f"₹ {st.session_state.total_revenue_lost:,.2f}", delta="- Loss", delta_color="inverse")
    metric_saved.metric(label="Total Revenue Converted", value=f"₹ {st.session_state.total_checkouts:,.2f}", delta="✅ Converted")
    metric_active.metric(label="Live Active Carts", value=len(active_carts))
    
    if leak_log:
        alert_placeholder.dataframe(pd.DataFrame(leak_log))
        
    time.sleep(0.3) # Smooth UI refresh rate
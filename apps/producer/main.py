import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker and Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC = 'market_transactions'
EVENT_VERSION = '1.0.0'

def generate_transaction():
    """Generates a random transaction event."""
    return {
        "event_version": EVENT_VERSION,
        "transaction_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "product_id": f"SKU-{fake.random_int(min=1000, max=9999)}",
        "category": random.choice(['Electronics', 'Fashion', 'Home', 'Books', 'Health']),
        "price": round(random.uniform(10.0, 1500.0), 2),
        "user_id": fake.user_name(),
        "location": fake.state_abbr()
    }

if __name__ == "__main__":
    print(f"🚀 Starting Producer: Sending data to topic '{TOPIC}'...")
    try:
        while True:
            # Create the event
            data = generate_transaction()
            
            # Send to Kafka
            producer.send(TOPIC, value=data)
            
            print(f"✅ Sent: {data['transaction_id']} | {data['category']} | ${data['price']}")
            
            # Control the frequency (simulate 2 transactions per second)
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")
    finally:
        producer.close()

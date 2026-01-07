
from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

users = ["Amit", "Riya", "Gaurav", "Anjali", "Rahul"]
pages = ["Home", "Products", "Cart", "Checkout", "Profile"]

print("Streaming user click data... Press Ctrl+C to stop\n")

try:
    while True:
        event = {
            "user": random.choice(users),
            "page": random.choice(pages),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send("user_clicks", event)
        print("Sent:", event)
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping click producer...")

finally:
    producer.flush()
    producer.close()
    print("Producer closed.")
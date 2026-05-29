from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x:
    json.dumps(x).encode("utf-8")
)

products = [
    "Laptop",
    "Mouse",
    "Keyboard",
    "Monitor"
]

while True:

    event = {

        "order_id":
        random.randint(1000,9999),

        "product":
        random.choice(products),

        "quantity":
        random.randint(1,5),

        "price":
        random.randint(500,70000)
    }

    producer.send(
        "sales_topic",
        value=event
    )

    print(event)

    time.sleep(2)
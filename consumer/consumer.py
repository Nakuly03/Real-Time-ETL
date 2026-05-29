from kafka import KafkaConsumer
import mysql.connector
import json
from datetime import datetime
import logging

# Logging Configuration

logging.basicConfig(
    filename="logs\pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka Consumer

consumer = KafkaConsumer(
    "sales_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# MySQL Connection

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="#Nakul007",   # Replace with your password
    database="realtime_etl"
)

cursor = conn.cursor()

print("Consumer Started...")

for message in consumer:

    try:

        data = message.value

        # Data Validation

        if data["quantity"] <= 0:
            logging.warning(
                f"Invalid quantity: {data}"
            )
            continue

        if data["price"] <= 0:
            logging.warning(
                f"Invalid price: {data}"
            )
            continue

        # Transformation

        total_amount = (
            data["quantity"] *
            data["price"]
        )

        event_time = datetime.now()

        # Insert Query

        query = """
        INSERT INTO sales_stream
        (
            order_id,
            product,
            quantity,
            price,
            total_amount,
            event_time
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        """

        values = (
            data["order_id"],
            data["product"],
            data["quantity"],
            data["price"],
            total_amount,
            event_time
        )

        cursor.execute(query, values)

        conn.commit()

        print(f"Loaded -> {data}")

        logging.info(
            f"Loaded Order ID: {data['order_id']}"
        )

    except mysql.connector.Error as err:

        print(f"MySQL Error: {err}")

        logging.error(
            f"MySQL Error: {err}"
        )

    except Exception as e:

        print(f"Error: {e}")

        logging.error(
            f"General Error: {e}"
        )
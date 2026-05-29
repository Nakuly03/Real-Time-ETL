from kafka import KafkaConsumer
import mysql.connector
import json
import logging
import os

from datetime import datetime
from dotenv import load_dotenv


load_dotenv()


logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

logging.info("Consumer Started")


consumer = KafkaConsumer(
    "sales_topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)

cursor = conn.cursor()

print("Consumer Started...")

# Consume Messages

for message in consumer:

    try:

        data = message.value

        # Data Validation

        if data["quantity"] <= 0:

            logging.warning(
                f"Invalid Quantity: {data}"
            )

            continue

        if data["price"] <= 0:

            logging.warning(
                f"Invalid Price: {data}"
            )

            continue

        # Transformation

        total_amount = (
            data["quantity"]
            *
            data["price"]
        )

        event_time = datetime.now()

        # SQL Query

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
        VALUES
        (
            %s,%s,%s,%s,%s,%s
        )
        """

        values = (
            data["order_id"],
            data["product"],
            data["quantity"],
            data["price"],
            total_amount,
            event_time
        )

        cursor.execute(
            query,
            values
        )

        conn.commit()

        print(
            f"Loaded -> {data}"
        )

        logging.info(
            f"Loaded Order ID: {data['order_id']}"
        )

    except mysql.connector.Error as err:

        print(
            f"MySQL Error: {err}"
        )

        logging.error(
            f"MySQL Error: {err}"
        )

    except Exception as e:

        print(
            f"Error: {e}"
        )

        logging.error(
            f"General Error: {e}"
        )
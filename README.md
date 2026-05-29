# Real-Time ETL Pipeline using Kafka and MySQL

## Overview

This project demonstrates a real-time ETL (Extract, Transform, Load) pipeline built using Apache Kafka, Python, Docker, and MySQL.

The pipeline continuously generates sales transactions, streams them through Kafka, validates and transforms incoming events, and loads the processed data into MySQL for analytical reporting.

---

## Architecture

```text
Producer.py
     ↓
Kafka Topic (sales_topic)
     ↓
Consumer.py
     ↓
Data Validation
     ↓
Data Transformation
     ↓
MySQL Database
```

---

## Tech Stack

- Python
- Apache Kafka
- Docker
- MySQL
- SQL
- kafka-python
- mysql-connector-python
- python-dotenv

---

## Features

- Real-time event streaming using Kafka
- Data validation checks
- Data transformation logic
- Automated loading into MySQL
- Logging and error handling
- Environment variable management using .env
- SQL analytics reporting
- Dockerized Kafka infrastructure

---

## Project Structure

```text
real_time_etl/

│
├── consumer/
│   └── consumer.py
│
├── producer/
│   └── producer.py
│
├── logs/
│   └── pipeline.log
│
├── sql/
│   └── analytics.sql
│
├── .env
├── .gitignore
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Data Flow

### Extract

Sales events are generated in real time using the Kafka producer.

Example:

```json
{
  "order_id": 1001,
  "product": "Laptop",
  "quantity": 2,
  "price": 65000
}
```

### Transform

The consumer performs:

- Quantity validation
- Price validation
- Total amount calculation
- Event timestamp generation

Example:

```text
total_amount = quantity × price
```

### Load

Validated records are inserted into MySQL.

---

## Database Schema

```sql
CREATE TABLE sales_stream (

    order_id INT PRIMARY KEY,

    product VARCHAR(100),

    quantity INT,

    price DOUBLE,

    total_amount DOUBLE,

    event_time DATETIME
);
```

---

## Setup Instructions

### Clone Repository

```bash
git clone https://github.com/your-username/Real-Time-ETL-Pipeline.git

cd Real-Time-ETL-Pipeline
```

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Configure Environment Variables

Create a `.env` file:

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=realtime_etl
```

### Start Kafka

```bash
docker compose up -d
```

### Run Producer

```bash
python producer/producer.py
```

### Run Consumer

```bash
python consumer/consumer.py
```

---

## Analytics Queries

### Total Revenue

```sql
SELECT SUM(total_amount) AS total_revenue
FROM sales_stream;
```

### Total Orders

```sql
SELECT COUNT(*) AS total_orders
FROM sales_stream;
```

### Revenue by Product

```sql
SELECT
    product,
    SUM(total_amount) AS revenue
FROM sales_stream
GROUP BY product
ORDER BY revenue DESC;
```

### Top Selling Product

```sql
SELECT
    product,
    SUM(quantity) AS units_sold
FROM sales_stream
GROUP BY product
ORDER BY units_sold DESC;
```

---

## Sample Consumer Output

```text
Loaded -> {
    'order_id': 4521,
    'product': 'Laptop',
    'quantity': 2,
    'price': 65000
}
```

---

## Learning Outcomes

- Real-time data streaming with Kafka
- ETL pipeline development
- Data validation and transformation
- MySQL integration
- Docker containerization
- SQL analytics
- Logging and error handling
- Secure credential management using environment variables

---

## Future Enhancements

- Apache Airflow Scheduling
- AWS S3 Integration
- Snowflake Data Warehouse Integration
- Real-Time Monitoring Dashboard
- Data Quality Framework
- CI/CD Pipeline

---

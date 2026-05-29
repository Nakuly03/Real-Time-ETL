# Real-Time ETL Pipeline using Kafka and MySQL

## Project Overview

This project demonstrates a real-time ETL (Extract, Transform, Load) pipeline built using Apache Kafka, Python, Docker, and MySQL.

The pipeline continuously generates sales events, streams them through Kafka, validates and transforms the data, and loads the processed records into a MySQL database for analytical reporting.

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

## Tech Stack

- Python
- Apache Kafka
- Docker
- MySQL
- SQL
- kafka-python
- mysql-connector-python

## Features

- Real-time data ingestion using Kafka
- Continuous event processing
- Data validation checks
- Data transformation logic
- Automated loading into MySQL
- Error handling and logging
- Analytical SQL reporting
- Containerized Kafka infrastructure using Docker

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
├── docker-compose.yml
│
├── requirements.txt
│
└── README.md
```

## Data Flow

### Extract

Sales events are generated in real time using the Kafka producer.

Example event:

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
- Timestamp generation

Example:

```text
total_amount = quantity × price
```

### Load

Validated records are loaded into MySQL.

## MySQL Schema

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

## Setup Instructions

### Clone Repository

```bash
git clone <repository-url>
cd real_time_etl
```

### Install Dependencies

```bash
pip install -r requirements.txt
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

## Analytics Queries

### Total Revenue

```sql
SELECT
    SUM(total_amount)
FROM sales_stream;
```

### Total Orders

```sql
SELECT
    COUNT(*)
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

## Sample Output

```text
Loaded -> {
    'order_id': 4321,
    'product': 'Laptop',
    'quantity': 2,
    'price': 65000
}
```

## Learning Outcomes

- Real-time data streaming with Kafka
- ETL pipeline development
- Data validation and transformation
- MySQL integration
- Docker-based deployment
- SQL analytics and reporting
- Logging and error handling

## Future Enhancements

- Apache Airflow scheduling
- AWS S3 integration
- Snowflake integration
- Real-time monitoring dashboard
- Data quality framework
- CI/CD pipeline implementation

## Author

Nakul Yadav
# Stream Enrichment Lab Environment Setup

## MySQL

Create a MySQL database server or cluster, or launch managed one (e.g., AWS RDS). Connect to your database with a MySQL 
client:

    mysql --database <datababse> -u <user> -p
Enter your password. Then use the following SQL script to create the table `customers` and populate data into it:

    CREATE TABLE IF NOT EXISTS customers (
        id INT NOT NULL,
        name VARCHAR(255),
        country VARCHAR(255),
        zipcode VARCHAR(255),
        status VARCHAR(255),
        updatedAt VARCHAR(255),
        PRIMARY KEY (id)
    );
    
    INSERT INTO customers VALUES (1, "Flink", "Germany", "10115", "free", "2024-10-01 10:01:00.000");
    
    UPDATE customers SET status="basic",      updatedAt="2024-10-01 10:04:00.000" WHERE id=1;
    UPDATE customers SET status="business",   updatedAt="2024-10-01 10:06:00.000" WHERE id=1;
    UPDATE customers SET status="enterprise", updatedAt="2024-10-01 10:08:00.000" WHERE id=1;

Verify your data with:

    SELECT * FROM customers;

## Kafka

Create a Kafka cluster or launch a managed one (e.g., AWS MSK). Use a Kafka client 
(e.g., download [Apache Kafka](https://kafka.apache.org/downloads), or
use [kafka-ui](https://github.com/provectus/kafka-ui/blob/master/README.md))
to connect to your Kafka cluster and
create the Kafka topic `orders`. Then ingest the following messages into the created topic:

    {
        "id": 2,
        "customerId": 1,
        "amount": 12.0,
        "timestamp": "2024-10-01 10:02:00.000"
    }
    {
        "id": 3,
        "customerId": 1,
        "amount": 13.0,
        "timestamp": "2024-10-01 10:03:00.000"
    }
    {
        "id": 5,
        "customerId": 1,
        "amount": 15.0,
        "timestamp": "2024-10-01 10:05:00.000"
    }
    {
        "id": 7,
        "customerId": 1,
        "amount": 17.0,
        "timestamp": "2024-10-01 10:07:00.000"
    }
    {
        "id": 9,
        "customerId": 1,
        "amount": 19.0,
        "timestamp": "2024-10-01 10:09:00.000"
    }
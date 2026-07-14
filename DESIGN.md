# Big Data Analytics Pipeline - Design Document

## Project Overview

This project implements an end-to-end Big Data Analytics Pipeline using Apache Spark, Hadoop HDFS, Apache Hive, and Apache Superset.

The objective is to transform raw e-commerce data into an analytical data warehouse and provide business insights through interactive dashboards.

The first two phases are complete, and Phase 3 is now being implemented:

- **Phase 1:** Data ingestion, transformation, storage, and visualization.
- **Phase 2:** Data warehouse design, ETL documentation, star schema modeling, and business analytics.
- **Phase 3:** Airflow orchestration, dbt transformation scaffolding, and a Medallion Architecture migration.

---

# System Architecture

```
          Olist CSV Dataset
                 │
                 ▼
           Apache Spark
      (Extract & Transform)
                 │
                 ▼
        Apache Parquet Files
                 │
                 ▼
           Hadoop HDFS
      (Distributed Storage)
                 │
                 ▼
            Apache Hive
      (Metadata & SQL Layer)
                 │
                 ▼
         Apache Superset
    (Business Intelligence)
```

---

# Architecture Layers

The project follows a layered architecture.

## 1. Data Source Layer

- Olist Brazilian E-Commerce Dataset
- CSV files

---

## 2. Processing Layer

Apache Spark performs:

- Reading CSV files
- Schema inference
- Data cleaning
- Data transformation
- Parquet conversion

---

## 3. Storage Layer

Hadoop HDFS stores all processed datasets in distributed storage.

Phase 1 writes curated Parquet datasets to the configured HDFS Parquet URI, and Phase 2 writes the warehouse fact and dimension tables to the configured HDFS warehouse URI. Raw CSV inputs remain mounted locally inside the container filesystem.

Benefits include:

- Fault tolerance
- Scalability
- High availability

---

## 4. Query Layer

Apache Hive provides SQL access through external tables built on Parquet files.

No data duplication is required because Hive references the existing files.

---

## 5. Visualization Layer

Apache Superset connects directly to Hive and provides:

- KPI Cards
- Business Charts
- Interactive Dashboards

---

# ETL Design

The project follows a traditional **ETL (Extract, Transform, Load)** workflow.

## Extract

Raw CSV files are loaded into Apache Spark DataFrames.

## Transform

Data is processed by:

- Cleaning records
- Selecting required columns
- Joining datasets
- Building analytical tables
- Converting CSV to Parquet

## Load

The transformed datasets are:

- Stored in Hadoop HDFS
- Registered as Hive external tables
- Queried from Apache Superset

---

# ETL vs ELT

This project uses the ETL approach because all transformations are completed before loading the data into the analytical environment.

Compared to ELT, ETL reduces dashboard query complexity and improves analytical performance.

The project documentation also discusses modern ELT architectures and dbt as part of the active Phase 3 transformation plan. Apache Airflow is now being implemented as the orchestration layer for Phase 3.

## Phase 3 Foundation

Phase 3 is now being implemented around a Medallion Architecture. Airflow and dbt are part of the active Phase 3 buildout, while the existing Phase 2 warehouse scripts remain available during the migration so the current warehouse can keep running unchanged.

---

# Airflow Concepts

- **Webserver:** The Airflow webserver serves the UI and API for browsing DAGs, monitoring runs, and managing the orchestration environment.
- **Scheduler:** The scheduler parses DAG files and queues task instances when their schedules and dependencies are ready.
- **Metadata Database:** The metadata database stores DAG definitions, task state, run history, connections, variables, and other Airflow metadata.
- **Executor:** The executor decides how task instances are executed; `LocalExecutor` runs tasks in parallel on the local Airflow host.
- **Triggerer:** The triggerer manages deferred tasks and asynchronous triggers without holding worker slots open.
- **DAG:** A DAG is the directed acyclic graph that defines workflow order and dependencies.
- **Task:** A task is one unit of work inside a DAG, such as a Python callable or an operator-based action.
- **Operator:** An operator is a reusable template that defines how a task performs its work.
- **XCom:** XCom is Airflow's lightweight cross-communication mechanism for passing small values between tasks.

---

# Data Warehouse Design

A Star Schema was implemented for analytical reporting.

## Fact Table

**fact_orders**

Order-item fact table with one row per `order_id` + `order_item_id`.

Measures:

- item_gross_value
- order_items_gross_total
- order_payment_value
- allocated_payment_value
- price
- freight_value
- review_score
- primary_payment_installments

Keys:

- order_id
- order_item_id
- customer_id
- seller_id
- product_id
- date_key

Primary grouping column:

- primary_payment_type

Order-level payments are aggregated before joining to order items, and `allocated_payment_value` is allocated proportionally from each item's gross value so revenue is not duplicated across payment rows.

---

## Dimension Tables

### dim_customers

Customer information.

### dim_products

Product information.

### dim_sellers

Seller information.

### dim_date

Date dimension used for time-based analysis.

---

# Business Questions

The warehouse was designed to answer common analytical questions such as:

- What is the total allocated sales revenue?
- How many distinct orders exist?
- How many unique customers are there?
- Which primary payment methods are most popular?
- Which order statuses occur most frequently?
- What is the average customer review score?

---

# Dashboard Design

Apache Superset was used to create an interactive dashboard.

## KPI Cards

- Total Sales (`sum(allocated_payment_value)`)
- Total Orders (`count(distinct order_id)`)
- Total Customers
- Average Review Score (`avg(review_score)`)

## Visualizations

- Payment Type Distribution by `primary_payment_type`
- Order Status Distribution
- Revenue by Order Status using `allocated_payment_value`
- Review Score Distribution

These visualizations provide a concise overview of marketplace performance and support business decision-making.

---

# Design Decisions

The following design choices were made during implementation:

- Apache Spark for distributed ETL processing
- Apache Parquet for optimized storage
- Hadoop HDFS for distributed file storage
- Hive External Tables to avoid data duplication
- Star Schema for analytical queries
- Apache Superset for dashboard visualization

These decisions improve scalability, maintainability, and query performance.

---

# Future Improvements

Possible future enhancements include:

- Incremental data loading
- Additional dimension tables
- Machine Learning integration
- Customer segmentation
- Sales forecasting
- Real-time streaming with Apache Kafka
- Cloud deployment on AWS or Azure

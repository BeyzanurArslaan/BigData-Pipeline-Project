# Big Data Analytics Pipeline - Design Document

## Project Overview

This project implements an end-to-end Big Data Analytics Pipeline using Apache Spark, Hadoop HDFS, Apache Hive, and Apache Superset.

The objective is to transform raw e-commerce data into an analytical data warehouse and provide business insights through interactive dashboards.

The project was completed in two phases:

- **Phase 1:** Data ingestion, transformation, storage, and visualization.
- **Phase 2:** Data warehouse design, ETL documentation, star schema modeling, and business analytics.

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

The project documentation also discusses modern ELT architectures and dbt as alternative transformation approaches.

---

# Data Warehouse Design

A Star Schema was implemented for analytical reporting.

## Fact Table

**fact_orders**

Measures:

- payment_value
- price
- freight_value
- review_score
- payment_installments

Keys:

- customer_id
- seller_id
- product_id
- date_key

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

- What is the total sales revenue?
- How many orders exist?
- How many unique customers are there?
- Which payment methods are most popular?
- Which order statuses occur most frequently?
- What is the average customer review score?

---

# Dashboard Design

Apache Superset was used to create an interactive dashboard.

## KPI Cards

- Total Sales
- Total Orders
- Total Customers
- Average Review Score

## Visualizations

- Payment Type Distribution
- Order Status Distribution
- Revenue by Order Status
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

- Automated ETL scheduling using Apache Airflow
- Incremental data loading
- Additional dimension tables
- Machine Learning integration
- Customer segmentation
- Sales forecasting
- Real-time streaming with Apache Kafka
- Cloud deployment on AWS or Azure
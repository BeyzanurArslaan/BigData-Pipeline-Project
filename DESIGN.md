# Phase 2 Design Document

## Objective

The goal of Phase 2 is to transform the raw Olist e-commerce dataset into a business-oriented data warehouse using Apache Spark.

Instead of analyzing raw transactional tables directly, the project will build a Star Schema consisting of one fact table and multiple dimension tables. The resulting warehouse will support business intelligence and analytical dashboards in Apache Superset.

---

# ETL Pipeline

Raw CSV Files
        │
        ▼
Apache Spark
        │
        ▼
Data Validation
        │
        ▼
Data Cleaning
        │
        ▼
Business Transformations
        │
        ▼
Star Schema
        │
        ▼
Hive Tables
        │
        ▼
Apache Superset

---

# Business Goals

The warehouse should answer questions such as:

- Monthly revenue
- Revenue by product category
- Top performing sellers
- Sales by customer state
- Average delivery time
- Payment trends
- Review score by category

---

# Star Schema

Fact Table

- fact_sales

Dimension Tables

- dim_customer
- dim_product
- dim_seller
- dim_payment
- dim_date
- dim_order

---

# Expected Benefits

- Faster analytical queries
- Cleaner data model
- Easier dashboard development
- Better scalability
- Industry-standard warehouse design
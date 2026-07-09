# 🚀 Big Data Analytics Pipeline with Apache Spark, Hadoop HDFS, Apache Hive & Apache Superset

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellow)
![Apache Hive](https://img.shields.io/badge/Apache-Hive-orange)
![Apache Superset](https://img.shields.io/badge/Apache-Superset-20A6C9)
![Docker](https://img.shields.io/badge/Docker-2496ED)

An end-to-end Big Data Analytics Pipeline built using **Apache Spark**, **Hadoop HDFS**, **Apache Hive**, and **Apache Superset** to process, store, query, and visualize the **Olist Brazilian E-Commerce Dataset**.

The project demonstrates a complete modern data engineering workflow, from raw CSV ingestion to interactive business intelligence dashboards.

---

# 📊 Dashboard

> Interactive dashboard built with Apache Superset.

![Dashboard](screenshots/dashboard.png)

---

# 📌 Project Overview

This project analyzes approximately **100,000 real Brazilian e-commerce orders** using distributed big data technologies.

The project was completed in **two phases**.

## Phase 1

- Read raw CSV datasets using Apache Spark
- Transform CSV files into Parquet format
- Store processed datasets in Hadoop HDFS
- Create Hive external tables
- Connect Apache Superset
- Build an interactive business dashboard

## Phase 2

The second phase extends the pipeline by introducing a complete analytical warehouse design.

Additional work includes:

- Data Quality Assessment
- ETL Architecture
- ETL vs ELT comparison
- Star Schema Design
- Fact & Dimension Modeling
- Business Questions Mapping
- Business Analytics Dashboard
- Documentation and technical report

---

# 🏗️ System Architecture

```
          Olist CSV Dataset
                 │
                 ▼
           Apache Spark
      (CSV → Parquet ETL)
                 │
                 ▼
            Hadoop HDFS
       (Distributed Storage)
                 │
                 ▼
            Apache Hive
        (External Tables)
                 │
                 ▼
         Apache Superset
      Business Intelligence
```

---

# 🛠️ Technologies

- Apache Spark
- Hadoop HDFS
- Apache Hive
- Apache Superset
- Docker
- Python
- SQL
- Apache Parquet

---

# 📂 Dataset

**Brazilian E-Commerce Public Dataset by Olist**

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Dataset includes approximately:

- 99,441 Orders
- 99,000+ Customers
- Products
- Sellers
- Payments
- Reviews
- Geolocation

---

# ⚙️ Data Processing Pipeline

## Step 1 — Data Ingestion

The original Olist dataset consists of multiple CSV files.

Apache Spark loads every CSV dataset and automatically infers the schema.

---

## Step 2 — Data Transformation

Spark transforms the datasets into Apache Parquet format.

Advantages include:

- Column-oriented storage
- Faster analytical queries
- Better compression
- Lower storage usage

---

## Step 3 — Distributed Storage

Processed Parquet datasets are stored in Hadoop HDFS.

---

## Step 4 — Query Layer

Hive external tables are created on top of Parquet files, enabling SQL queries without duplicating data.

---

## Step 5 — Business Intelligence

Apache Superset connects directly to Hive and generates interactive business dashboards.

---

# 🏛️ Data Warehouse Design (Phase 2)

The analytical warehouse follows a **Star Schema** architecture.

### Fact Table

- fact_orders

### Dimension Tables

- dim_customers
- dim_products
- dim_sellers
- dim_date

The warehouse separates business measures from descriptive dimensions, making analytical queries simpler and significantly faster.

---

# 🔄 ETL Architecture

The project follows a traditional **ETL (Extract, Transform, Load)** workflow.

**Extract**

- Read raw CSV files using Apache Spark

**Transform**

- Clean datasets
- Join multiple tables
- Convert to Parquet
- Build analytical datasets

**Load**

- Store Parquet files in Hadoop HDFS
- Create Hive external tables
- Query data through Apache Superset

The project also discusses the differences between **ETL**, **ELT**, and **dbt** as part of Phase 2.

---

# 📈 Dashboard Metrics

The dashboard contains several KPI cards and business charts.

## KPIs

- Total Sales
- Total Orders
- Total Customers
- Average Review Score

## Charts

- Payment Type Distribution
- Order Status Distribution
- Revenue by Order Status
- Review Score Distribution

These visualizations provide a concise overview of marketplace performance and customer behavior.

---

# 💼 Business Questions Answered

The analytical warehouse supports business questions such as:

- What is the total sales revenue?
- How many orders have been placed?
- How many unique customers exist?
- Which payment methods are most frequently used?
- What is the distribution of order statuses?
- What is the average customer review score?
- Which order statuses generate the highest revenue?
- How can business performance be monitored using KPI dashboards?

---

# 📸 Screenshots

## Apache Spark

![Spark](screenshots/spark-master.png)

---

## Hadoop HDFS

![HDFS](screenshots/hdfs.png)

---

## Apache Superset Dashboard

![Dashboard](screenshots/dashboard.png)

---

# 📁 Project Structure

```text
BigData-Pipeline-Project/
│
├── data/
│   └── raw/
│
├── docker/
│
├── processing/
│
├── scripts/
│
├── visualization/
│
├── reports/
│   └── Big_Data_Analytics_Pipeline_Report.pdf
│
├── screenshots/
│   ├── dashboard.png
│   ├── spark-master.png
│   └── hdfs.png
│
├── DESIGN.md
├── README.md
└── .gitignore
```

---

# ▶️ Running the Project

Clone the repository:

```bash
git clone https://github.com/BeyzanurArslaan/BigData-Pipeline-Project.git
```

Start Hadoop:

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
```

Start Spark:

```bash
docker compose -f docker/docker-compose-spark.yml up -d
```

Start Apache Superset:

```bash
docker compose -f docker/docker-compose-superset.yml up -d
```

Open the services:

| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Apache Superset | http://localhost:8088 |

Default Superset credentials:

```text
Username: admin
Password: admin
```

---

# ✅ Project Outcomes

Successfully implemented:

- Apache Spark ETL Pipeline
- CSV → Parquet Transformation
- Hadoop HDFS Distributed Storage
- Apache Hive External Tables
- Star Schema Data Warehouse
- Fact & Dimension Modeling
- ETL Documentation
- Business Question Mapping
- Interactive Apache Superset Dashboard
- End-to-End Big Data Analytics Pipeline

---

# 📄 Documentation

The repository includes:

- Final Project Report (Phase 1 + Phase 2)
- DESIGN.md
- README.md
- Apache Superset Dashboard
- Spark ETL Scripts

---

# 👩‍💻 Author

**Beyzanur Arslan**

Software Engineering Student

- GitHub: https://github.com/BeyzanurArslaan
- LinkedIn: https://www.linkedin.com/in/beyzanur-arslan-ba18b832a
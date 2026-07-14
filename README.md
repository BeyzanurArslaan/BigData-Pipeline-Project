# 🚀 Big Data Analytics Pipeline with Apache Spark, Hadoop HDFS, Apache Hive, Apache Airflow, dbt & Apache Superset

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellow)
![Apache Hive](https://img.shields.io/badge/Apache-Hive-orange)
![Apache Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE)
![dbt](https://img.shields.io/badge/dbt-FF694B)
![Apache Superset](https://img.shields.io/badge/Apache-Superset-20A6C9)
![Docker](https://img.shields.io/badge/Docker-2496ED)

An end-to-end Big Data Analytics Pipeline built using **Apache Spark**, **Hadoop HDFS**, **Apache Hive**, **Apache Airflow**, **dbt**, and **Apache Superset**.

The project processes the Olist Brazilian E-Commerce dataset and demonstrates data ingestion, distributed storage, dimensional modeling, workflow orchestration, Medallion Architecture, testing, and business intelligence.

---

# 📌 Project Overview

The project analyzes approximately **100,000 Brazilian e-commerce orders**.

It is organized into three phases.

## Phase 1 — Data Ingestion and Processing

Phase 1 implements the initial big data pipeline:

- Read CSV files with Apache Spark
- Infer schemas
- Convert CSV datasets to Parquet
- Store Parquet datasets in HDFS
- Expose data through Hive
- Connect Apache Superset
- Build a business dashboard

Raw files:

```text
data/raw
```

Phase 1 HDFS output:

```text
hdfs://namenode:9000/olist/parquet
```

## Phase 2 — Data Warehouse and Analytics

Phase 2 introduces:

- Data-quality assessment
- ETL architecture
- Star Schema
- Fact and dimension tables
- Business-question mapping
- Revenue allocation
- Superset KPI cards and charts

Phase 2 warehouse output:

```text
hdfs://namenode:9000/olist/warehouse
```

## Phase 3 — Airflow, dbt, and Medallion Architecture

Phase 3 reconstructs the pipeline with:

- Apache Airflow orchestration
- PostgreSQL Airflow metadata database
- dbt Bronze, Silver, and Gold models
- Data-quality tests
- Gold Star Schema
- Automated validation
- Revenue reconciliation
- Superset metadata refresh
- Medallion Architecture documentation

The original Spark warehouse scripts remain available for comparison.

---

# 🏗️ Architecture

```text
Olist CSV
    │
    ▼
Apache Spark
    │
    ▼
HDFS Bronze Parquet
    │
    ▼
dbt Bronze
    │
    ▼
dbt Silver
    │
    ▼
dbt Gold Star Schema
    │
    ▼
Hive / Spark ThriftServer
    │
    ▼
Apache Superset
```

Apache Airflow orchestrates the complete workflow.

---

# 🥉🥈🥇 Medallion Architecture

## Bronze

Bronze models preserve source-aligned data.

Responsibilities:

- Minimal transformation
- Safe type casting
- Standardized column names
- Source traceability
- No business joins

## Silver

Silver models create cleaned business entities.

Responsibilities:

- Explicit data types
- Deduplication
- Payment aggregation
- Business-key validation
- Reusable transformations
- Data-quality tests

## Gold

Gold models provide reporting-ready facts and dimensions.

Models:

- `fact_orders`
- `dim_customers`
- `dim_products`
- `dim_sellers`
- `dim_date`

---

# 🔄 Airflow DAG

Main DAG:

```text
olist_medallion_pipeline
```

Pipeline sequence:

```text
Preflight Checks
        ↓
Validate Source Files
        ↓
Spark Ingestion
        ↓
Verify Bronze Output
        ↓
dbt deps
        ↓
dbt debug
        ↓
dbt Bronze Run and Test
        ↓
dbt Silver Run and Test
        ↓
dbt Gold Run and Test
        ↓
Reconcile Metrics
        ↓
Verify Hive Tables
        ↓
Refresh Superset Metadata
```

Airflow uses:

- LocalExecutor
- PostgreSQL metadata database
- Webserver
- Scheduler
- Triggerer
- Environment-variable configuration

---

# 🛠️ Technologies

- Apache Spark
- Hadoop HDFS
- Apache Hive
- Spark ThriftServer
- Apache Airflow
- dbt
- Apache Superset
- PostgreSQL
- Docker
- Python
- SQL
- Apache Parquet

---

# 📂 Dataset

The project uses the Brazilian E-Commerce Public Dataset by Olist.

Dataset source:

```text
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
```

Main datasets:

| Dataset | Description |
|---|---|
| Orders | Order lifecycle and timestamps |
| Order Items | Product, seller, price, and freight |
| Payments | Payment type, installments, and value |
| Reviews | Review scores and comments |
| Customers | Customer identity and location |
| Sellers | Seller identity and location |
| Products | Product category and dimensions |
| Category Translation | Portuguese-to-English categories |
| Geolocation | ZIP codes and coordinates |

---

# ⚙️ Data Pipeline

## Step 1 — Ingestion

Spark reads CSV files from:

```text
/app/data/raw
```

The local paths are converted with `Path.resolve().as_uri()`.

## Step 2 — Parquet Conversion

Spark converts CSV datasets to Parquet.

Benefits:

- Columnar storage
- Compression
- Faster queries
- Better analytical performance

## Step 3 — HDFS Storage

Parquet data is written to:

```text
hdfs://namenode:9000/olist/parquet
```

## Step 4 — dbt Transformation

dbt builds:

```text
Bronze → Silver → Gold
```

## Step 5 — SQL Access

Hive and Spark ThriftServer expose analytical models to external clients.

## Step 6 — Business Intelligence

Apache Superset queries the Gold warehouse models.

---

# 🏛️ Star Schema

## Fact Table

### `fact_orders`

Grain:

```text
one row per order_id + order_item_id
```

Important fields:

- `order_id`
- `order_item_id`
- `customer_id`
- `seller_id`
- `product_id`
- `date_key`
- `order_status`
- `price`
- `freight_value`
- `item_gross_value`
- `order_items_gross_total`
- `order_payment_value`
- `allocated_payment_value`
- `primary_payment_type`
- `primary_payment_installments`
- `review_score`

## Payment Allocation

```text
item_gross_value = price + freight_value
```

```text
allocated_payment_value =
    order_payment_value
    × item_gross_value
    ÷ order_items_gross_total
```

This prevents duplicated revenue when an order has multiple items or payment records.

## Dimensions

- `dim_customers`
- `dim_products`
- `dim_sellers`
- `dim_date`

---

# 📊 Dashboard

## KPIs

- Total Sales  
  `SUM(allocated_payment_value)`

- Total Orders  
  `COUNT(DISTINCT order_id)`

- Total Customers  
  `COUNT(DISTINCT customer_id)`

- Average Review Score  
  `AVG(review_score)`

## Charts

- Payment Type Distribution
- Order Status Distribution
- Revenue by Order Status
- Review Score Distribution

---

# 🧪 Data Quality

dbt tests include:

- Unique keys
- Not-null keys
- Relationship checks
- Accepted order statuses
- Review-score validation
- Positive numeric values
- Fact-grain validation
- Payment reconciliation

Custom tests are stored under:

```text
dbt/macros/generic_tests.sql
```

---

# 📁 Project Structure

```text
BigData-Pipeline-Project/
├── airflow/
│   ├── dags/
│   │   └── olist_medallion_pipeline.py
│   ├── config/
│   ├── logs/
│   └── plugins/
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── macros/
│   │   └── generic_tests.sql
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── seeds/
│   ├── snapshots/
│   └── tests/
├── docker/
│   ├── Dockerfile.airflow
│   ├── Dockerfile.superset
│   ├── docker-compose-airflow.yml
│   ├── docker-compose-hdfs.yml
│   ├── docker-compose-spark.yml
│   └── docker-compose-superset.yml
├── processing/
│   ├── analysis.py
│   ├── config.py
│   ├── logger.py
│   ├── spark_session.py
│   └── warehouse/
├── reports/
│   └── phase3/
│       └── runs/
├── tests/
│   └── test_olist_medallion_pipeline.py
├── .env.airflow.example
├── .gitignore
├── DESIGN.md
└── README.md
```

---

# ▶️ Running the Project

## 1. Clone

```bash
git clone https://github.com/BeyzanurArslaan/BigData-Pipeline-Project.git
cd BigData-Pipeline-Project
```

## 2. Start Docker Desktop

Verify:

```bash
docker info
```

## 3. Create the shared network

```bash
docker network create bigdata-net
```

If it already exists, Docker may return an error that can be ignored.

## 4. Start HDFS

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
```

## 5. Start Spark

```bash
docker compose -f docker/docker-compose-spark.yml up -d
```

## 6. Start Superset

```bash
docker compose -f docker/docker-compose-superset.yml up -d
```

## 7. Configure Airflow

```bash
cp .env.airflow.example .env.airflow
```

Update local demo credentials and secret values.

## 8. Start Airflow

```bash
docker compose \
  --env-file .env.airflow \
  -f docker/docker-compose-airflow.yml \
  up -d --build
```

---

# 🌐 Service URLs

| Service | URL |
|---|---|
| Hadoop NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Apache Superset | http://localhost:8088 |
| Apache Airflow | http://localhost:8085 |

---

# ▶️ Triggering the DAG

```bash
docker compose \
  --env-file .env.airflow \
  -f docker/docker-compose-airflow.yml \
  exec airflow-webserver \
  airflow dags trigger olist_medallion_pipeline
```

Test one DAG run:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker/docker-compose-airflow.yml \
  exec airflow-webserver \
  airflow dags test olist_medallion_pipeline 2024-01-01
```

---

# ▶️ dbt Commands

Run dbt from the Airflow container:

```bash
docker compose \
  --env-file .env.airflow \
  -f docker/docker-compose-airflow.yml \
  exec airflow-webserver \
  bash
```

Inside the container:

```bash
cd /opt/airflow/dbt
```

```bash
dbt deps
```

```bash
dbt debug
```

```bash
dbt run
```

```bash
dbt test
```

```bash
dbt docs generate
```

---

# 🧪 Local Tests

Compile Python files:

```bash
python3 -m compileall processing airflow
```

Run DAG unit tests:

```bash
python3 -m pytest tests/test_olist_medallion_pipeline.py
```

Current local result:

```text
2 passed, 4 skipped
```

The skipped tests require the full Airflow runtime dependencies.

Check formatting:

```bash
git diff --check
```

---

# ✅ Project Outcomes

Implemented:

- Spark CSV ingestion
- CSV-to-Parquet conversion
- HDFS distributed storage
- Hive SQL integration
- Spark ThriftServer
- Star Schema
- Order-item fact table
- Payment aggregation
- Proportional payment allocation
- Airflow orchestration
- dbt Bronze layer
- dbt Silver layer
- dbt Gold layer
- dbt tests
- Medallion Architecture
- Superset dashboard
- Reconciliation output
- DAG unit tests

---

# ⚠️ Validation Note

Source code compilation, Git validation, conflict-marker checks, and DAG unit tests were completed successfully.

Full end-to-end execution requires all Docker services and external dependencies to be running together.

---

# 👩‍💻 Author

**Beyzanur Arslan**

Software Engineering Student

- GitHub: https://github.com/BeyzanurArslaan
- LinkedIn: [Beyzanur Arslan](https://www.linkedin.com/in/beyzanur-arslan-ba18b832a/)
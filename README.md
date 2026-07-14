# 🚀 Big Data Analytics Pipeline with Apache Spark, Hadoop HDFS, Apache Hive & Apache Superset

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Apache Spark](https://img.shields.io/badge/Apache-Spark-E25A1C)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-yellow)
![Apache Hive](https://img.shields.io/badge/Apache-Hive-orange)
![Apache Superset](https://img.shields.io/badge/Apache-Superset-20A6C9)
![Docker](https://img.shields.io/badge/Docker-2496ED)

An end-to-end Big Data Analytics Pipeline built using **Apache Spark**, **Hadoop HDFS**, **Apache Hive**, and **Apache Superset** to process, store, query, and visualize the **Olist Brazilian E-Commerce Dataset**.

The project demonstrates a complete modern data engineering workflow, from raw CSV ingestion to analytical warehouse modeling and interactive business intelligence dashboards.

---

# 📊 Dashboard

The Apache Superset dashboard is built from the Phase 2 warehouse and tracks the KPI cards and charts described later in this README.

Screenshot assets are not tracked in this repository.

---

# 📌 Project Overview

This project analyzes approximately **100,000 Brazilian e-commerce orders** using distributed data processing and business intelligence technologies.

The project is organized into phases.

## Phase 1 — Data Ingestion and Processing

Phase 1 focuses on building the initial big data pipeline.

Implemented tasks include:

- Reading raw CSV datasets using Apache Spark
- Inferring dataset schemas
- Transforming CSV files into Apache Parquet format
- Storing curated Parquet datasets in Hadoop HDFS
- Creating Hive external tables
- Connecting Apache Superset to Hive
- Building an interactive business dashboard

Raw CSV files live in `data/raw` in the repository and are mounted into the
container at runtime.

Curated Phase 1 Parquet datasets are stored in HDFS under:

```text
hdfs://namenode:9000/olist/parquet
```

## Phase 2 — Data Warehouse and Business Analytics

Phase 2 extends the pipeline with a star-schema analytical warehouse.

Implemented tasks include:

- Data quality assessment
- ETL architecture design
- ETL vs ELT comparison
- dbt foundation work for the Phase 3 transition
- Star schema design
- Fact and dimension modeling
- Business-question mapping
- Business analytics dashboard development
- Technical documentation

The Phase 2 warehouse stores fact and dimension tables under:

```text
hdfs://namenode:9000/olist/warehouse
```

The analytical model is built around an order-item fact table.

## Phase 3 — Foundation

Phase 3 foundation work is now being implemented.

Phase 3 uses a Medallion Architecture and begins the migration toward Airflow-driven orchestration and dbt-managed transformations. The existing Phase 2 warehouse scripts remain available during the transition so the current warehouse can continue to run unchanged while Phase 3 is introduced.

Planned work includes:

- Apache Airflow for scheduling, orchestration, and monitoring
- dbt for modular SQL transformations and testing
- Bronze, Silver, and Gold Medallion Architecture layers
- Incremental data loading
- Automated data quality checks
- Improved observability and pipeline monitoring

Airflow and dbt are being implemented in Phase 3, starting with repository scaffolding and documentation.

Phase 3.4 implemented the dbt Silver layer, and Phase 3.5 is now rebuilding the Phase 2 star schema in dbt Gold models.

The existing Spark warehouse builders remain available for comparison during the migration.

Medallion boundaries:

- Bronze: source-aligned and minimally processed data
- Silver: cleaned, typed, and deduplicated business-ready entities
- Gold: facts, dimensions, KPIs, and reporting marts

The Gold layer will eventually replace the existing Spark-built star schema, while the current Phase 2 warehouse scripts remain available during the migration.

## Phase 3 — Airflow Local Infrastructure

Phase 3 now includes a production-like local Airflow stack for orchestration, scheduling, and monitoring.

Startup command:

```bash
docker compose -f docker/docker-compose-airflow.yml up -d --build
```

Airflow UI:

```text
http://localhost:8085
```

Local-demo credentials are created by `airflow-init` from environment variables.

The example values in `.env.airflow.example` are for local demonstrations only:

- `AIRFLOW_ADMIN_USERNAME=admin`
- `AIRFLOW_ADMIN_PASSWORD=admin`

Use environment variables and strong, unique credentials for non-local deployments.

Phase 3.7 connects Airflow to Spark ingestion, dbt `deps`/`debug`/`run`/`test`, readiness checks, and reconciliation artifacts written to `reports/phase3/runs/`.

The `.env.airflow.example` file shows the required environment-variable placeholders for the dbt target and for either a Superset bearer token or a username/password pair.

## Phase 3.8 Documentation Checklist

- Airflow infrastructure with `LocalExecutor`, PostgreSQL metadata storage, and the existing big-data services on `bigdata-net`
- Airflow DAG orchestration for Spark ingestion, dbt `deps`, `debug`, `run`, `test`, readiness checks, reconciliation, and Superset refresh
- dbt project scaffolding for Bronze, Silver, and Gold layers with sources, refs, models, tests, macros, docs, and lineage
- Bronze Parquet ingestion, Silver cleaning and typing, and Gold star-schema rebuilding from Silver only
- Order-item fact grain with proportional payment allocation and comparison against the Phase 2 Spark-built warehouse
- Runtime-safe reruns, idempotent metadata refresh, and reconciliation artifacts under `reports/phase3/runs/`
- Documentation for architecture, challenges, and recovery behavior without removing the Phase 1 or Phase 2 sections

Airflow containers connect to the existing services on `bigdata-net`:

- Spark master: `spark://spark-master:7077`
- Spark ThriftServer: `spark-thriftserver:10000`
- HDFS NameNode: `hdfs://namenode:9000`
- Superset: `http://superset:8088`

Service roles:

| Service | Role |
|---|---|
| `airflow-postgres` | PostgreSQL metadata database for Airflow state |
| `airflow-init` | Migrates the metadata database and creates the local admin user |
| `airflow-webserver` | Hosts the Airflow UI and API |
| `airflow-scheduler` | Queues and schedules DAG tasks |
| `airflow-triggerer` | Handles deferred and async task triggers |

## Phase 3.7 Operations

Run the medallion pipeline from Airflow:

```bash
docker compose -f docker/docker-compose-airflow.yml exec airflow-webserver \
  airflow dags trigger olist_medallion_pipeline
```

Run a one-off DAG test with a specific execution date:

```bash
docker compose -f docker/docker-compose-airflow.yml exec airflow-webserver \
  airflow dags test olist_medallion_pipeline 2024-01-01
```

Check dbt connectivity from the Airflow container:

```bash
docker compose -f docker/docker-compose-airflow.yml exec airflow-webserver \
  bash -lc 'cd /opt/airflow/dbt && dbt debug --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt --target "${AIRFLOW_DBT_TARGET:-local}"'
```

Refresh dbt packages in the same environment:

```bash
docker compose -f docker/docker-compose-airflow.yml exec airflow-webserver \
  bash -lc 'cd /opt/airflow/dbt && dbt deps --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt'
```

Inspect reconciliation artifacts:

```bash
ls -1 reports/phase3/runs/
```

---

# 🏗️ System Architecture

```text
          Olist CSV Dataset
                 │
                 ▼
           Apache Spark
      CSV Ingestion and ETL
                 │
                 ▼
         Apache Parquet
                 │
                 ▼
            Hadoop HDFS
       Distributed Storage
                 │
                 ▼
            Apache Hive
       Metadata and SQL Layer
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

The project uses the **Brazilian E-Commerce Public Dataset by Olist**.

Dataset source:

https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

The source contains approximately:

- 99,441 orders
- 99,000+ customers
- 112,000+ order items
- 104,000+ payment records
- 100,000+ reviews
- 33,000+ products
- 3,000+ sellers
- 1 million geolocation records

Main source files:

| File | Approximate Rows | Description |
|---|---:|---|
| `olist_orders_dataset.csv` | 100k | Order lifecycle, timestamps, and status |
| `olist_order_items_dataset.csv` | 112k | Product, seller, price, and freight information |
| `olist_order_payments_dataset.csv` | 104k | Payment type, installments, and payment value |
| `olist_order_reviews_dataset.csv` | 100k | Review scores and customer comments |
| `olist_customers_dataset.csv` | 100k | Customer city, state, and ZIP code |
| `olist_sellers_dataset.csv` | 3k | Seller city, state, and ZIP code |
| `olist_products_dataset.csv` | 33k | Product category, dimensions, and weight |
| `olist_geolocation_dataset.csv` | 1M | ZIP code, latitude, and longitude |
| `product_category_name_translation.csv` | 71 | Portuguese-to-English category translation |

---

# ⚙️ Data Processing Pipeline

## Step 1 — Data Ingestion

Apache Spark reads the raw CSV files from the mounted local directory.

Local input files are converted to valid local URIs using `Path.resolve().as_uri()`.

Spark schema inference is enabled so numeric, date, and string fields are represented using appropriate data types.

## Step 2 — Data Transformation

Spark transforms the datasets into Apache Parquet format.

Advantages include:

- Column-oriented storage
- Faster analytical queries
- Better compression
- Reduced storage usage
- Improved compatibility with analytical engines

## Step 3 — Distributed Storage

Processed Parquet datasets are written to Hadoop HDFS.

Phase 1 datasets:

```text
hdfs://namenode:9000/olist/parquet
```

Phase 2 warehouse tables:

```text
hdfs://namenode:9000/olist/warehouse
```

## Step 4 — Query Layer

Apache Hive provides SQL access to the Parquet datasets.

Hive external tables reference the underlying Parquet files without duplicating the data.

## Step 5 — Business Intelligence

Apache Superset connects to Hive through Spark ThriftServer and provides interactive KPI cards and analytical charts.

---

# 🏛️ Data Warehouse Design

The analytical warehouse follows a **Star Schema** architecture.

## Fact Table

### `fact_orders`

`fact_orders` is an **order-item fact table**.

Its grain is:

```text
one row per order_id + order_item_id
```

This grain supports product-, seller-, customer-, order-, and payment-level analysis while preventing duplicate revenue caused by joining multiple payment rows directly to multiple order items.

Important measures and derived columns include:

- `price`
- `freight_value`
- `item_gross_value`
- `order_items_gross_total`
- `order_payment_value`
- `allocated_payment_value`
- `review_score`
- `primary_payment_installments`

Important grouping columns include:

- `order_status`
- `primary_payment_type`

## Payment Allocation Logic

Payment records are first aggregated at order level before being joined to order items.

Order-level payment value is allocated proportionally to each item:

```text
item_gross_value = price + freight_value
```

```text
allocated_payment_value =
    order_payment_value
    × item_gross_value
    ÷ order_items_gross_total
```

This prevents revenue from being multiplied when an order contains multiple items or multiple payment records.

## Dimension Tables

### `dim_customers`

Contains customer attributes such as:

- `customer_id`
- `customer_unique_id`
- `customer_city`
- `customer_state`
- `customer_zip_code_prefix`

### `dim_products`

Contains product attributes such as:

- `product_id`
- `product_category_name`
- `product_weight_g`
- Product dimensions

### `dim_sellers`

Contains seller attributes such as:

- `seller_id`
- `seller_city`
- `seller_state`
- `seller_zip_code_prefix`

### `dim_date`

Contains date attributes such as:

- `date_key`
- `date`
- `year`
- `month`
- `day`

Dimension builders preserve one row per dimension key using `dropDuplicates`.

---

# 🔄 ETL Architecture

The project follows a traditional **ETL** workflow.

## Extract

- Read raw CSV files using Apache Spark
- Load source datasets into Spark DataFrames

## Transform

- Infer schemas
- Cast numeric columns
- Select required attributes
- Deduplicate dimension entities
- Aggregate payment records
- Join source datasets
- Build fact and dimension tables
- Convert results into Parquet

## Load

- Write curated datasets to Hadoop HDFS
- Store warehouse tables in HDFS
- Register tables through Hive
- Query data through Apache Superset

## ETL vs ELT

ETL was selected because Spark performs distributed transformations before data is exposed to the analytical layer.

In an ELT architecture, raw data would first be loaded into the warehouse and transformed later using SQL.

dbt is being implemented in Phase 3 as the transformation framework because it supports:

- Modular SQL models
- Testing
- Documentation
- Lineage
- Version-controlled transformations

dbt is part of the active Phase 3 foundation work in this repository.

---

# 💼 Business Questions

The analytical warehouse supports the following business questions:

| Business Question | Fact Table | Main Dimensions |
|---|---|---|
| Monthly revenue | `fact_orders` | `dim_date` |
| Revenue by product category | `fact_orders` | `dim_products` |
| Top-performing sellers | `fact_orders` | `dim_sellers` |
| Sales by customer state | `fact_orders` | `dim_customers` |
| Payment method trends | `fact_orders` | `primary_payment_type` |
| Average review score by category | `fact_orders` | `dim_products` |
| Revenue by order status | `fact_orders` | `order_status` |

Average delivery time by state is identified as a future extension because the current fact table does not include all delivery-duration fields required for that analysis.

---

# 📈 Dashboard Metrics

The Apache Superset dashboard contains KPI cards and business-oriented visualizations.

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

- Payment Type Distribution using `primary_payment_type`
- Order Status Distribution
- Revenue by Order Status using `allocated_payment_value`
- Review Score Distribution

These visualizations provide a concise overview of marketplace performance, payment behavior, order fulfillment, and customer satisfaction.

---

# 📁 Project Structure

```text
BigData-Pipeline-Project/
├── .env.airflow.example
├── .gitignore
├── airflow/
│   ├── config/
│   │   └── .gitkeep
│   ├── dags/
│   │   ├── .gitkeep
│   │   └── olist_medallion_pipeline.py
│   ├── logs/
│   │   └── .gitkeep
│   └── plugins/
│       └── .gitkeep
├── dbt/
│   ├── dbt_project.yml
│   ├── macros/
│   │   └── generic_tests.sql
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── README.md
│   │   │   ├── sources.yml
│   │   │   ├── stg_category_translation.sql
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_order_payments.sql
│   │   │   ├── stg_order_reviews.sql
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_products.sql
│   │   │   └── stg_sellers.sql
│   │   ├── gold/
│   │   │   ├── README.md
│   │   │   ├── dim_customers.sql
│   │   │   ├── dim_date.sql
│   │   │   ├── dim_products.sql
│   │   │   ├── dim_sellers.sql
│   │   │   ├── fact_orders.sql
│   │   │   └── schema.yml
│   │   └── silver/
│   │       ├── README.md
│   │       ├── customers.sql
│   │       ├── order_items.sql
│   │       ├── order_payments.sql
│   │       ├── order_reviews.sql
│   │       ├── orders.sql
│   │       ├── products.sql
│   │       ├── schema.yml
│   │       └── sellers.sql
│   ├── profiles.yml.example
│   ├── seeds/
│   │   └── .gitkeep
│   ├── snapshots/
│   │   └── .gitkeep
│   └── tests/
│       └── .gitkeep
├── docker/
│   ├── Dockerfile.airflow
│   ├── Dockerfile.dev
│   ├── Dockerfile.superset
│   ├── docker-compose-airflow.yml
│   ├── docker-compose-dev.yml
│   ├── docker-compose-hdfs.yml
│   ├── docker-compose-minio.yml
│   ├── docker-compose-spark.yml
│   └── docker-compose-superset.yml
├── processing/
│   ├── analysis.py
│   ├── config.py
│   ├── logger.py
│   ├── spark_session.py
│   └── warehouse/
│       ├── build_dim_customers.py
│       ├── build_dim_date.py
│       ├── build_dim_products.py
│       ├── build_dim_sellers.py
│       └── build_fact_orders.py
├── reports/
│   └── phase3/
│       └── runs/
│           └── .gitkeep
├── DESIGN.md
├── README.md
└── .gitignore
```

---

# ▶️ Running the Project

Clone the repository:

```bash
git clone https://github.com/BeyzanurArslaan/BigData-Pipeline-Project.git
cd BigData-Pipeline-Project
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

Start Apache Airflow:

```bash
docker compose -f docker/docker-compose-airflow.yml up -d --build
```

Check running services:

```bash
docker ps
```

Open the interfaces:

| Service | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Apache Superset | http://localhost:8088 |
| Apache Airflow | http://localhost:8085 |

## Superset Local Demo Credentials

```text
Username: admin
Password: admin
```

These credentials are for local demonstrations only.

Use environment variables and strong, unique credentials for non-local deployments.

## Airflow Local Demo Credentials

The Airflow admin user is created by `airflow-init` from environment variables.

The example credentials in `.env.airflow.example` are for local demos only:

- Username: `admin`
- Password: `admin`

Use strong credentials for any non-local deployment.

---

# ▶️ Running Spark Jobs

Enter the Spark container:

```bash
docker exec -it spark-master bash
```

Move to the mounted project directory:

```bash
cd /app
```

Run Phase 1 ingestion:

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/analysis.py
```

Run warehouse builders:

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/warehouse/build_fact_orders.py
```

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/warehouse/build_dim_customers.py
```

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/warehouse/build_dim_products.py
```

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/warehouse/build_dim_sellers.py
```

```bash
PYTHONPATH=/app \
/spark/bin/spark-submit processing/warehouse/build_dim_date.py
```

---

# ✅ Project Outcomes

Successfully implemented:

- Apache Spark ETL pipeline
- CSV-to-Parquet transformation
- Hadoop HDFS distributed storage
- Apache Hive SQL integration
- Star schema data warehouse
- Order-item fact table
- Fact and dimension modeling
- Payment aggregation and allocation
- Business-question mapping
- Interactive Apache Superset dashboard
- End-to-end big data analytics workflow

---

# 📄 Documentation

The repository includes:

- `README.md`
- `DESIGN.md`
- Airflow repository scaffolding
- dbt repository scaffolding
- Spark ingestion scripts
- Spark warehouse scripts

---

# 👩‍💻 Author

**Beyzanur Arslan**

Software Engineering Student

- GitHub: https://github.com/BeyzanurArslaan
- LinkedIn: [Beyzanur Arslan](https://www.linkedin.com/in/beyzanur-arslan-ba18b832a/)

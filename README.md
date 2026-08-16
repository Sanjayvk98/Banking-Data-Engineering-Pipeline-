# Real-Time Banking Data Pipeline — CDC, Kafka, Snowflake & dbt

**A streaming Medallion architecture pipeline: Postgres CDC via Kafka + Debezium, landed in MinIO S3, orchestrated into Snowflake by Airflow, and modeled through dbt (Staging → SCD Type 2 Snapshots → Star Schema Marts) — fully containerized with Docker.**

![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-3A5C7C?style=flat)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
|[Postgres](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql&logoColor=white)
---
<img width="1024" height="603" alt="architecture" src="https://github.com/user-attachments/assets/5382954e-0e21-413b-94a5-0b95c30d5b54" />

---
## 📌 Overview

This project simulates a real-time banking data platform: a Postgres OLTP database (customers, accounts, transactions) is captured via **Change Data Capture (CDC)** using **Debezium + Kafka**, landed as Parquet in **MinIO** (S3-compatible object storage), and picked up by an **Airflow**-orchestrated job every minute to load into **Snowflake**. From there, **dbt** takes over: staging views deduplicate and type the raw JSON payloads, **SCD Type 2 snapshots** track full history on customers and accounts, and a **star schema** in the Marts layer serves the curated data to **Power BI**. The entire stack — Kafka, Zookeeper, Debezium Connect, Postgres (×2), MinIO, and three Airflow services — runs from a single `docker-compose.yml`.

---

## 🏗️ Architecture

![Architecture Diagram](assets/architecture-diagram.jpg)

| Stage | Service | What happens |
|---|---|---|
| **Source (RDBMS)** | **PostgreSQL** (`banking` DB), `wal_level=logical` | OLTP schema: `customers`, `accounts`, `transactions` — seeded continuously by a Faker-based generator script |
| **Data Streaming (CDC)** | **Kafka + Debezium** (`postgres-connector`, `pgoutput` plugin) | Captures row-level inserts on all 3 tables via Postgres logical replication, publishes to topics (`banking_server.public.<table>`) |
| **Data Storage** | **MinIO S3** (`raw` bucket) | A Python Kafka consumer batches CDC events (50 records) and writes them as Parquet, partitioned by `<table>/date=<YYYY-MM-DD>/` |
| **Orchestration** | **Apache Airflow** (2 DAGs) | `minio_to_snowflake_banking` (every 1 min): downloads new Parquet from MinIO, `COPY INTO`s it into Snowflake RAW. `SCD2_snapshots` (daily): runs `dbt snapshot` then `dbt run --select marts` |
| **Data Warehouse — Staging (Raw/Source)** | **Snowflake**, `RAW` schema | Landed as `VARIANT` (raw JSON) tables — `customers`, `accounts`, `transactions` |
| **Data Processing — Cleaned (Silver/Analytics)** | **dbt views**, `ANALYTICS` schema | `stg_customers`, `stg_accounts`, `stg_transactions` — JSON parsing, type casting, dedup via `ROW_NUMBER()` |
| **Data Processing — Marts (Gold/Business)** | **dbt tables**, `MARTS` schema | Star schema: `dim_customers`, `dim_accounts` (built from SCD2 snapshots), `fact_transactions` (incremental) |
| **SCD Type 2** | **dbt snapshots**, `ANALYTICS` schema | `customers_snapshot`, `accounts_snapshot` — timestamp strategy, full history with `dbt_valid_to_current = '9999-12-31'` |
| **Visualization** | **Power BI** | `Banking Dashboard.pbix` ("Nova Bank Dashboard") — KPI cards, account/transaction type breakdowns, top customers by spend, time-series activity, and a balance-vs-amount scatter, all connected to the Marts star schema |

**Data flow:**
`Postgres (banking OLTP) → Debezium (CDC) → Kafka → Python consumer → MinIO S3 (Parquet) → Airflow (every 1 min) → Snowflake RAW (VARIANT) → dbt staging views (ANALYTICS) → dbt snapshots (SCD2) → dbt marts (star schema) → Power BI`

---

## 🔧 Pipeline Walkthrough

### 1. Source & CDC — Postgres, Debezium, Kafka
A Faker-based generator (`faker_generator.py`) continuously inserts synthetic customers, accounts, and transactions into Postgres (configured with `wal_level=logical` specifically to support CDC). A Debezium connector (`generate_and_post_connector.py`) is registered against Postgres via the Kafka Connect REST API, using the `pgoutput` logical decoding plugin to stream every insert on `customers`, `accounts`, and `transactions` into Kafka topics in near real time.

![Postgres Source](assets/03-postgres-source-oltp.png)

### 2. Streaming Consumer — Kafka → MinIO
`kafka_to_minio.py` subscribes to all three Debezium topics, batches incoming CDC events (50 records per batch), extracts just the `payload.after` (the row's new state), and writes each batch to MinIO as a Parquet file under a date-partitioned key (`<table>/date=<YYYY-MM-DD>/<table>_<HHMMSS>.parquet`) — turning a continuous CDC stream into landed, queryable files.

### 3. Orchestration — Airflow
Two DAGs drive the warehouse side:
- **`minio_to_snowflake_banking`** (every 1 minute): lists and downloads any new Parquet files per table from MinIO, then `PUT`s and `COPY INTO`s them into Snowflake's `RAW` schema tables (stored as `VARIANT`) — a micro-batch pattern that keeps the warehouse close to real time without needing a native Snowflake streaming ingestion service.
- **`SCD2_snapshots`** (daily): runs `dbt snapshot` to capture SCD Type 2 history, then `dbt run --select marts` to rebuild the star schema from the latest snapshots.

![Ingestion DAG](assets/07-airflow-ingestion-dag.png)
![SCD2 Snapshot DAG](assets/06-airflow-scd2-dag.png)

### 4. Landing — Snowflake RAW
Each Snowflake RAW table is a single-column `VARIANT` table (`CREATE TABLE customers (V VARIANT)`), holding the Debezium payload as-is — schema-on-read, deferring all typing and shape decisions to dbt.

![Snowflake RAW VARIANT](assets/04-snowflake-raw-variant.png)

### 5. Staging — dbt Views (Cleaned/Analytics)
`stg_customers`, `stg_accounts`, and `stg_transactions` parse the `VARIANT` JSON into typed columns (`v:id::string`, `v:created_at::timestamp`, etc.) and — for customers and accounts — deduplicate using `ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC)`, keeping only the latest version of each row per micro-batch load.

![Staging Views](assets/05-snowflake-staging-views.png)

### 6. SCD Type 2 — dbt Snapshots
`customers_snapshot` and `accounts_snapshot` use dbt's **timestamp** strategy (keyed on each staging view's `load_timestamp`) to track full change history, with `dbt_valid_to_current` explicitly set to `to_date('9999-12-31')` so current rows carry an open-ended sentinel date rather than `NULL`.

### 7. Marts — Star Schema
`dim_customers` and `dim_accounts` are built directly from the snapshots, deriving `effective_from`/`effective_to`/`is_current` from `dbt_valid_from`/`dbt_valid_to`. `fact_transactions` is an incremental model joining staged transactions to staged accounts (for `customer_id`), keyed on `transaction_id`.

![dbt Lineage Graph](assets/08-dbt-lineage-graph.png)

### 8. Visualization — "Nova Bank Dashboard"
The curated Marts star schema is connected to **Power BI** (`Banking Dashboard.pbix`), built as a single-page executive dashboard:

![Power BI Dashboard](assets/10-powerbi-dashboard.png)

- **KPI cards:** customer count, account count, transaction count, average transaction amount, and total transaction volume
- **MAX AMOUNT by CUSTOMER:** a ranked bar chart surfacing the highest single-transaction customers — useful for spotting high-value or anomalous activity at a glance
- **ACCOUNT TYPE / TRANSACTION TYPE donuts:** checking-vs-savings account split, and deposit/withdrawal/transfer transaction mix
- **Transaction activity over time:** a combo chart of transaction count and total amount by `TRANSACTION_TIME`
- **Average Balance vs Amount scatter:** account balance plotted against transaction amount, colored by transaction type — a quick visual check for whether transaction size correlates with account balance (it doesn't, visually, which is itself a useful negative finding)
- **KPI vs Average:** a goal-tracking visual comparing the live average transaction amount against a target

**Scale note:** the dashboard's KPI cards report 100 customers / 200 accounts / 500 transactions, noticeably smaller than the ~56K/72K/96K rows observed directly in the source Postgres tables (Section 4 below) — consistent with the dashboard being built against an early or sampled snapshot of the data rather than the full accumulated volume from repeated generator runs. Worth refreshing the `.pbix` against current Marts data before using it as a live reporting artifact.



---

## 🗂️ Repository Structure
```
Banking DBT Snowflake Airflow/
├── docker-compose.yml         # Full stack: Kafka, Zookeeper, Debezium Connect, 2x Postgres, MinIO, 3x Airflow
├── docker-airflow.dockerfile  # Airflow image + dbt-core/dbt-snowflake
├── requirements.txt
├── postgre/schema.sql         # Source OLTP schema (customers, accounts, transactions)
├── data-generator/
│   └── faker_generator.py     # Continuous synthetic data generator
├── kafka-debezium/
│   └── generate_and_post_connector.py   # Registers the Debezium Postgres connector
├── consumer/
│   └── kafka_to_minio.py      # Kafka -> MinIO Parquet consumer
└── docker/
    ├── dags/
    │   ├── mio_to_snowflake_dag.py   # Airflow: MinIO -> Snowflake RAW (every 1 min)
    │   └── scd_snapshots.py          # Airflow: dbt snapshot -> dbt run --select marts (daily)
    └── banking_dbt/            # dbt project
        ├── models/
        │   ├── sources.yml
        │   ├── staging/        # stg_customers, stg_accounts, stg_transactions
        │   └── marts/
        │       ├── dimensions/ # dim_customers, dim_accounts
        │       └── facts/      # fact_transactions
        ├── snapshots/          # customers_snapshot.yml, accounts_snapshot.yml (SCD2)
        └── profiles.yml        # Snowflake connection profile (credentials masked)

Banking Dashboard.pbix          # "Nova Bank Dashboard" — Power BI report on the Marts star schema
Assets/                         # Architecture diagram + pipeline screenshots
```

---

## ⚙️ Tech Stack

**Source & CDC:** PostgreSQL (logical replication), Debezium, Apache Kafka, Zookeeper
**Streaming consumer:** Python (`kafka-python`, `boto3`, `fastparquet`, `pandas`)
**Storage:** MinIO (S3-compatible object storage)
**Orchestration:** Apache Airflow (containerized, custom image with dbt installed)
**Warehouse & Transformation:** Snowflake, dbt (views, incremental models, snapshots)
**Modeling pattern:** Medallion architecture (Staging/Cleaned/Marts) + SCD Type 2 + Star Schema
**Visualization:** Power BI
**Infrastructure:** Docker Compose (single-command full-stack local environment)

---

## 🚀 Reproducing This Project

1. Copy `.env.example`-style files and fill in local values for Postgres, MinIO, and Airflow DB credentials (see the Security Note above — defaults are fine for local-only use).
2. `docker compose up -d` to bring up Postgres, MinIO, Kafka, Zookeeper, Debezium Connect, and the 3 Airflow services.
3. Run `postgre/schema.sql` against the `banking` Postgres database to create the OLTP schema.
4. Run `data-generator/faker_generator.py` to start continuously generating customers/accounts/transactions.
5. Run `kafka-debezium/generate_and_post_connector.py` to register the Debezium connector against Postgres.
6. Run `consumer/kafka_to_minio.py` to start consuming CDC events into MinIO as Parquet.
7. In Snowflake, run `Snowflake Script/kafkadbt.sql` to create the `BANKING` database, `RAW` schema, and the 3 VARIANT tables, plus grants for the dbt role.
8. Unpause both Airflow DAGs (`minio_to_snowflake_banking`, `SCD2_snapshots`) in the Airflow UI (`localhost:8080`).
9. Connect Power BI (or any BI tool) to the Snowflake `BANKING.MARTS` schema.

---

## 🔭 Future Work
- Fix the `is_current` flag bug in `dim_customers`/`dim_accounts` (see Known Issue above)
- Move local-default credentials (`postgres`/`postgres`, `minioadmin`/`minioadmin`) to a proper secrets manager before any non-local deployment
- Add dbt tests (`not_null`, `unique`, referential integrity between `fact_transactions` and the dimension tables) — currently absent from the marts layer
- Add a `customers`/`accounts` snapshot-driven history view to the Power BI report to actually make use of the SCD2 data being captured
- Extend CDC coverage with Debezium's `delete`/`update` handling explicitly (the consumer currently only reads `payload.after`, which is `null` on deletes and would silently drop them)

---

## 📬 Contact
Built by **Sanjay** — [GitHub](https://github.com/Sanjayvk98) · open to AI/ML Engineer and Data Engineering roles.

# Project Report: Real-Time Banking Data Pipeline — CDC, Kafka, Snowflake & dbt

**Type:** Personal portfolio project (solo build)
**Domain:** Banking / financial services
**Source data:** Synthetic banking OLTP data (customers, accounts, transactions), continuously generated

---

## 1. Problem Statement

Most portfolio data pipelines start from a static file and never touch the harder, more realistic problem of getting data out of an operational database *without* querying it directly for every report — which is exactly the constraint real banking systems operate under (an OLTP database serving live transactions cannot also serve as an analytics query engine). This project builds a genuine **Change Data Capture (CDC) pipeline**: Postgres stays untouched by analytics workloads, every row-level change streams out via logical replication, and the warehouse stays near-real-time through a full streaming-to-batch bridge (Kafka → object storage → scheduled warehouse load) rather than a periodic full-table dump.

## 2. Objectives

- Capture row-level changes from a Postgres OLTP database without querying it for analytics
- Stream those changes through Kafka using Debezium, the industry-standard CDC connector framework
- Land the stream in object storage (MinIO, S3-compatible) as an intermediate, replayable buffer
- Orchestrate a near-real-time load from object storage into Snowflake via Airflow
- Model the warehouse with dbt through a proper medallion structure: raw JSON landing, cleaned/typed staging, SCD Type 2 history, and a business-facing star schema
- Run the entire stack locally and reproducibly via Docker Compose
- Audit the repository for credential leaks before treating it as portfolio-ready

## 3. Architecture Decisions & Rationale

| Decision | Why |
|---|---|
| **Debezium + Kafka for CDC, instead of periodic batch extracts** | Captures every insert/update/delete as it happens via Postgres logical replication (`wal_level=logical`), rather than re-querying the source table on a schedule — the correct approach for both minimizing load on an OLTP system and catching a full change history rather than only current state. |
| **MinIO as an intermediate landing zone between Kafka and Snowflake** | Decouples the streaming layer from the warehouse layer: Kafka topics aren't infinitely retained by default, so buffering CDC events as durable Parquet files in object storage means the warehouse load can run on its own schedule (or be replayed) without needing to replay the Kafka log itself. |
| **A Python consumer batching 50 records before writing to MinIO** | Balances latency against file-count overhead — writing one file per CDC event would create excessive small files in object storage (a classic "small files problem"); batching in-memory before flushing is a standard streaming-to-batch pattern. |
| **Airflow polling MinIO every 1 minute rather than a Snowflake native streaming ingestion service (e.g., Snowpipe)** | A deliberate, resource-light choice for a portfolio/local-dev environment — a 1-minute micro-batch is close enough to "real-time" for demonstration purposes while keeping the architecture fully open-source and Docker-composable, without requiring a cloud-managed streaming ingestion product. |
| **Landing Snowflake RAW tables as single-column `VARIANT`, not typed columns** | Defers all schema decisions to dbt at the staging layer — if the upstream Debezium payload shape changes, the RAW landing step doesn't break; only the staging model needs to adapt. This is the same schema-on-read philosophy used in this portfolio's other Snowflake project. |
| **`ROW_NUMBER() OVER (PARTITION BY id ORDER BY created_at DESC)` in staging to dedupe** | Because the RAW landing job can load overlapping or repeated CDC events across micro-batches, staging must be idempotent to duplicate rows — this window-function pattern keeps only the latest version of each entity per load. |
| **SCD Type 2 snapshots on `customers` and `accounts`, but not `transactions`** | Customer and account attributes (name, email, balance, account type) can change and that change history has real analytical value (e.g., "what was this customer's balance last month"). Transactions are immutable events by nature — once recorded, a transaction doesn't get "updated" in place — so `fact_transactions` is modeled as a plain incremental append instead of a snapshot. |
| **`dbt_valid_to_current = to_date('9999-12-31')` sentinel instead of `NULL`** | A common, deliberate dbt convention that makes "is this row currently valid" queries range-based (`effective_date BETWEEN valid_from AND valid_to`) rather than requiring special-case `NULL` handling — though, as documented below, this project's downstream `is_current` flag was not updated to match this choice, which is exactly the kind of subtle bug this convention is meant to avoid when done consistently. |
| **Two separate Airflow DAGs (ingestion every 1 min, SCD2/marts daily) instead of one combined DAG** | Ingestion and transformation have fundamentally different freshness requirements and failure domains — a slow or failed dbt run shouldn't block or be coupled to the frequent MinIO-to-Snowflake load, and vice versa. |
| **Full stack in a single `docker-compose.yml`** | Makes the entire CDC pipeline — source database, streaming layer, object storage, and orchestration — reproducible with one command, which is a meaningfully stronger portfolio signal than a pipeline that only runs against pre-existing cloud infrastructure. |

## 4. Data & Scale

- **Source:** synthetic banking data continuously generated by a Faker-based script (10 customers, 2 accounts/customer, 50 transactions per generation cycle, looped every 2 seconds by default)
- **Observed scale in the source Postgres database** (via DBeaver inspection): **~56K accounts, ~72K customers, ~96K transactions** accumulated from repeated generator runs
- **CDC topics:** `banking_server.public.customers`, `banking_server.public.accounts`, `banking_server.public.transactions`
- **RAW landing:** single-`VARIANT`-column tables per entity in Snowflake's `BANKING.RAW` schema

## 5. Pipeline Detail

### 5.1 Source & CDC
Postgres runs with `wal_level=logical`, `max_wal_senders=10`, and `max_replication_slots=10` — the specific configuration logical replication (and therefore Debezium) requires. A Debezium connector (`postgres-connector`) is registered via a POST request to the Kafka Connect REST API (`generate_and_post_connector.py`), configured with the `pgoutput` plugin, a replication slot (`banking_slot`), and `publication.autocreate.mode=filtered` scoped to the three tables of interest.

### 5.2 Streaming Consumer
`kafka_to_minio.py` runs a `KafkaConsumer` subscribed to all three topics with `auto_offset_reset='earliest'` and `enable_auto_commit=True`, buffering records per topic and flushing to MinIO as Parquet once a topic's buffer reaches 50 records. Only `payload.after` (the row's new state post-change) is extracted from each Debezium envelope — the simplest correct handling for inserts, though this scoping choice has an implication for deletes (see Future Work).

### 5.3 Orchestration
`minio_to_snowflake_banking` runs every minute: `download_from_minio()` lists and pulls any new Parquet objects per table prefix, then `load_to_snowflake()` opens a Snowflake connector session, `PUT`s each local file to a table stage (`@%<table>`), and issues `COPY INTO ... FILE_FORMAT=(TYPE=PARQUET) ON_ERROR='CONTINUE'`. `SCD2_snapshots` runs daily via two `BashOperator` tasks: `dbt snapshot` followed by `dbt run --select marts`, both invoked with an explicit `--profiles-dir` pointing at the mounted `.dbt` credentials directory inside the Airflow container.

### 5.4 Staging — Cleaned/Analytics Layer
Three dbt views parse `VARIANT` JSON into typed columns using Snowflake's `:` path + `::type` cast syntax (e.g., `v:id::string`, `v:created_at::timestamp`). `stg_customers` and `stg_accounts` additionally deduplicate via a `ROW_NUMBER()` window keyed on the entity ID and ordered by `created_at DESC`, keeping only the latest row per entity. `stg_transactions` does not deduplicate (transactions are treated as immutable, uniquely-IDed events).

### 5.5 SCD Type 2 — Snapshots
`customers_snapshot` and `accounts_snapshot` are configured with `strategy: timestamp`, `updated_at: load_timestamp`, and the `9999-12-31` current-row sentinel — dbt's standard SCD2 pattern, correctly configured at the snapshot level.

### 5.6 Marts — Star Schema
`dim_customers` and `dim_accounts` read from their respective snapshots, renaming `dbt_valid_from`/`dbt_valid_to` to `effective_from`/`effective_to` and deriving an `is_current` boolean. `fact_transactions` is a `materialized='incremental'` model on `unique_key='transaction_id'`, left-joining staged transactions to staged accounts to bring in `customer_id`.

### 5.7 Visualization — "Nova Bank Dashboard"
The Marts star schema feeds a single-page Power BI report (`Banking Dashboard.pbix`): KPI cards (customer/account/transaction counts, average and total transaction amount), a top-customers-by-max-transaction bar chart, account-type and transaction-type donut breakdowns, a transaction-activity-over-time combo chart, a balance-vs-amount scatter colored by transaction type, and a goal-tracking KPI-vs-average visual.

The dashboard's KPI cards report **100 customers / 200 accounts / 500 transactions** — materially smaller than the **~56K / ~72K / ~96K** rows observed directly in the source Postgres tables (Section 4). This is noted here as an observation, not a defect: it indicates the `.pbix` was built against an earlier or sampled extract rather than being refreshed against current Marts volume, and should be re-pointed at live data before being treated as a current reporting artifact.

## 6. Known Issue — `is_current` Always False

**Finding:** `dim_customers.sql` and `dim_accounts.sql` both compute:
```sql
CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
```
But the corresponding snapshot configs set `dbt_valid_to_current: "to_date('9999-12-31')"` — meaning a currently-valid row's `dbt_valid_to` is `9999-12-31`, **never `NULL`**. The two settings are inconsistent with each other, so `is_current` evaluates to `FALSE` unconditionally, for every row, including active ones.

**Evidence:** a direct query against `BANKING.MARTS.DIM_ACCOUNTS` in Snowflake shows `EFFECTIVE_TO = 9999-12-31 00:00:00.000 -0800` alongside `IS_CURRENT = FALSE` on the same rows — confirming the bug is live in the actual warehouse output, not just a theoretical read of the code. The Airflow run history for the `SCD2_snapshots` DAG also shows a run of failures preceding the eventual passing runs, consistent with this area of the project being debugged.

**Fix:** align the dimension models' `is_current` logic with the snapshot's actual sentinel value:
```sql
CASE WHEN dbt_valid_to = '9999-12-31' THEN TRUE ELSE FALSE END AS is_current
```
This is documented here rather than silently patched, both because the source files weren't modified as part of this report/README generation, and because catching-and-explaining a bug like this is itself a demonstrable debugging skill worth keeping visible rather than hiding.

## 7. Security Audit — Findings

A credential audit was run across every `.env` file, SQL script, Python script, Docker config, and dbt profile in the project before this report/README were written:

- **All `.env` files** (root, `kafka-debezium/`, `consumer/`, `data-generator/`, `docker/dags/`): local-only default values (`postgres`/`postgres`, `minioadmin`/`minioadmin`) or angle-bracket placeholders (`<user>`, `<access-key>`) — nothing that would grant access to any real external system.
- **Root `.gitignore`** correctly excludes `.env` (which covers every subdirectory's `.env` file via the bare pattern), `docker-compose.override.yml`, and various cache/log directories.
- **All Python scripts** consistently load credentials via `python-dotenv` + `os.getenv()` — no hardcoded values found in `generate_and_post_connector.py`, `kafka_to_minio.py`, or `faker_generator.py`.
- **`docker-compose.yml`** sources every credential from `${VAR}` environment substitution, with no inline secrets.
- **`docker/banking_dbt/profiles.yml`**: masked in its current state (account/user/password all show as asterisks), but **not excluded by `.gitignore`** — only `target/`, `dbt_packages/`, `logs/`, and `.env` are. This file additionally contains full connection profiles for two other projects in this portfolio (`movies_sf_dbt`, and `walmart_project` — the latter including a Databricks host/http_path/token), strongly suggesting a shared, global `~/.dbt/profiles.yml` was copied directly into this repository rather than a project-scoped profile being created.

**Recommended actions (not performed as part of this audit, since it did not modify source files):**
1. Add `profiles.yml` to `.gitignore` immediately.
2. Run `git log -p -- docker/banking_dbt/profiles.yml` to check whether any earlier commit contains unmasked credentials for the Snowflake, Movies, or Walmart projects.
3. If any unmasked credential is found in history, rotate it (Snowflake password, Databricks token) regardless of whether the repository is ever made public — git history can be reconstructed even after a file is later removed or masked.
4. Consider a project-scoped `profiles.yml` per repo going forward, rather than one shared file covering multiple unrelated projects, to reduce blast radius if any single repo is exposed.

## 8. Challenges & How They Were Addressed

- **Keeping Postgres free of analytics query load** → solved with Debezium's log-based CDC (reading the WAL) rather than any form of polling query against the live tables.
- **Kafka's limited default retention as a durable analytics source** → solved by treating Kafka as a transport layer only, immediately persisting consumed events to MinIO as durable, replayable Parquet.
- **Avoiding a small-files problem from event-at-a-time writes** → solved with in-memory batching (50 records) before each MinIO write.
- **Getting CDC data into Snowflake without a managed streaming ingestion product** → solved with a lightweight Airflow polling DAG on a 1-minute schedule, appropriate for a local/portfolio-scale deployment.
- **Deduplicating a stream that can produce overlapping micro-batches** → solved with window-function deduplication at the dbt staging layer, keeping the pipeline idempotent regardless of what the ingestion layer re-delivers.
- **A genuine bug surfaced during review (`is_current` always false)** → documented with direct warehouse-level evidence rather than silently fixed, since the audit's scope was analysis and reporting, not code modification.

## 9. Skills Demonstrated

`Change Data Capture (CDC)` · `Debezium` · `Apache Kafka` · `PostgreSQL logical replication` · `Python streaming consumers (kafka-python, boto3, fastparquet)` · `MinIO / S3-compatible object storage` · `Apache Airflow (multi-DAG orchestration, PythonOperator, BashOperator, XCom)` · `Snowflake (VARIANT/semi-structured data, staging, COPY INTO)` · `dbt (views, incremental models, snapshots, SCD Type 2)` · `Star schema design` · `Docker Compose (multi-service local infrastructure)` · `Power BI` · `Credential hygiene / repository security auditing` · `Root-cause debugging of data-model logic (is_current bug)`

## 10. Future Work

- Fix the `is_current` logic bug in `dim_customers`/`dim_accounts` (Section 6)
- Add `profiles.yml` to `.gitignore` and rotate any credentials found in git history (Section 7)
- Add dbt tests to the Marts layer — currently no `not_null`/`unique`/relationship tests exist on `dim_customers`, `dim_accounts`, or `fact_transactions`
- Handle Debezium delete events explicitly — the consumer currently reads only `payload.after`, which is `null` on a delete event, so deletes are silently dropped rather than propagated as tombstones or soft-deletes
- Replace the 1-minute Airflow polling pattern with Snowpipe Streaming or a Kafka Connect Snowflake sink connector for genuinely lower-latency ingestion
- Move local-default credentials out of version control entirely (even as placeholders) in favor of a `.env.example` template pattern

---

## Appendix: Resume-Ready Bullet Points

- Built a real-time CDC pipeline using Debezium and Kafka to stream row-level changes from a PostgreSQL OLTP database, decoupling analytics ingestion from live transactional queries.
- Designed a streaming-to-batch bridge (Kafka → Python consumer → MinIO/S3 Parquet → Airflow-orchestrated Snowflake load) achieving near-real-time (1-minute) warehouse freshness without a managed streaming ingestion service.
- Implemented a dbt-based medallion architecture (Staging/Analytics → SCD Type 2 Snapshots → Star Schema Marts) on Snowflake, with window-function deduplication to keep staging models idempotent against overlapping CDC micro-batches.
- Orchestrated two independently-scheduled Airflow DAGs (1-minute ingestion, daily snapshot/marts transformation) within a fully containerized Docker Compose stack spanning 8+ services.
- Diagnosed and documented a data-model defect where an `is_current` SCD2 flag was inconsistently derived from a `NULL` check against a snapshot configured with a `9999-12-31` sentinel value, confirming the bug with direct warehouse query evidence.
- Conducted a full credential-leak security audit across `.env` files, SQL scripts, and dbt connection profiles prior to publishing, identifying a `.gitignore` gap that left a multi-project Snowflake/Databricks credentials file untracked for exclusion.

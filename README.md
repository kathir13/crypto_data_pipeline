# Crypto Price Ingestion Pipeline (Airflow + Snowflake)

## Overview

This project implements an end-to-end **data ingestion pipeline** that fetches real-time cryptocurrency prices from a public REST API and loads them into **Snowflake** using **Apache Airflow** for orchestration.

The pipeline follows **production-style data engineering patterns**, focusing on direct database ingestion, incremental updates, idempotency, and clear separation between staging and final tables.

---

## Problem Statement

Financial and market data changes frequently and must be ingested reliably without duplicates or stale values.

This project solves that by:
- Periodically fetching crypto prices
- Storing raw data in a staging table
- Updating only the latest values in the final table

---

## High-Level Architecture
```
CoinGecko REST API
↓
Apache Airflow (PythonOperator)
↓
Snowflake Staging Table
↓
Snowflake MERGE (Upsert Logic)
↓
Final Analytics Table
```


---

## Core Concepts Explained

### 1. Apache Airflow (Orchestration)

Apache Airflow is used to orchestrate the pipeline by:
- Defining task dependencies
- Managing retries and failures
- Providing visibility through logs and UI

This project uses:
- Docker-based Airflow
- LocalExecutor
- A single DAG with multiple tasks

---

### 2. REST API Ingestion

The pipeline extracts data from the **CoinGecko API**, which returns cryptocurrency prices in JSON format.

Key concepts:
- API requests using Python `requests`
- Handling multiple coins and currencies
- Stateless extraction (each run fetches fresh data)

---

### 3. Python-Based ETL

The extraction and transformation logic is implemented using an Airflow `PythonOperator`.

Responsibilities:
- Call the API
- Parse JSON responses
- Add ingestion timestamp (`run_ts`)
- Prepare structured rows for database insertion

No intermediate files (CSV, Parquet) are used — data is loaded **directly into Snowflake**.

---

### 4. Snowflake Staging Table

The staging table acts as a **temporary landing zone**.

Purpose:
- Store raw ingested data
- Decouple ingestion from final analytics logic
- Enable safe reprocessing

The staging table is **truncated after each successful merge** to prevent uncontrolled growth.

---

### 5. Incremental MERGE Logic

The final table is updated using a **Snowflake MERGE statement**.

Techniques used:
- Window function (`ROW_NUMBER`) to select the latest record
- Upsert logic (`WHEN MATCHED` / `WHEN NOT MATCHED`)
- Business keys: `run_date`, `coin_id`, `vs_currency`

This ensures:
- No duplicate records
- Latest price always wins
- Idempotent behavior across DAG runs

---

### 6. Idempotency & Reliability

The pipeline is designed so that:
- Multiple DAG runs do not corrupt data
- Failures do not leave partial results
- Restarts are safe

This mirrors real-world production data pipelines.

---

## DAG Structure

**DAG Name:** `crypto_prices_pipeline`

### Tasks

1. **extract_transform_load**
   - Fetches data from the API
   - Transforms JSON into structured rows
   - Inserts data into Snowflake staging table

2. **merge_into_final**
   - Performs incremental MERGE into the final table
   - Updates existing rows or inserts new ones

3. **truncate_staging**
   - Cleans up the staging table after a successful merge

---

## Tech Stack

- Apache Airflow (Docker, LocalExecutor)
- Python
- Snowflake
- SQL (MERGE, Window Functions)
- REST API (CoinGecko)

---

## How to Run the Project

### Prerequisites

- Docker & Docker Compose
- Snowflake account
- Airflow Snowflake connection configured (`snowflake_default`)

---

### 1. Start Airflow

```bash
cd airflow
docker compose up -d
```
### 2. Open Airflow UI

http://localhost:8080

**Login credentials:**
```
Username: admin
Password: admin
```

### 3. Configure Snowflake Connection

In Airflow UI:

Go to Admin → Connections

Create a connection with ID: snowflake_default

Provide Snowflake account, user, authentication, warehouse, database, and schema details

### 4. Trigger the DAG

Open crypto_prices_pipeline in Airflow UI

Click Trigger DAG

Monitor task execution using logs

### 5. Verify Data in Snowflake
``` SQL
SELECT
  coin_id,
  vs_currency,
  price,
  last_updated_ts
FROM DE_PROJECTS.ORDERS_DEMO.CRYPTO_PRICES
ORDER BY last_updated_ts DESC;
```

Trigger the DAG again and confirm that last_updated_ts updates correctly.

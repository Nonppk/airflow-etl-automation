# Airflow ETL Automation

A containerized **ETL pipeline** built with **Apache Airflow**, **Docker**, and **Python**, designed to demonstrate workflow automation and data transformation orchestration.

## Overview
This project automates an end-to-end ETL workflow for toll transaction data:
- Downloads and extracts source datasets (CSV, TSV, fixed-width text).
- Cleans, merges, and consolidates heterogeneous data formats.
- Transforms and outputs structured data for analysis.

## Tech Stack
- **Apache Airflow** – workflow orchestration
- **Docker Compose** – containerized setup
- **Python (Pandas, Requests)** – data extraction & transformation
- **PostgreSQL (optional)** – data persistence layer

## Project Structure
```
airflow-etl-automation/
├─ docker-compose.yml        # Airflow, Scheduler, Postgres setup
├─ dags/
│  └─ etl_toll_data.py       # Airflow DAG using PythonOperator
├─ data/                     # Input & output data (mounted volume)
├─ logs/                     # Airflow logs
├─ plugins/                  # Optional custom plugins
└─ README.md
```

## Run Instructions

### 1. Start Airflow Environment
```bash
docker compose up airflow-init
docker compose up -d
```
Then open **http://localhost:8081**  
Login with user:(as your setting)

### 2. Trigger the DAG
In the Airflow UI, enable and trigger DAG **`etl_toll_data`**.

### 3. View Outputs
Processed CSV files are generated under:
```
/opt/airflow/dags/data
```
If using volume mapping, they appear locally under:
```
./data
```

## Automation & Workflow Focus
This project highlights workflow **automation principles** — including orchestration, task dependencies, retries, and environment reproducibility — commonly used in production data pipelines.

## Author
**Ratchanon Prukprakarn**  
Passionate about automation, data workflows, and ETL system design.

GitHub: [https://github.com/Nonppk](https://github.com/Nonppk)

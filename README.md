# 🚀 AlphaStream: Automated Financial Data Pipeline
![Python](https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python&logoColor=white)
![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![BigQuery](https://img.shields.io/badge/BigQuery-Google_Data_Warehouse-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Looker](https://img.shields.io/badge/Looker_Studio-Visuals-4285F4?style=for-the-badge&logo=google-looker-studio&logoColor=white)

**A Cloud-Native ETL Pipeline for Real-Time Stock Volatility Analysis**

---

## 📌 Project Overview
This project automates the ingestion, transformation, and visualization of historical stock market data. It utilizes a **Serverless Architecture** on Google Cloud Platform (GCP) to fetch data from Yahoo Finance, process it through a multi-layer storage strategy, and deliver interactive insights via Looker Studio.

---

## 🏗 Architecture & Data Flow
```mermaid
graph LR
    A[Yahoo Finance API] -->|Fetch Data| B(Cloud Function)
    subgraph GCP Environment
        B -->|Raw Parquet| C[(GCS Staging Bucket)]
        C -->|Load Job| D[(BigQuery Warehouse)]
        D -->|SQL View| E[Looker Studio Dashboard]
    end
    F[Cloud Scheduler] -->|Trigger Daily| B
    style B fill:#f9f,stroke:#333,stroke-width:2px
    style D fill:#bbf,stroke:#333,stroke-width:2px

Ingestion: Python Cloud Function triggers daily via Scheduler.

Staging: Saves raw data to GCS (Parquet) for fault tolerance.

Warehouse: Loads data into BigQuery with schema enforcement.

Analytics: Visualizes volatility trends in Looker Studio.

🛠 Tech Stack
Language: Python 3.11 (Pandas, yfinance)

Compute: Cloud Functions (Gen 2)

Orchestration: Cloud Scheduler (Cron: 0 9 * * *)

Storage: GCS (Bronze Layer) & BigQuery (Gold Layer)

🔑 Key Engineering Decisions
🛡️ Security: Used a dedicated Service Account with granular IAM permissions (principle of least privilege).

💾 Format: Chose Parquet over CSV to preserve data types (schema enforcement) and reduce storage costs.

🔄 Idempotency: Pipeline uses WRITE_TRUNCATE strategies to prevent duplicate data if the job is re-run.
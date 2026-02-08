AlphaStream: Automated Financial Data Pipeline
A Cloud-Native ETL Pipeline for Real-Time Stock Volatility Analysis

Project Overview
This project automates the ingestion, transformation, and visualization of historical stock market data. It utilizes a Serverless Architecture on Google Cloud Platform (GCP) to fetch data from Yahoo Finance, process it through a multi-layer storage strategy, and deliver interactive insights via Looker Studio.

Tech Stack
Language: Python 3.11 (Pandas, yfinance, functions-framework)

Infrastructure: GCP Cloud Functions (Gen 2), Cloud Scheduler, Artifact Registry

Storage: Google Cloud Storage (GCS) - Staging Layer

Database: Google BigQuery (Standard SQL)

Visualization: Looker Studio

Architecture & Data Flow
Ingestion (Extract): A Python-based Cloud Function is triggered daily via Cloud Scheduler (Cron: 0 9 * * *).

Staging (Load): Raw data is converted to Apache Parquet format and stored in a GCS Bucket. This "decouples" the ingestion from the database for better fault tolerance.

Warehouse (Transform): Data is loaded into BigQuery using a WRITE_TRUNCATE policy to ensure idempotency.

Analytics Layer: A SQL View calculates a 7-Day Moving Average using Window Functions to provide trend analysis.

Key Engineering Decisions
Why Parquet? Chose Parquet over CSV to preserve schema (data types) and optimize storage costs and query performance in BigQuery.

Security (IAM): Implemented a dedicated Service Account with the "Principle of Least Privilege," ensuring the ingestion bot only has access to specific project resources.

Idempotency: The pipeline is designed to be re-run safely. If a job fails, re-running it will not result in duplicate data, thanks to the WRITE_TRUNCATE configuration.

Serverless Scaling: Using Cloud Functions ensures "Scale-to-Zero" cost efficiency—we only pay for the exact seconds the data is being processed.

Final Dashboard
The final output is an interactive dashboard showing:

Daily Closing Prices vs. 7-Day Moving Averages.

Custom Volatility Index (High-Low Spread).

Monthly Aggregated Trends with linear interpolation for weekend gaps.
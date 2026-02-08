import functions_framework
import pandas as pd
import yfinance as yf  # <--- The new tool
from google.cloud import storage, bigquery
import os
from datetime import datetime

# Settings
PROJECT_ID = "alphastream-batch-26-ey"
BUCKET_NAME = "alphastream-batch-26-ey-data"
DATASET_ID = "raw"
TABLE_ID = "volatility_raw"

@functions_framework.http
def trigger_ingestion(request):
    print("--- Starting Real-World Ingestion ---")

    # 1. DEFINE THE SYMBOLS
    # We will fetch 1 year of data for these tech giants
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]
    
    print(f"Fetching data for: {symbols}")

    # 2. FETCH REAL DATA FROM YAHOO FINANCE
    # download() returns a complex MultiIndex dataframe, we need to flatten it
    raw_df = yf.download(symbols, period="1y", group_by='ticker', auto_adjust=True)

    # 3. TRANSFORM DATA (Flattening the weird structure)
    all_data = []
    
    for ticker in symbols:
        # Extract just this ticker's dataframe
        ticker_df = raw_df[ticker].copy()
        
        # Reset index so 'Date' becomes a column, not the index
        ticker_df = ticker_df.reset_index()
        
        # Add the symbol column manually
        ticker_df['symbol'] = ticker
        
        # Rename columns to match BigQuery Schema (Lowercase, no spaces)
        # Yahoo gives: 'Date', 'Close', 'Volume', 'High', 'Low', 'Open'
        ticker_df.rename(columns={
            'Date': 'date',
            'Close': 'close_price',
            'Volume': 'volume'
        }, inplace=True)
        
        # Calculate a simple metric so we have 'volatility_index'
        # (High - Low) / Open
        ticker_df['volatility_index'] = (ticker_df['High'] - ticker_df['Low']) / ticker_df['Open']
        
        # Select only the columns we want
        final_df = ticker_df[['symbol', 'date', 'close_price', 'volatility_index', 'volume']]
        
        # Append to our master list
        all_data.append(final_df)

    # Combine all tickers into one giant dataframe
    master_df = pd.concat(all_data)
    
    # Convert Date to string for Parquet compatibility
    master_df['date'] = master_df['date'].dt.date.astype(str)

    print(f"Data Transformed. Rows generated: {len(master_df)}")

    # 4. SAVE TO PARQUET (LOCAL)
    filename = f"/tmp/real_stock_data_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    master_df.to_parquet(filename)
    print(f"Saved locally to {filename}")

    # 5. UPLOAD TO STORAGE
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob_name = f"raw/real_data/{datetime.now().strftime('%Y-%m-%d')}_stocks.parquet"
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(filename)
    print("Upload complete!")

    # 6. LOAD TO BIGQUERY
    bq_client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Replace old data
    )

    load_job = bq_client.load_table_from_uri(
        f"gs://{BUCKET_NAME}/{blob_name}",
        table_ref,
        job_config=job_config
    )

    load_job.result()  # Wait for it to finish
    print(f"Pipeline finished. Loaded {load_job.output_rows} rows.")

    return "Real Data Ingestion Successful", 200
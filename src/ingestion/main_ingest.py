# src/ingestion/main_ingest.py
import json
import pandas as pd
from pathlib import Path
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from src.common import config

def fetch_data() -> pd.DataFrame:
    """
    Reads mock data from local JSON. 
    In Composer, this will look in /home/airflow/gcs/dags/data/
    """
    mock_path = Path(config.MOCK_DATA_PATH)

    if not mock_path.exists():
        # Helpful for debugging in Airflow logs
        raise FileNotFoundError(f"Mock data file not found at: {mock_path.resolve()}")

    with mock_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"]).dt.date

    print(f"Fetched {len(df)} rows of data.")
    return df

def transform_to_parquet(df: pd.DataFrame, filename: str) -> Path:
    """
    Saves the DataFrame to a Parquet file.
    Uses /tmp/ to ensure write permissions in Cloud Composer.
    """
    output_path = Path("/tmp") / filename
    df.to_parquet(output_path, index=False)

    print(f"Success! Data saved to: {output_path}")
    return output_path

def upload_to_gcs(local_file_path: Path, run_date: str, credentials=None) -> str:
    """
    Uploads the Parquet file to GCS.
    If credentials=None, it uses the Composer Service Account automatically.
    """
    storage_client = storage.Client(project=config.PROJECT_ID, credentials=credentials)
    bucket = storage_client.bucket(config.BUCKET_NAME)

    blob_name = f"raw/vendor=alphastream/dt={run_date}/{local_file_path.name}"
    blob = bucket.blob(blob_name)

    print(f"Uploading to {blob_name}...")
    blob.upload_from_filename(str(local_file_path))

    print("Upload complete!")
    return f"gs://{config.BUCKET_NAME}/{blob_name}"

def load_to_bq(gcs_uri: str, credentials=None) -> str:
    """
    Loads the Parquet file from GCS into BigQuery.
    """
    client = bigquery.Client(project=config.PROJECT_ID, credentials=credentials)
    table_id = f"{config.PROJECT_ID}.{config.DATASET_RAW}.volatility_raw"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"Loading {gcs_uri} into {table_id}...")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Waits for the job to complete

    print("Load complete!")
    return table_id

def run_pipeline():
    """
    Main entry point for Cloud Functions.
    Uses Application Default Credentials (ADC) automatically.
    """
    # We can use the same hardcoded date for now to match your mock data
    RUN_DATE = "2026-01-06"

    print("--- Starting Cloud Pipeline ---")

    # 1. Fetch Data
    data_df = fetch_data()

    # 2. Transform
    parquet_path = transform_to_parquet(data_df, f"volatility_{RUN_DATE}.parquet")

    # 3. Upload
    # Note: We pass credentials=None so it uses the Cloud Function's identity automatically
    uri = upload_to_gcs(parquet_path, RUN_DATE, credentials=None)

    # 4. Load
    # Note: We pass credentials=None here as well
    final_table = load_to_bq(uri, credentials=None)

    print(f"Pipeline finished successfully. Data loaded to: {final_table}")

if __name__ == "__main__":
    # --- LOCAL TESTING ONLY ---
    RUN_DATE = "2026-01-06"

    # Use the JSON key for local runs
    local_creds = service_account.Credentials.from_service_account_file(
        config.SERVICE_ACCOUNT_KEY
    )

    data_df = fetch_data()
    parquet_path = transform_to_parquet(data_df, f"volatility_{RUN_DATE}.parquet")
    uri = upload_to_gcs(parquet_path, RUN_DATE, credentials=local_creds)
    final_table = load_to_bq(uri, credentials=local_creds)

    print("\n--- Local Job Finished ---")
    print(f"Data ready at: {final_table}")
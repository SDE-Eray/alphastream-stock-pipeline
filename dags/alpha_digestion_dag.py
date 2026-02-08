from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path

# --- IMPORTS ---
# Airflow adds the /dags folder to the path automatically, 
# so 'from src...' works as long as __init__.py files are present.
from src.ingestion.main_ingest import (
    fetch_data,
    transform_to_parquet,
    upload_to_gcs,
    load_to_bq
)

# --- DEFAULT ARGUMENTS ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="alpha_volatility_pipeline",
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    tags=["ingestion", "finance"],
) as dag:

    # Task 1: Fetch Data
    # fetch_data() reads from config.MOCK_DATA_PATH (data/mock_volatility.json)
    fetch_task = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data
    )

    # Task 2: Transform
    def transform_callable(**kwargs):
        df = kwargs['ti'].xcom_pull(task_ids='fetch_data')
        # 'ds' is the Airflow macro for the execution date (YYYY-MM-DD)
        run_date = kwargs['ds'] 
        return transform_to_parquet(df, f"volatility_{run_date}.parquet")

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_callable
    )
    
    # Task 3: Upload
    def upload_callable(**kwargs):
        # file_path comes from transform_data via XCom
        file_path = kwargs['ti'].xcom_pull(task_ids='transform_data')
        run_date = kwargs['ds']
        
        # Passing credentials=None triggers Application Default Credentials
        return upload_to_gcs(file_path, run_date, credentials=None)

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_callable
    )

    # Task 4: Load to BQ
    def load_callable(**kwargs):
        # gcs_uri comes from upload_to_gcs via XCom
        gcs_uri = kwargs['ti'].xcom_pull(task_ids='upload_to_gcs')
        
        return load_to_bq(gcs_uri, credentials=None)

    load_task = PythonOperator(
        task_id="load_to_bq",
        python_callable=load_callable
    )

    # Execution Flow
    fetch_task >> transform_task >> upload_task >> load_task
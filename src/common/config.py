import os

# Google Cloud Configuration
PROJECT_ID = "alphastream-batch-26-ey"
# UPDATE THIS: Paste the exact name of your existing us-central1 bucket below
BUCKET_NAME = "alphastream-batch-26-ey-data" 
DATASET_RAW = "raw"

# Data Paths
MOCK_DATA_PATH = "data/mock_volatility.json"

# Service Account Key (Not used in Cloud Function, but kept for local testing)
SERVICE_ACCOUNT_KEY = "service-account.json"
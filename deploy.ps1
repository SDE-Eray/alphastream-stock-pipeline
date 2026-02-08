# deploy.ps1
Write-Host "Deploying to Google Cloud..." -ForegroundColor Cyan

# The Monster Command (Saved here so you don't have to type it!)
gcloud functions deploy alpha-ingest-function `
  --gen2 `
  --runtime=python311 `
  --region=us-central1 `
  --source=. `
  --entry-point=trigger_ingestion `
  --trigger-http `
  --memory=1Gi `
  --timeout=540s `
  --service-account=ingestion-bot@alphastream-batch-26-ey.iam.gserviceaccount.com `
  --build-service-account=projects/alphastream-batch-26-ey/serviceAccounts/builder-sa@alphastream-batch-26-ey.iam.gserviceaccount.com

Write-Host "Done! Function is active." -ForegroundColor Green
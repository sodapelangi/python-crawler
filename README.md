# BPK Regulation Crawler Service

Python Flask API service for crawling Indonesian regulations from peraturan.bpk.go.id

## Features

- Scrape regulation metadata from BPK website
- Convert PDF documents to markdown text
- Upload files to Supabase Storage
- Store metadata in Supabase database
- Track crawl jobs with progress monitoring
- Duplicate detection to avoid re-crawling

## Deployment to Google Cloud Run

### Prerequisites

1. Google Cloud Project with billing enabled
2. Google Cloud SDK installed and authenticated
3. Supabase project with credentials

### Environment Variables

Configure these in Cloud Run:

- `SUPABASE_URL`: Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY`: Your Supabase service role key

### Build and Deploy

```bash
# Set your project ID
export PROJECT_ID=your-gcp-project-id

# Build container image
gcloud builds submit --tag gcr.io/$PROJECT_ID/bpk-crawler

# Deploy to Cloud Run
gcloud run deploy bpk-crawler \
  --image gcr.io/$PROJECT_ID/bpk-crawler \
  --platform managed \
  --region asia-southeast1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --min-instances 0 \
  --max-instances 10 \
  --set-env-vars SUPABASE_URL=$SUPABASE_URL,SUPABASE_SERVICE_ROLE_KEY=$SUPABASE_SERVICE_ROLE_KEY
```

### Using Secret Manager (Recommended)

```bash
# Create secrets
echo -n "your_supabase_url" | gcloud secrets create supabase-url --data-file=-
echo -n "your_service_role_key" | gcloud secrets create supabase-service-key --data-file=-

# Deploy with secrets
gcloud run deploy bpk-crawler \
  --image gcr.io/$PROJECT_ID/bpk-crawler \
  --platform managed \
  --region asia-southeast1 \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 2 \
  --timeout 300 \
  --min-instances 0 \
  --max-instances 10 \
  --set-secrets SUPABASE_URL=supabase-url:latest,SUPABASE_SERVICE_ROLE_KEY=supabase-service-key:latest
```

## API Endpoints

### Health Check
```
GET /health
```

### Start Crawl
```
POST /api/crawl
Content-Type: application/json

{
  "max_items": 50,
  "years": [2025, 2024, 2023],
  "jenis_ids": [8, 10, 11, 19],
  "created_by": "user_uuid"
}
```

### Get Job Status
```
GET /api/crawl/jobs/{job_id}
```

### List Jobs
```
GET /api/crawl/jobs?page=1&limit=20
```

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials

# Run locally
python app.py
```

## Regulation Types (jenis_ids)

- 8: Undang-Undang (UU)
- 10: Peraturan Pemerintah (PP)
- 11: Peraturan Presiden (Perpres)
- 19: Peraturan Menteri (Permen)

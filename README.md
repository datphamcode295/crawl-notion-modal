# Notion Data Processing Pipeline

A Modal application that processes and stores Notion page data using FastAPI endpoints and scheduled S3 uploads.

## Core Functions

### Web Endpoints
- `/api/upload` - Receives Notion page data and processes it into parquet files

```
curl --location 'https://datpvworkspace--has-simple-web-endpoint-fastapi-app.modal.run/api/upload-to-volume' \
--header 'Content-Type: application/json' \
--data '{
    "data": "### helo5",
    "url": "https://www.notion.so/Test-note-4a5f29fd1dc44de7b3f9ca9da8b044a9",
    "org": "test"
}'
```

### Data Processing
- `process_page_data()` - Processes markdown data into parquet files stored in Modal volume
- `get_file_name()` - Manages file naming and checks existing files in S3
- `send_all_file_to_s3()` - Scheduled task (Monday 8am) to upload volume files to S3

### S3 Integration  
- `upload_to_s3_api()` - Handles S3 uploads via API endpoint

## Configuration

Requires environment variables:
- `API_URL` - S3 upload API endpoint
- `API_TOKEN` - Authentication token
- `BUCKET` - S3 bucket name
- `BASE_PATH` - Base path in S3
- `GET_FILE_URL` - API for checking existing files
- `GET_FILE_API_TOKEN` - Authentication token for file check API

## Volume Management
Uses Modal volume named "output" to store parquet files temporarily before S3 upload.
Files are uploaded to S3 when row count exceeds 50 entries.

## Data Flow
1. Content received via API
2. Stored in Modal volume as parquet
3. Auto-uploads to S3 when file reaches 50 rows
4. Weekly upload of remaining files
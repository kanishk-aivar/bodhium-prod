# Adhoc Results Lambda Function

This Lambda function processes job results and generates enhanced CSV exports with additional analysis columns including brand analysis.

## Features

### Core Functionality
- Fetches completed job results from PostgreSQL database
- Converts results to CSV format
- Analyzes content from S3 files (JSON, Markdown)
- Detects soft failures and AI Overview presence
- Extracts citations from responses

### New Analysis Features (Added)
- **brand_name**: Extracted from query text using Gemini AI
- **citation_presence**: Boolean indicating if citation files exist
- **citation_count**: Number of citation files found
- **brand_count**: Number of citations where brand name is found
- **brand_present**: Boolean indicating if brand appears in any citations

## Environment Variables

Required:
- `RDS_DB_SECRET`: Secret name for database credentials
- `CSV_OUTPUT_BUCKET`: S3 bucket for CSV output
- `GEMINI_API_KEY`: API key for Gemini AI brand extraction

Optional:
- `CSV_OUTPUT_PATH`: S3 path prefix for CSV files (default: 'csv-job-results/')
- `AWS_REGION`: AWS region (default: 'us-east-1')

## Dependencies

- `boto3`: AWS SDK
- `psycopg2-binary`: PostgreSQL adapter
- `pandas`: Data manipulation
- `google-genai`: Gemini AI integration
- `numpy`, `pytz`: Additional utilities

## Brand Analysis Process

### 1. Brand Name Extraction (Batched)
- Uses Gemini AI model `gemini-2.5-flash`
- Processes up to 20 queries per API call for efficiency
- Analyzes `query_text` field from database
- Returns extracted brand name or "None" for each query

### 2. Citation Analysis
- Lists all `citation_{i}.md` files from S3 folder
- Counts total citation files found
- Checks each citation for brand name presence

### 3. Brand Count Calculation
- Counts number of citations containing the brand name
- Simple integer count of citations with brand mentions
- Case-insensitive brand name matching

### 4. Brand Presence Determination
- `true` if brand appears in any citation files (brand_count > 0)
- `false` if no brand mentions found in citations (brand_count = 0)

## CSV Output Columns

Original columns from database:
- task_id, job_id, query_id, llm_model_name, query_text, query_type
- status, s3_output_path, error_message, created_at, completed_at
- is_active, duration_seconds

Analysis columns added (in order):
- `result`: Extracted content from S3 files
- `soft_failure`: Boolean indicating soft failure detection
- `presence`: Boolean indicating AI Overview presence
- `citation_presence`: Boolean indicating if citation files exist
- `citation_count`: Number of citation files found
- `citations`: Extracted citations from responses
- `brand_name`: Extracted brand name from query
- `brand_present`: Boolean indicating brand presence in citations
- `brand_count`: Number of citations containing brand name

## Usage

### Lambda Invocation
```json
{
  "job_id": "your-job-id-here"
}
```

### API Gateway
```
POST /jobs/{job_id}/results
```

### Response
```json
{
  "status": "completed",
  "job_id": "job-id",
  "summary": {...},
  "content_analysis": {
    "total_records": 100,
    "records_with_citation_presence": 85,
    "average_citation_count": 4.2,
    "records_with_brand": 45,
    "records_with_brand_present": 38
  },
  "csv_details": {
    "s3_location": "s3://bucket/path/file.csv",
    "analysis_columns_added": ["result", "soft_failure", "presence", "citation_presence", "citation_count", "citations", "brand_name", "brand_present", "brand_count"]
  }
}
```

## Deployment

### Docker
```bash
docker build -t adhoc-results .
docker run -e GEMINI_API_KEY=your-key adhoc-results
```

### AWS Lambda
1. Build Docker image
2. Push to ECR
3. Deploy as Lambda function
4. Set environment variables
5. Configure IAM permissions for S3, RDS, and Secrets Manager

## Error Handling

- Graceful fallback if Gemini AI unavailable
- Comprehensive logging for debugging
- Thread-safe DataFrame operations
- S3 error handling for missing files/folders

## Performance

- **Batched Gemini API calls**: Processes up to 20 queries per request (vs 1 per request)
- Parallel processing with ThreadPoolExecutor for content analysis
- Configurable worker count (default: 5)
- Efficient S3 operations with pagination
- Memory-efficient DataFrame operations
- Two-phase processing: batch brand extraction, then parallel content analysis

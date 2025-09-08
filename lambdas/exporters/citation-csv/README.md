# Citations CSV Exporter Lambda

This Lambda function exports citations data from completed jobs to CSV format with enhanced analysis.

## Features

- Exports job results to CSV with citations analysis
- Extracts citations from S3 stored content (JSON and Markdown files)
- Supports both new citations.json format and legacy citation markdown files
- Provides detailed statistics on citation presence and counts
- Generates presigned URLs for direct CSV download
- Parallel processing for improved performance

## Preserved Columns

The lambda preserves these columns from the original implementation:

- `task_id` - Unique task identifier
- `job_id` - Job identifier
- `query_id` - Query identifier
- `llm_model_name` - LLM model used
- `query_text` - Original query text
- `query_type` - Type of query
- `status` - Task status
- `s3_output_path` - S3 path to output file
- `error_message` - Error message if any
- `created_at` - Task creation timestamp
- `completed_at` - Task completion timestamp
- `is_active` - Query active status
- `duration_seconds` - Task duration
- `result` - Extracted content result
- `soft_failure` - Soft failure detection
- `presence` - AI Overview presence (for GOOGLE_AI_OVERVIEW)
- `citation_presence` - Whether citations are present
- `citation_count` - Number of citations found
- `citations` - List of citations (JSON format)
- `citation1`, `citation2`, ..., `citation10` - Individual citation content columns (configurable)

## Environment Variables

- `RDS_DB_SECRET` - AWS Secrets Manager secret name for database credentials (default: 'dev/rds')
- `AWS_REGION` - AWS region (default: 'us-east-1')
- `CSV_OUTPUT_BUCKET` - S3 bucket for CSV output storage
- `CSV_OUTPUT_PATH` - S3 path prefix for CSV files (default: 'csv-job-results/')

## Input Format

The lambda accepts job_id through multiple methods:

### Direct Event
```json
{
  "job_id": "your-job-id",
  "max_citations": 10
}
```

### API Gateway Path Parameters
```
/export/citations/{job_id}
```

### Query String Parameters
```
?job_id=your-job-id&max_citations=10
```

### Request Body
```json
{
  "job_id": "your-job-id",
  "max_citations": 10
}
```

## Output Format

### Success Response
```json
{
  "status": "completed",
  "job_id": "your-job-id",
  "csv_details": {
    "generated": true,
    "s3_location": "s3://bucket/Citations/job-id/timestamp.csv",
    "filename": "Citations/job-id/timestamp.csv",
    "type": "citations_export"
  },
  "download": {
    "presigned_url": "https://...",
    "expires_in": "1 hour",
    "direct_download": true
  },
  "statistics": {
    "total_records": 100,
    "records_with_citations": 75,
    "total_citations": 250
  }
}
```

### Error Response
```json
{
  "error": "Job not found",
  "job_id": "your-job-id",
  "message": "Job does not exist or has no tasks"
}
```

## S3 Output Structure

CSV files are saved to S3 with the following structure:
```
s3://bucket/Citations/{job_id}/{timestamp}.csv
```

## Citations Analysis

The lambda performs comprehensive citations analysis:

1. **Primary Method**: Reads `citations.json` files from S3 folders
2. **Fallback Method**: Counts citation markdown files (`citation_*.md`)
3. **Content Extraction**: Extracts citations from response content using regex patterns
4. **Individual Citation Content**: Reads and includes the full content of each citation file in separate columns (`citation1`, `citation2`, etc.)
5. **Statistics**: Provides counts and presence indicators

### Citation Content Columns

- **Default**: Up to 10 citation content columns (`citation1` through `citation10`)
- **Configurable**: Use the `max_citations` parameter to control the number of citation columns
- **Content**: Each column contains the full markdown content of the corresponding citation file
- **Ordering**: Citations are sorted alphabetically by filename for consistent ordering

## Supported File Types

- **JSON files**: Extracts model-specific content and citations
- **Markdown files**: Handles ChatGPT response content extraction
- **Other formats**: Generic content extraction

## Model-Specific Processing

- **GOOGLE_AI_MODE/GOOGLE_AI_OVERVIEW**: Extracts content field from JSON
- **ChatGPT**: 
  - JSON: Extracts content field
  - Markdown: Extracts "Response Content" section
- **Perplexity**: Extracts content field from JSON

## Performance

- Parallel processing with configurable worker threads (default: 30, max: 50)
- Optimized S3 operations with enhanced connection pooling (100 max connections)
- Individual S3 clients per operation to prevent connection pool exhaustion
- TCP keepalive for efficient connection reuse
- Efficient DataFrame operations for large datasets
- Memory-conscious CSV generation with automatic cleanup

## Error Handling

- Comprehensive error logging
- Graceful degradation for missing citations
- Timeout protection for database and S3 operations
- Fallback connection methods for database
- Connection pool management to prevent exhaustion warnings
- Automatic cleanup of S3 connections and resources

## Deployment

Build and deploy using Docker:

```bash
# Build the image
docker build -t citation-csv-exporter .

# Tag for ECR
docker tag citation-csv-exporter:latest {account}.dkr.ecr.{region}.amazonaws.com/citation-csv-exporter:latest

# Push to ECR
docker push {account}.dkr.ecr.{region}.amazonaws.com/citation-csv-exporter:latest
```

## IAM Permissions Required

- `secretsmanager:GetSecretValue` - For database credentials
- `secretsmanager:DescribeSecret` - For secret validation
- `s3:GetObject` - For reading citation files
- `s3:PutObject` - For saving CSV files
- `s3:ListObjectsV2` - For listing citation files
- `rds:DescribeDBInstances` - For database connectivity (if needed)

## Monitoring

The lambda provides detailed CloudWatch logs including:
- Processing statistics
- Error details
- Performance metrics
- S3 operation results
- Database query performance

# Query Generator Batch Job - Technical Architecture

## System Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              AWS Batch Environment                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    Query Generator Batch Job                       │   │
│  │                                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │   Input     │  │   Product   │  │     AI      │  │   Output    │ │   │
│  │  │  Handler    │  │  Processor  │  │ Generation  │  │  Handler    │ │   │
│  │  │             │  │             │  │             │  │             │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  │                                                                     │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐ │   │
│  │  │   Secrets   │  │   Database  │  │   Logging   │  │   Error     │ │   │
│  │  │   Cache     │  │  Operations │  │   System    │  │  Handling   │ │   │
│  │  │             │  │             │  │             │  │             │ │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              External Services                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │     S3      │  │   Secrets   │  │     RDS     │  │ DynamoDB    │     │
│  │  Storage    │  │  Manager    │  │ PostgreSQL  │  │   Logs      │     │
│  │             │  │             │  │             │  │             │     │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     │
│                                                                             │
│  ┌─────────────┐                                                           │
│  │   Google    │                                                           │
│  │   Gemini    │                                                           │
│  │     AI      │                                                           │
│  └─────────────┘                                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow Architecture

### 1. Input Processing Flow
```
Input Sources
     │
     ├── Environment Variables (PRODUCTS_JSON)
     ├── S3 Object (INPUT_S3_BUCKET + INPUT_S3_KEY)
     └── Local File (--products-file argument)
     │
     ▼
┌─────────────────┐
│ Input Handler   │ ── Validates and parses input data
│                 │ ── Determines input source priority
│                 │ ── Loads data into memory
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Data Validation │ ── Schema validation
│                 │ ── Required field checks
│                 │ ── Data type conversion
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Product Mapper  │ ── Maps input to internal format
│                 │ ── Generates composite keys
│                 │ ── Handles product_id resolution
└─────────────────┘
```

### 2. Question Generation Flow
```
Product Data
     │
     ▼
┌─────────────────┐
│ Gemini AI       │ ── Product-specific questions
│ Model 1         │ ── Context-aware generation
│                 │ ── Structured output parsing
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Gemini AI       │ ── Market analysis questions
│ Model 2         │ ── Positioning insights
│                 │ ── Competitive context
└─────────────────┘
     │
     ▼
┌─────────────────┐
│ Question        │ ── Deduplication
│ Post-Processor  │ ── Quality filtering
│                 │ ── Format standardization
└─────────────────┘
```

### 3. Data Persistence Flow
```
Generated Questions
     │
     ├─────────────────┐
     │                 │
     ▼                 ▼
┌─────────────┐  ┌─────────────┐
│ PostgreSQL  │  │     S3      │
│ Database    │  │   Storage   │
│             │  │             │
│ ── queries  │  │ ── JSON     │
│    table    │  │    output   │
│ ── Bulk     │  │ ── Metadata │
│    inserts  │  │ ── Error    │
└─────────────┘  └─────────────┘
     │                 │
     └─────────────────┘
                    │
                    ▼
┌─────────────────┐
│ DynamoDB        │ ── Execution logs
│ Orchestration   │ ── Performance metrics
│ Logs            │ ── Error tracking
└─────────────────┘
```

## Component Architecture

### Core Processing Components

#### 1. **Input Handler (`load_input_data`)**
```python
def load_input_data() -> tuple[List[dict], str]:
    """
    Priority order:
    1. PRODUCTS_JSON environment variable
    2. S3 input (INPUT_S3_BUCKET + INPUT_S3_KEY)
    3. Local file (--products-file argument)
    4. Command line arguments
    """
```

**Responsibilities:**
- Multi-source input handling
- Data format validation
- Error handling for malformed input
- Fallback mechanisms

#### 2. **Product Processor (`get_product_ids_from_job`)**
```python
def get_product_ids_from_job(job_id: str) -> Dict[str, int]:
    """
    Retrieves existing products from job_id instead of creating new ones.
    Uses JOIN with jobselectedproducts table for efficient lookup.
    """
```

**Responsibilities:**
- Product ID resolution
- Database query optimization
- Composite key generation
- Existing product detection

#### 3. **AI Question Generator (`generate_questions`)**
```python
def generate_questions(product_info: dict, num_questions: int = 25) -> Dict[str, List[str]]:
    """
    Generates two types of questions:
    1. Product-specific questions (features, benefits, usage)
    2. Market analysis questions (positioning, reputation, ranking)
    """
```

**Responsibilities:**
- Gemini API integration
- Prompt engineering
- Response parsing
- Error handling for AI failures

#### 4. **Data Persistence Manager**
```python
def save_queries_to_rds(queries: List[tuple]) -> List[int]:
    """Bulk-insert generated questions with batch processing"""
    
def save_to_s3(data: dict, job_id: str) -> str:
    """Persist results to S3 with metadata and error handling"""
```

**Responsibilities:**
- Database transaction management
- Batch processing optimization
- S3 upload with retry logic
- Error data persistence

### Performance Optimization Components

#### 1. **Secret Caching System**
```python
# Global cache variables
_api_key_cache = None
_rds_config_cache = None

def get_gemini_api_key():
    """Fetches API key once, caches for entire job session"""
    
def get_rds_config():
    """Fetches RDS config once, caches for entire job session"""
```

**Benefits:**
- 99% reduction in Secrets Manager API calls
- Faster execution for large jobs
- Cost optimization
- Rate limit avoidance

#### 2. **Lazy Import System**
```python
def lazy_import_psycopg():
    """Import heavy dependencies only when needed"""
    
def lazy_import_genai():
    """Import AI libraries only when needed"""
```

**Benefits:**
- Faster cold start times
- Reduced memory footprint
- Conditional dependency loading
- Better resource utilization

#### 3. **Batch Database Operations**
```python
# Process queries in batches for better performance
batch_size = 50
for i in range(0, len(queries), batch_size):
    batch = queries[i:i + batch_size]
    # Process batch
```

**Benefits:**
- Reduced database round trips
- Better transaction management
- Improved error isolation
- Scalable performance

## Database Schema Integration

### Tables Used

#### 1. **products Table**
```sql
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_data JSONB NOT NULL,
    brand_name VARCHAR(255),
    product_hash VARCHAR(64) UNIQUE
);
```

**Usage:**
- Product information storage
- Brand categorization
- Hash-based deduplication

#### 2. **queries Table**
```sql
CREATE TABLE queries (
    query_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    query_text TEXT NOT NULL,
    query_type VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Usage:**
- Generated question storage
- Product association
- Question categorization
- Active/inactive status

#### 3. **jobselectedproducts Table**
```sql
CREATE TABLE jobselectedproducts (
    job_id VARCHAR(255) NOT NULL,
    product_id INTEGER REFERENCES products(product_id),
    selected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, product_id)
);
```

**Usage:**
- Job-product associations
- Efficient product lookup
- Job tracking

#### 4. **OrchestrationLogs Table (DynamoDB)**
```json
{
  "pk": "job_id",
  "sk": "timestamp#event_id",
  "job_id": "string",
  "eventName": "string",
  "details": "object",
  "event_timestamp_id": "string"
}
```

**Usage:**
- Execution event logging
- Performance metrics
- Error tracking
- Audit trail

## Error Handling Architecture

### Error Categories

#### 1. **Input Validation Errors**
```python
if not products:
    print(f"[INPUT ERROR] No products found")
    sys.exit(1)
```

**Handling:**
- Early exit with clear error messages
- Input source fallback
- Data format validation

#### 2. **External Service Errors**
```python
try:
    response = s3_client.put_object(...)
except Exception as s3_error:
    # Try alternative key
    alternative_key = f"query/backup_{uuid.uuid4().hex[:8]}_{timestamp}.json"
```

**Handling:**
- Retry mechanisms
- Alternative approaches
- Graceful degradation
- Error logging

#### 3. **AI Generation Errors**
```python
try:
    prod_resp = model.generate_content(prod_prompt)
except Exception as exc:
    print(f"[AI ERROR] Gemini error: {exc}")
    raise RuntimeError(f"Gemini error: {exc}") from exc
```

**Handling:**
- Detailed error logging
- Error propagation
- Context preservation
- Debug information

#### 4. **Database Errors**
```python
try:
    query_ids = save_queries_to_rds(all_queries)
except Exception as db_error:
    print(f"[BATCH WARNING] Database save failed: {db_error}")
    query_ids = []
```

**Handling:**
- Transaction rollback
- Partial failure handling
- Error isolation
- Continuation strategies

## Security Architecture

### Authentication & Authorization

#### 1. **IAM Role Requirements**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "s3:GetObject",
        "s3:PutObject",
        "dynamodb:PutItem"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 2. **Secret Management**
- **Gemini API Key**: Stored in AWS Secrets Manager
- **RDS Credentials**: Stored in AWS Secrets Manager
- **Access Control**: IAM role-based permissions
- **Encryption**: AES-256 encryption at rest

#### 3. **Data Protection**
- **Input Validation**: Schema validation and sanitization
- **Output Sanitization**: JSON escaping and validation
- **Access Logging**: All operations logged to DynamoDB
- **Error Masking**: Sensitive data not exposed in error messages

## Monitoring & Observability

### Metrics Collection

#### 1. **Performance Metrics**
```python
execution_stats = {
    "products_processed": len(output),
    "total_queries_generated": len(all_queries),
    "queries_saved_to_db": len(query_ids),
    "products_processing_time": products_elapsed,
    "questions_generation_time": generation_elapsed,
    "results_persistence_time": persist_elapsed
}
```

#### 2. **Health Indicators**
- **Job completion rate**: Success vs. failure ratio
- **Processing time**: Per-product and total execution time
- **Resource utilization**: Memory and CPU usage
- **Error rates**: By error type and frequency

#### 3. **Alerting Thresholds**
- **Job failure rate**: >5% over 1 hour
- **Processing time**: >2x average for job size
- **AI API errors**: >10% failure rate
- **Database errors**: >5% failure rate

### Logging Strategy

#### 1. **Structured Logging**
```python
print(f"[BATCH] Processing product {i+1}/{len(products)}: {prod.get('name', 'unknown')}")
```

**Format:**
- `[COMPONENT]` prefix for easy filtering
- Structured data in consistent format
- Timestamp inclusion
- Context preservation

#### 2. **Log Levels**
- **INFO**: Normal operation tracking
- **WARNING**: Non-critical issues
- **ERROR**: Critical failures
- **DEBUG**: Detailed execution flow

#### 3. **Log Destinations**
- **stdout/stderr**: AWS Batch job logs
- **DynamoDB**: Structured event logging
- **S3**: Error data persistence
- **CloudWatch**: AWS-native monitoring

## Scalability Considerations

### Horizontal Scaling

#### 1. **Job Parallelization**
- Multiple batch jobs can run simultaneously
- Independent product sets per job
- Shared database with proper isolation
- S3-based job coordination

#### 2. **Resource Optimization**
- **Small jobs**: 1 vCPU, 1GB memory
- **Medium jobs**: 2 vCPU, 2GB memory
- **Large jobs**: 4 vCPU, 4GB memory
- **Auto-scaling**: Based on job queue depth

#### 3. **Database Scaling**
- Connection pooling per job
- Batch insert optimization
- Read replicas for analytics
- Partitioning for large datasets

### Vertical Scaling

#### 1. **Memory Optimization**
- Lazy loading of heavy dependencies
- Efficient data structures
- Batch processing to reduce memory footprint
- Garbage collection optimization

#### 2. **CPU Optimization**
- Parallel question generation (future)
- Efficient JSON processing
- Optimized database queries
- Caching strategies

## Deployment Architecture

### Container Strategy

#### 1. **Base Image Selection**
```dockerfile
FROM python:3.11-slim
```

**Benefits:**
- Smaller image size
- Faster deployment
- Better security
- Optimized for Python

#### 2. **Layer Optimization**
```dockerfile
# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application code
COPY batch_job.py .
```

**Benefits:**
- Better Docker layer caching
- Faster rebuilds
- Efficient CI/CD pipelines

#### 3. **Security Hardening**
```dockerfile
# Create non-root user
RUN useradd --create-home --shell /bin/bash batch
USER batch
```

**Benefits:**
- Reduced attack surface
- Principle of least privilege
- Compliance requirements

### AWS Batch Integration

#### 1. **Job Definition**
```json
{
  "jobDefinitionName": "query-generator",
  "type": "container",
  "containerProperties": {
    "image": "query-generator:latest",
    "vcpus": 2,
    "memory": 2048,
    "jobRoleArn": "arn:aws:iam::123456789012:role/BatchJobRole"
  }
}
```

#### 2. **Job Queue Configuration**
- **Priority**: High for urgent jobs
- **Compute Environment**: Spot instances for cost optimization
- **Job State**: Automatic state management
- **Retry Strategy**: Exponential backoff

#### 3. **Compute Environment**
- **Instance Types**: c5, m5, r5 families
- **Spot Instances**: Up to 90% cost savings
- **Auto-scaling**: Based on queue depth
- **VPC Configuration**: Private subnets with NAT

## Integration Patterns

### Upstream Systems

#### 1. **Product Catalog Integration**
```python
# Product data can come from:
# - Direct API calls
# - S3 data exports
# - Database queries
# - Event-driven triggers
```

#### 2. **Job Orchestrator Integration**
```python
# Job submission can be triggered by:
# - Scheduled events (CloudWatch Events)
# - API Gateway requests
# - S3 object creation
# - Database changes
```

### Downstream Systems

#### 1. **Query Database Integration**
```python
# Generated questions are stored in:
# - PostgreSQL queries table
# - Search engine indexes
# - Analytics databases
# - Caching layers
```

#### 2. **User Interface Integration**
```python
# Questions are consumed by:
# - Search interfaces
# - Product pages
# - Recommendation engines
# - Analytics dashboards
```

## Future Architecture Enhancements

### Planned Improvements

#### 1. **Multi-Model AI Support**
```python
# Support for multiple AI providers:
# - OpenAI GPT models
# - Anthropic Claude
# - Local models (Llama, Mistral)
# - Model selection based on content type
```

#### 2. **Real-Time Processing**
```python
# Stream processing capabilities:
# - Kinesis integration
# - Real-time question generation
# - Live product updates
# - Streaming analytics
```

#### 3. **Advanced Caching**
```python
# Multi-layer caching:
# - Redis for frequently accessed data
# - CDN for static content
# - Application-level caching
# - Database query caching
```

#### 4. **Microservices Architecture**
```python
# Service decomposition:
# - Input processing service
# - AI generation service
# - Data persistence service
# - Monitoring service
```

---

*This architecture document provides a comprehensive technical overview of the Query Generator Batch Job system. For implementation details, refer to the main README.md and the source code.*

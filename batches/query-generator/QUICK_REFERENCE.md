# Query Generator Batch Job - Quick Reference

## Essential Commands

### Job Submission
```bash
# Basic job submission (Product IDs)
aws batch submit-job \
  --job-name "query-gen-$(date +%s)" \
  --job-queue query-generator-queue \
  --job-definition query-generator \
  --parameters '{"JOB_ID":"batch-123","PRODUCTS_JSON":"[98, 99, 100, 101]"}' \
  --region us-east-1

# Basic job submission (Legacy Format)
aws batch submit-job \
  --job-name "query-gen-legacy-$(date +%s)" \
  --job-queue query-generator-queue \
  --job-definition query-generator \
  --parameters '{"JOB_ID":"batch-123","PRODUCTS_JSON":"[{\"name\":\"Product\",\"brand\":\"Brand\"}]"}' \
  --region us-east-1

# Job with S3 input
aws batch submit-job \
  --job-name "query-gen-s3-$(date +%s)" \
  --job-queue query-generator-queue \
  --job-definition query-generator \
  --parameters '{"JOB_ID":"batch-456","INPUT_S3_BUCKET":"input-bucket","INPUT_S3_KEY":"products.json"}' \
  --region us-east-1
```

### Job Monitoring
```bash
# Check job status
aws batch describe-jobs --jobs JOB_NAME --region us-east-1

# List running jobs
aws batch list-jobs --job-queue query-generator-queue --job-status RUNNING --region us-east-1

# List recent jobs
aws batch list-jobs --job-queue query-generator-queue --job-status SUCCEEDED --region us-east-1

# Get job logs
aws logs filter-log-events \
  --log-group-name /aws/batch/job \
  --log-stream-names query-generator/JOB_NAME \
  --region us-east-1
```

### Infrastructure Management
```bash
# Check compute environment
aws batch describe-compute-environments \
  --compute-environments query-generator-compute \
  --region us-east-1

# Check job queue
aws batch describe-job-queues \
  --job-queues query-generator-queue \
  --region us-east-1

# List job definitions
aws batch describe-job-definitions \
  --job-definition-name query-generator \
  --region us-east-1
```

## Environment Variables Reference

### Required Variables
| Variable | Description | Example |
|----------|-------------|---------|
| `JOB_ID` | Unique job identifier | `"batch-12345"` |
| `NUM_QUESTIONS` | Questions per product | `"25"` |

### Input Format
| Format | Description | Example |
|--------|-------------|---------|
| **Product IDs** | Array of product IDs (recommended) | `[98, 99, 100, 101]` |
| **Legacy** | Full product data objects | `[{"name":"Product","brand":"Brand"}]` |

### Optional Variables
| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `PRODUCTS_JSON` | Product data as JSON string | `None` | `'[{"name":"Product"}]'` |
| `INPUT_S3_BUCKET` | S3 bucket for input data | `None` | `"input-bucket"` |
| `INPUT_S3_KEY` | S3 key for input data | `None` | `"products.json"` |
| `S3_BUCKET` | Output S3 bucket | `"bodhium-query"` | `"output-bucket"` |
| `ORCHESTRATION_LOGS_TABLE` | DynamoDB table name | `"OrchestrationLogs"` | `"LogsTable"` |

### AWS Configuration
| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `SECRET_NAME` | Gemini API key secret | `"Gemini-API-ChatGPT"` | `"prod/gemini"` |
| `RDS_SECRET` | RDS credentials secret | `"dev/rds"` | `"prod/rds"` |
| `SECRET_REGION` | AWS region for secrets | `"us-east-1"` | `"eu-west-1"` |

## Docker Commands

### Local Development
```bash
# Build image
docker build -t query-generator:latest .

# Test locally
docker run --rm -e JOB_ID="test-123" \
  -e PRODUCTS_JSON='[{"name":"Test","brand":"Brand"}]' \
  query-generator:latest

# Run with volume mount
docker run --rm -v $(pwd)/data:/app/data \
  -e JOB_ID="test-456" \
  -e PRODUCTS_JSON='[{"name":"Test","brand":"Brand"}]' \
  query-generator:latest
```

### ECR Operations
```bash
# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com

# Tag and push
docker tag query-generator:latest \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com/query-generator:latest

docker push \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com/query-generator:latest
```

## Database Operations

### PostgreSQL Queries
```sql
-- Check generated queries
SELECT q.query_text, q.query_type, p.product_data->>'productname' as product_name
FROM queries q
JOIN products p ON q.product_id = p.product_id
WHERE q.created_at >= NOW() - INTERVAL '1 day'
ORDER BY q.created_at DESC;

-- Check products for a specific job
SELECT p.product_data->>'productname' as product_name, p.brand_name
FROM products p
JOIN jobselectedproducts jsp ON p.product_id = jsp.product_id
WHERE jsp.job_id = 'batch-12345';

-- Count queries by type
SELECT query_type, COUNT(*) as count
FROM queries
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY query_type;
```

### DynamoDB Operations
```bash
# Query job logs
aws dynamodb query \
  --table-name OrchestrationLogs \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values '{":pk":{"S":"batch-12345"}}' \
  --region us-east-1

# Scan recent events
aws dynamodb scan \
  --table-name OrchestrationLogs \
  --filter-expression "contains(eventName, :event)" \
  --expression-attribute-values '{":event":{"S":"Completed"}}' \
  --region us-east-1
```

## S3 Operations

### Data Management
```bash
# List output files
aws s3 ls s3://bodhium-query-generator/query/ --region us-east-1

# Download results
aws s3 cp s3://bodhium-query-generator/query/batch-12345_20240115_103000_queries.json . --region us-east-1

# Upload input data
aws s3 cp products.json s3://bodhium-query-input/products/batch-12345.json --region us-east-1

# Check file metadata
aws s3api head-object \
  --bucket bodhium-query-generator \
  --key query/batch-12345_20240115_103000_queries.json \
  --region us-east-1
```

## Performance Tuning

### Resource Allocation
| Job Size | vCPUs | Memory | Expected Time |
|----------|-------|--------|---------------|
| 1-10 products | 1 | 1024 MB | 2-5 min |
| 10-50 products | 2 | 2048 MB | 5-15 min |
| 50-200 products | 4 | 4096 MB | 15-45 min |
| 200+ products | 8 | 8192 MB | 45+ min |

### Batch Size Optimization
```python
# Database batch size (in batch_job.py)
batch_size = 50  # Optimal for most cases

# For high-volume jobs, consider:
batch_size = 100  # If memory allows
batch_size = 25   # If experiencing timeouts
```

## Error Codes & Solutions

### Common Error Messages
| Error | Cause | Solution |
|-------|-------|----------|
| `[SECRET ERROR]` | Secrets Manager access denied | Check IAM permissions |
| `[DB ERROR]` | Database connection failed | Verify RDS endpoint and credentials |
| `[AI ERROR]` | Gemini API failure | Check API key and rate limits |
| `[S3 ERROR]` | S3 upload failed | Verify bucket permissions and IAM role |

### Exit Codes
| Code | Meaning | Action |
|------|---------|--------|
| 0 | Success | Job completed successfully |
| 1 | Error | Check logs for specific error |
| 137 | OOM killed | Increase memory allocation |
| 139 | Segmentation fault | Check for code issues |

## Monitoring & Alerting

### CloudWatch Metrics
```bash
# Get job metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Batch \
  --metric-name JobsSucceeded \
  --dimensions Name=JobQueueName,Value=query-generator-queue \
  --start-time $(date -d '1 hour ago' --iso-8601) \
  --end-time $(date --iso-8601) \
  --period 300 \
  --statistics Sum \
  --region us-east-1
```

### Health Checks
```bash
# Check compute environment health
aws batch describe-compute-environments \
  --compute-environments query-generator-compute \
  --query 'computeEnvironments[0].status' \
  --output text \
  --region us-east-1

# Check job queue depth
aws batch list-jobs \
  --job-queue query-generator-queue \
  --job-status RUNNABLE \
  --query 'jobSummaryList | length(@)' \
  --output text \
  --region us-east-1
```

## Cost Optimization

### Spot Instance Usage
```bash
# Update compute environment for maximum spot usage
aws batch update-compute-environment \
  --compute-environment query-generator-compute \
  --compute-resources \
    type=SPOT,minvCpus=0,maxvCpus=256,desiredvCpus=0,bidPercentage=90 \
  --region us-east-1
```

### S3 Cost Management
```bash
# Set up lifecycle policies
aws s3api put-bucket-lifecycle-configuration \
  --bucket bodhium-query-generator \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "MoveToIA",
        "Status": "Enabled",
        "Filter": {"Prefix": "query/"},
        "Transitions": [{"Days": 30, "StorageClass": "STANDARD_IA"}]
      }
    ]
  }'
```

## Security Checklist

### IAM Permissions
- [ ] `secretsmanager:GetSecretValue`
- [ ] `s3:GetObject` (input bucket)
- [ ] `s3:PutObject` (output bucket)
- [ ] `dynamodb:PutItem`
- [ ] `rds-db:connect` (if using IAM auth)

### Network Security
- [ ] VPC configuration with private subnets
- [ ] Security groups with minimal required access
- [ ] VPC endpoints for AWS services
- [ ] NAT Gateway for outbound internet access

### Data Protection
- [ ] Encryption at rest enabled
- [ ] Encryption in transit (TLS)
- [ ] Access logging enabled
- [ ] Backup and recovery procedures

## Troubleshooting Checklist

### Job Won't Start
- [ ] Check compute environment status
- [ ] Verify job queue is enabled
- [ ] Check IAM role permissions
- [ ] Verify container image exists

### Job Fails Immediately
- [ ] Check environment variables
- [ ] Verify input data format
- [ ] Check Secrets Manager access
- [ ] Review CloudWatch logs

### Job Runs but Fails
- [ ] Check database connectivity
- [ ] Verify API key validity
- [ ] Check S3 permissions
- [ ] Review error logs in DynamoDB

### Performance Issues
- [ ] Increase vCPU/memory allocation
- [ ] Check database performance
- [ ] Verify network latency
- [ ] Monitor resource utilization

## Support Contacts

### Development Team
- **Primary Contact**: Bodhium Backend Team
- **Documentation**: This Quick Reference Guide
- **Code Repository**: GitHub repository
- **Issue Tracking**: Team communication channels

### AWS Support
- **Technical Support**: AWS Support Center
- **Documentation**: AWS Batch User Guide
- **Community**: AWS Developer Forums

---

*This quick reference guide provides essential information for daily operations. For detailed information, refer to the main README.md, ARCHITECTURE.md, and DEPLOYMENT.md documents.*

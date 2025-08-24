# Query Generator Batch Job - Deployment Guide

## Prerequisites

### AWS Account Setup
- **AWS Account**: Active AWS account with appropriate permissions
- **IAM User/Role**: Administrator access or specific permissions for AWS Batch, S3, DynamoDB, RDS, and Secrets Manager
- **Region**: Choose your preferred AWS region (default: us-east-1)

### Required AWS Services
- **AWS Batch**: For job execution
- **Amazon S3**: For input/output storage
- **Amazon DynamoDB**: For orchestration logging
- **Amazon RDS**: PostgreSQL database
- **AWS Secrets Manager**: For API keys and credentials
- **Amazon ECR**: For container image storage (optional)

### Local Development Environment
- **Docker**: Version 20.10 or later
- **Python**: Version 3.11 or later
- **AWS CLI**: Version 2.0 or later
- **Git**: For version control

## Infrastructure Setup

### 1. **S3 Bucket Creation**

```bash
# Create S3 bucket for input/output data
aws s3 mb s3://bodhium-query-generator --region us-east-1

# Create S3 bucket for input data (if separate)
aws s3 mb s3://bodhium-query-input --region us-east-1

# Enable versioning for data protection
aws s3api put-bucket-versioning \
  --bucket bodhium-query-generator \
  --versioning-configuration Status=Enabled

# Set up lifecycle policies for cost optimization
aws s3api put-bucket-lifecycle-configuration \
  --bucket bodhium-query-generator \
  --lifecycle-configuration file://lifecycle-policy.json
```

**Lifecycle Policy Example:**
```json
{
  "Rules": [
    {
      "ID": "DeleteOldVersions",
      "Status": "Enabled",
      "Filter": {},
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      }
    },
    {
      "ID": "DeleteOldLogs",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "logs/"
      },
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
```

### 2. **DynamoDB Table Creation**

```bash
# Create OrchestrationLogs table
aws dynamodb create-table \
  --table-name OrchestrationLogs \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1

# Add GSI for timestamp-based queries (optional)
aws dynamodb update-table \
  --table-name OrchestrationLogs \
  --attribute-definitions \
    AttributeName=event_timestamp_id,AttributeType=S \
  --global-secondary-indexes \
    IndexName=TimestampIndex,KeySchema=[{AttributeName=event_timestamp_id,KeyType=HASH}],Projection={ProjectionType=ALL} \
  --region us-east-1
```

### 3. **Secrets Manager Setup**

```bash
# Store Gemini API key
aws secretsmanager create-secret \
  --name "Gemini-API-ChatGPT" \
  --description "Gemini API key for query generation" \
  --secret-string '{"GEMINI_API_KEY":"your-api-key-here"}' \
  --region us-east-1

# Store RDS credentials
aws secretsmanager create-secret \
  --name "dev/rds" \
  --description "RDS database credentials" \
  --secret-string '{
    "DB_HOST":"your-rds-endpoint.amazonaws.com",
    "DB_PORT":5432,
    "DB_NAME":"bodhium",
    "DB_USER":"bodhium_user",
    "DB_PASSWORD":"your-secure-password"
  }' \
  --region us-east-1
```

### 4. **RDS Database Setup**

```bash
# Create RDS subnet group
aws rds create-db-subnet-group \
  --db-subnet-group-name bodhium-subnet-group \
  --db-subnet-group-description "Subnet group for Bodhium RDS" \
  --subnet-ids subnet-12345678 subnet-87654321 \
  --region us-east-1

# Create RDS instance
aws rds create-db-instance \
  --db-instance-identifier bodhium-db \
  --db-instance-class db.t3.micro \
  --engine postgres \
  --master-username bodhium_user \
  --master-user-password your-secure-password \
  --allocated-storage 20 \
  --db-subnet-group-name bodhium-subnet-group \
  --vpc-security-group-ids sg-12345678 \
  --backup-retention-period 7 \
  --region us-east-1

# Wait for RDS to be available
aws rds wait db-instance-available \
  --db-instance-identifier bodhium-db \
  --region us-east-1
```

**Database Schema Creation:**
```sql
-- Connect to your RDS instance and run:

-- Create products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_data JSONB NOT NULL,
    brand_name VARCHAR(255),
    product_hash VARCHAR(64) UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create queries table
CREATE TABLE queries (
    query_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(product_id),
    query_text TEXT NOT NULL,
    query_type VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create jobselectedproducts table
CREATE TABLE jobselectedproducts (
    job_id VARCHAR(255) NOT NULL,
    product_id INTEGER REFERENCES products(product_id),
    selected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (job_id, product_id)
);

-- Create indexes for performance
CREATE INDEX idx_products_brand_name ON products(brand_name);
CREATE INDEX idx_queries_product_id ON queries(product_id);
CREATE INDEX idx_queries_query_type ON queries(query_type);
CREATE INDEX idx_jobselectedproducts_job_id ON jobselectedproducts(job_id);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for products table
CREATE TRIGGER update_products_updated_at 
    BEFORE UPDATE ON products 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

## Container Image Build & Push

### 1. **Local Docker Build**

```bash
# Navigate to query-generator directory
cd batches/query-generator

# Build Docker image
docker build -t query-generator:latest .

# Test locally
docker run --rm -e JOB_ID="test-123" \
  -e PRODUCTS_JSON='[{"name":"Test Product","brand":"Test Brand"}]' \
  query-generator:latest
```

### 2. **Push to Amazon ECR (Optional)**

```bash
# Create ECR repository
aws ecr create-repository \
  --repository-name query-generator \
  --region us-east-1

# Get login token
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com

# Tag image
docker tag query-generator:latest \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com/query-generator:latest

# Push image
docker push \
  $(aws sts get-caller-identity --query Account --output text).dkr.ecr.us-east-1.amazonaws.com/query-generator:latest
```

## AWS Batch Configuration

### 1. **IAM Role Creation**

```bash
# Create trust policy for batch jobs
cat > batch-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
  --role-name BatchJobRole \
  --assume-role-policy-document file://batch-trust-policy.json

# Attach required policies
aws iam attach-role-policy \
  --role-name BatchJobRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name BatchJobRole \
  --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

aws iam attach-role-policy \
  --role-name BatchJobRole \
  --policy-arn arn:aws:iam::aws:policy/SecretsManagerReadWrite

# Create custom policy for RDS access
cat > rds-access-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": [
        "arn:aws:rds-db:us-east-1:$(aws sts get-caller-identity --query Account --output text):dbuser:$(aws rds describe-db-instances --db-instance-identifier bodhium-db --query 'DBInstances[0].DbiResourceId' --output text)/bodhium_user"
      ]
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name BatchJobRole \
  --policy-name RDSAccessPolicy \
  --policy-document file://rds-access-policy.json
```

### 2. **Compute Environment Creation**

```bash
# Create compute environment
aws batch create-compute-environment \
  --compute-environment-name query-generator-compute \
  --type MANAGED \
  --state ENABLED \
  --compute-resources \
    type=SPOT,minvCpus=0,maxvCpus=256,desiredvCpus=0,bidPercentage=75,spotIamFleetRole=arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetRole,instanceTypes=c5,m5,r5,subnets=subnet-12345678,securityGroupIds=sg-12345678,instanceRole=ecsInstanceRole \
  --service-role AWSBatchServiceRole \
  --region us-east-1

# Wait for compute environment to be ready
aws batch wait compute-environment-valid \
  --compute-environments query-generator-compute \
  --region us-east-1
```

### 3. **Job Queue Creation**

```bash
# Create job queue
aws batch create-job-queue \
  --job-queue-name query-generator-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order \
    order=1,computeEnvironment=query-generator-compute \
  --region us-east-1

# Wait for job queue to be valid
aws batch wait job-queue-valid \
  --job-queues query-generator-queue \
  --region us-east-1
```

### 4. **Job Definition Creation**

```bash
# Create job definition
aws batch register-job-definition \
  --job-definition-name query-generator \
  --type container \
  --container-properties '{
    "image": "query-generator:latest",
    "vcpus": 2,
    "memory": 2048,
    "jobRoleArn": "arn:aws:iam::'$(aws sts get-caller-identity --query Account --output text)':role/BatchJobRole",
    "environment": [
      {
        "name": "SECRET_REGION",
        "value": "us-east-1"
      },
      {
        "name": "S3_BUCKET",
        "value": "bodhium-query-generator"
      },
      {
        "name": "ORCHESTRATION_LOGS_TABLE",
        "value": "OrchestrationLogs"
      }
    ],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "/aws/batch/job",
        "awslogs-region": "us-east-1",
        "awslogs-stream-prefix": "query-generator"
      }
    }
  }' \
  --region us-east-1
```

## CloudWatch Logs Setup

```bash
# Create CloudWatch log group
aws logs create-log-group \
  --log-group-name /aws/batch/job \
  --region us-east-1

# Set retention policy
aws logs put-retention-policy \
  --log-group-name /aws/batch/job \
  --retention-in-days 30 \
  --region us-east-1
```

## Testing & Validation

### 1. **Test Job Submission**

```bash
# Submit a test job
aws batch submit-job \
  --job-name "test-query-generation-$(date +%s)" \
  --job-queue query-generator-queue \
  --job-definition query-generator \
  --parameters '{
    "JOB_ID":"test-123",
    "NUM_QUESTIONS":"5",
    "PRODUCTS_JSON":"[{\"name\":\"Test Product\",\"brand\":\"Test Brand\",\"description\":\"A test product for validation\"}]"
  }' \
  --region us-east-1
```

### 2. **Monitor Job Execution**

```bash
# Get job status
aws batch describe-jobs \
  --jobs test-query-generation-1234567890 \
  --region us-east-1

# List recent jobs
aws batch list-jobs \
  --job-queue query-generator-queue \
  --job-status RUNNING \
  --region us-east-1

# Get job logs
aws logs filter-log-events \
  --log-group-name /aws/batch/job \
  --log-stream-names query-generator/test-query-generation-1234567890 \
  --region us-east-1
```

### 3. **Verify Results**

```bash
# Check S3 for output
aws s3 ls s3://bodhium-query-generator/query/ --region us-east-1

# Check DynamoDB for logs
aws dynamodb query \
  --table-name OrchestrationLogs \
  --key-condition-expression "pk = :pk" \
  --expression-attribute-values '{":pk":{"S":"test-123"}}' \
  --region us-east-1

# Check database for queries
psql -h your-rds-endpoint.amazonaws.com -U bodhium_user -d bodhium -c "
SELECT q.query_text, q.query_type, p.product_data->>'productname' as product_name
FROM queries q
JOIN products p ON q.product_id = p.product_id
WHERE p.product_data->>'productname' = 'Test Product';
"
```

## Production Deployment

### 1. **Environment-Specific Configuration**

```bash
# Create production secrets
aws secretsmanager create-secret \
  --name "prod/gemini-api" \
  --description "Production Gemini API key" \
  --secret-string '{"GEMINI_API_KEY":"your-prod-api-key"}' \
  --region us-east-1

aws secretsmanager create-secret \
  --name "prod/rds" \
  --description "Production RDS credentials" \
  --secret-string '{
    "DB_HOST":"prod-rds-endpoint.amazonaws.com",
    "DB_PORT":5432,
    "DB_NAME":"bodhium_prod",
    "DB_USER":"bodhium_prod_user",
    "DB_PASSWORD":"your-prod-password"
  }' \
  --region us-east-1
```

### 2. **Production Job Definition**

```bash
# Create production job definition
aws batch register-job-definition \
  --job-definition-name query-generator-prod \
  --type container \
  --container-properties '{
    "image": "query-generator:latest",
    "vcpus": 4,
    "memory": 4096,
    "jobRoleArn": "arn:aws:iam::'$(aws sts get-caller-identity --query Account --output text)':role/BatchJobRole",
    "environment": [
      {
        "name": "SECRET_REGION",
        "value": "us-east-1"
      },
      {
        "name": "S3_BUCKET",
        "value": "bodhium-query-generator-prod"
      },
      {
        "name": "ORCHESTRATION_LOGS_TABLE",
        "value": "OrchestrationLogsProd"
      },
      {
        "name": "SECRET_NAME",
        "value": "prod/gemini-api"
      },
      {
        "name": "RDS_SECRET",
        "value": "prod/rds"
      }
    ]
  }' \
  --region us-east-1
```

### 3. **Auto-scaling Configuration**

```bash
# Create auto-scaling policy
aws application-autoscaling register-scalable-target \
  --service-namespace batch \
  --scalable-dimension batch:job-queue \
  --resource-id job-queue/query-generator-queue \
  --min-capacity 0 \
  --max-capacity 100 \
  --region us-east-1

# Create scaling policy
aws application-autoscaling put-scaling-policy \
  --service-namespace batch \
  --scalable-dimension batch:job-queue \
  --resource-id job-queue/query-generator-queue \
  --policy-name QueryGeneratorScalingPolicy \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 10.0,
    "ScaleOutCooldown": 300,
    "ScaleInCooldown": 300
  }' \
  --region us-east-1
```

## Monitoring & Alerting

### 1. **CloudWatch Dashboard**

```bash
# Create dashboard
aws cloudwatch put-dashboard \
  --dashboard-name QueryGeneratorMetrics \
  --dashboard-body '{
    "widgets": [
      {
        "type": "metric",
        "x": 0,
        "y": 0,
        "width": 12,
        "height": 6,
        "properties": {
          "metrics": [
            ["AWS/Batch", "JobsSubmitted", "JobQueueName", "query-generator-queue"],
            [".", "JobsSucceeded", ".", "."],
            [".", "JobsFailed", ".", "."]
          ],
          "period": 300,
          "stat": "Sum",
          "region": "us-east-1",
          "title": "Batch Job Metrics"
        }
      }
    ]
  }' \
  --region us-east-1
```

### 2. **CloudWatch Alarms**

```bash
# Create job failure alarm
aws cloudwatch put-metric-alarm \
  --alarm-name QueryGeneratorJobFailures \
  --alarm-description "Alert when batch jobs fail" \
  --metric-name JobsFailed \
  --namespace AWS/Batch \
  --statistic Sum \
  --period 300 \
  --threshold 5 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 2 \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:alerts-topic \
  --dimensions Name=JobQueueName,Value=query-generator-queue \
  --region us-east-1

# Create processing time alarm
aws cloudwatch put-metric-alarm \
  --alarm-name QueryGeneratorSlowProcessing \
  --alarm-description "Alert when jobs take too long" \
  --metric-name JobDuration \
  --namespace AWS/Batch \
  --statistic Average \
  --period 300 \
  --threshold 3600 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 3 \
  --alarm-actions arn:aws:sns:us-east-1:123456789012:alerts-topic \
  --dimensions Name=JobQueueName,Value=query-generator-queue \
  --region us-east-1
```

## Cost Optimization

### 1. **Spot Instance Usage**

```bash
# Update compute environment to use more spot instances
aws batch update-compute-environment \
  --compute-environment query-generator-compute \
  --compute-resources \
    type=SPOT,minvCpus=0,maxvCpus=256,desiredvCpus=0,bidPercentage=90 \
  --region us-east-1
```

### 2. **S3 Lifecycle Policies**

```bash
# Set up intelligent tiering
aws s3api put-bucket-intelligent-tiering-configuration \
  --bucket bodhium-query-generator \
  --intelligent-tiering-configuration '{
    "Id": "IntelligentTiering",
    "Status": "Enabled",
    "Tierings": [
      {
        "Id": "StandardIA",
        "Days": 30
      },
      {
        "Id": "OneZoneIA",
        "Days": 90
      },
      {
        "Id": "Glacier",
        "Days": 180
      }
    ]
  }'
```

## Troubleshooting

### Common Issues & Solutions

#### 1. **Job Stuck in RUNNABLE State**
```bash
# Check compute environment status
aws batch describe-compute-environments \
  --compute-environments query-generator-compute \
  --region us-east-1

# Check job queue status
aws batch describe-job-queues \
  --job-queues query-generator-queue \
  --region us-east-1
```

#### 2. **Permission Denied Errors**
```bash
# Verify IAM role permissions
aws iam get-role --role-name BatchJobRole

# Check attached policies
aws iam list-attached-role-policies --role-name BatchJobRole
```

#### 3. **Container Pull Errors**
```bash
# Verify image exists and is accessible
docker pull query-generator:latest

# Check ECR repository (if using ECR)
aws ecr describe-images \
  --repository-name query-generator \
  --region us-east-1
```

#### 4. **Database Connection Issues**
```bash
# Test RDS connectivity
telnet your-rds-endpoint.amazonaws.com 5432

# Verify security group rules
aws ec2 describe-security-groups \
  --group-ids sg-12345678 \
  --region us-east-1
```

## Maintenance & Updates

### 1. **Regular Maintenance Tasks**

```bash
# Update container image
docker build -t query-generator:latest .
docker tag query-generator:latest query-generator:v$(date +%Y%m%d)
docker push query-generator:latest

# Update job definition
aws batch register-job-definition \
  --job-definition-name query-generator \
  --type container \
  --container-properties '{
    "image": "query-generator:latest",
    "vcpus": 2,
    "memory": 2048
  }' \
  --region us-east-1

# Clean up old job definitions
aws batch deregister-job-definition \
  --job-definition query-generator:1 \
  --region us-east-1
```

### 2. **Backup & Recovery**

```bash
# Create RDS snapshot
aws rds create-db-snapshot \
  --db-instance-identifier bodhium-db \
  --db-snapshot-identifier bodhium-backup-$(date +%Y%m%d) \
  --region us-east-1

# Export DynamoDB data
aws dynamodb create-export-task \
  --table-arn arn:aws:dynamodb:us-east-1:123456789012:table/OrchestrationLogs \
  --s3-bucket bodhium-backups \
  --export-format DYNAMODB_JSON \
  --region us-east-1
```

## Security Best Practices

### 1. **Network Security**
- Use private subnets for compute resources
- Implement VPC endpoints for AWS services
- Restrict security group rules to minimum required access

### 2. **Data Protection**
- Enable encryption at rest for all data stores
- Use HTTPS/TLS for data in transit
- Implement proper access logging and monitoring

### 3. **Access Control**
- Use least privilege principle for IAM roles
- Regularly rotate API keys and credentials
- Implement multi-factor authentication for console access

---

*This deployment guide provides comprehensive instructions for setting up and operating the Query Generator Batch Job system. For additional support or questions, refer to the main README.md and architecture documentation.*

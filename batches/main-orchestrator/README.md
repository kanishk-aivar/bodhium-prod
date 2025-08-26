# AWS Batch LLM Orchestrator

This project converts the LLM Orchestrator from AWS Lambda to AWS Batch for better scalability and longer execution times.

## Architecture

```
API Gateway → Trigger Lambda → AWS Batch Job → LLM Lambda Functions
```

1. **API Gateway**: Receives HTTP requests with queries
2. **Trigger Lambda**: Receives API Gateway events, submits AWS Batch job
3. **AWS Batch Job**: Processes queries, triggers LLM Lambda functions
4. **LLM Lambda Functions**: Process individual queries

## Files Overview

- `app.py` - Main batch application (converted from Lambda function)
- `trigger.py` - Lambda function to trigger batch jobs on API Gateway events
- `Dockerfile` - Container definition for batch job
- `requirements.txt` - Python dependencies
- `README.md` - This documentation

## Environment Variables

### Trigger Lambda Environment Variables

The trigger Lambda needs these environment variables:

```bash
# AWS Batch Configuration
BATCH_JOB_QUEUE=your-batch-job-queue
BATCH_JOB_DEFINITION=your-batch-job-definition

# AWS Configuration
AWS_REGION=us-east-1

# DynamoDB Tables
ORCHESTRATION_LOGS_TABLE=OrchestrationLogs

# Database Configuration
DB_SECRET_NAME=rds-db-credentials/cluster-bodhium/bodhium

# Lambda ARNs
LAMBDA_CHATGPT=arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-chatgpt-v1
LAMBDA_AIOVERVIEW=arn:aws:lambda:us-east-1:ACCOUNT:function:BodhiumAiOverview
LAMBDA_AIMODE=arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-aimode-brightdata
LAMBDA_PERPLEXITYAPI=arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-perplexityapi
```

### Batch Job Environment Variables

The batch job receives these via `containerOverrides` from the trigger Lambda:

- `JOB_ID` - Unique job ID
- `TRIGGERED_AT` - Timestamp
- `AWS_REGION` - AWS region
- `ORCHESTRATION_LOGS_TABLE` - DynamoDB table for logs
- `DB_SECRET_NAME` - Secrets Manager path for RDS credentials
- `LAMBDA_CHATGPT` - ChatGPT Lambda ARN
- `LAMBDA_AIOVERVIEW` - AI Overview Lambda ARN
- `LAMBDA_AIMODE` - AI Mode Lambda ARN
- `LAMBDA_PERPLEXITYAPI` - Perplexity Lambda ARN

## IAM Policies

### 1. Trigger Lambda IAM Role

Create an IAM role for the trigger Lambda with this policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:SubmitJob",
                "batch:DescribeJobs"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/OrchestrationLogs"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

### 2. Batch Job IAM Role

Create an IAM role for the AWS Batch job with this policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:PutItem",
                "dynamodb:GetItem",
                "dynamodb:UpdateItem",
                "dynamodb:Query",
                "dynamodb:Scan"
            ],
            "Resource": [
                "arn:aws:dynamodb:*:*:table/OrchestrationLogs"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-chatgpt-v1",
                "arn:aws:lambda:us-east-1:ACCOUNT:function:BodhiumAiOverview",
                "arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-aimode-brightdata",
                "arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-perplexityapi"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": "arn:aws:secretsmanager:*:*:secret:rds-db-credentials/cluster-bodhium/bodhium*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        }
    ]
}
```

## Deployment Steps

### 1. Build and Push Docker Image

```bash
# Set variables
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPOSITORY=llm-orchestrator
IMAGE_TAG=latest

# Create ECR repository (if not exists)
aws ecr create-repository --repository-name $ECR_REPOSITORY --region $AWS_REGION

# Login to ECR
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

# Build Docker image
docker build -t $ECR_REPOSITORY:$IMAGE_TAG .

# Tag for ECR
docker tag $ECR_REPOSITORY:$IMAGE_TAG $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG

# Push to ECR
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPOSITORY:$IMAGE_TAG
```

### 2. Create AWS Batch Job Definition

```json
{
    "jobDefinitionName": "llm-orchestrator-definition",
    "type": "container",
    "containerProperties": {
        "image": "ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/llm-orchestrator:latest",
        "vcpus": 2,
        "memory": 8192,
        "jobRoleArn": "arn:aws:iam::ACCOUNT_ID:role/BatchJobRole",
        "executionRoleArn": "arn:aws:iam::ACCOUNT_ID:role/BatchExecutionRole"
    },
    "platformCapabilities": ["EC2"]
}
```

### 3. Deploy Trigger Lambda

```bash
# Create deployment package
zip -r trigger.zip trigger.py

# Deploy Lambda function
aws lambda create-function \
    --function-name llm-orchestrator-trigger \
    --runtime python3.9 \
    --role arn:aws:iam::ACCOUNT_ID:role/TriggerLambdaRole \
    --handler trigger.lambda_handler \
    --zip-file fileb://trigger.zip \
    --timeout 60 \
    --memory-size 256 \
    --environment Variables='{
        "BATCH_JOB_QUEUE":"your-batch-job-queue",
        "BATCH_JOB_DEFINITION":"llm-orchestrator-definition",
        "AWS_REGION":"us-east-1",
        "ORCHESTRATION_LOGS_TABLE":"OrchestrationLogs",
        "DB_SECRET_NAME":"rds-db-credentials/cluster-bodhium/bodhium",
        "LAMBDA_CHATGPT":"arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-chatgpt-v1",
        "LAMBDA_AIOVERVIEW":"arn:aws:lambda:us-east-1:ACCOUNT:function:BodhiumAiOverview",
        "LAMBDA_AIMODE":"arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-aimode-brightdata",
        "LAMBDA_PERPLEXITYAPI":"arn:aws:lambda:us-east-1:ACCOUNT:function:bodhium-llm-perplexityapi"
    }'
```

### 4. Configure API Gateway

Create an API Gateway with a POST endpoint that triggers the Lambda function.

## Usage

### API Request Format

**New Format (Recommended):**
```json
{
    "selected_queries": [
        {
            "product_id": "product-123",
            "existing_queries": [
                {
                    "query_id": 1,
                    "query_text": "What are the key features?"
                }
            ],
            "new_queries": [
                "How does this compare to competitors?",
                "What are the pricing options?"
            ]
        }
    ],
    "options": {
        "max_tokens": 1000
    }
}
```

**Old Format (Backward Compatible):**
```json
{
    "queries": [
        "What are the key features?",
        "How does this compare to competitors?"
    ],
    "options": {
        "max_tokens": 1000
    }
}
```

## Monitoring

### CloudWatch Logs

- **Trigger Lambda**: `/aws/lambda/llm-orchestrator-trigger`
- **Batch Job**: Check CloudWatch logs for the batch job

### DynamoDB Tables

- **OrchestrationLogs**: Job execution logs

## Troubleshooting

### Common Issues

1. **"Unable to locate credentials"**
   - Ensure batch job has proper IAM role attached
   - Check job definition has `jobRoleArn` and `executionRoleArn`

2. **"No selected_queries provided"**
   - Verify API Gateway is sending the correct JSON format
   - Check Lambda function is receiving the event properly

3. **"Batch job queue not found"**
   - Ensure batch job queue exists and is active
   - Check job definition is registered

4. **"Lambda function not found"**
   - Verify Lambda ARNs in environment variables
   - Ensure Lambda functions exist and are accessible

### Debug Commands

```bash
# Check batch job status
aws batch describe-jobs --jobs JOB_ID

# Check Lambda logs
aws logs tail /aws/lambda/llm-orchestrator-trigger --follow

# Test the API endpoint
curl -X POST https://your-api-gateway-url/prod/orchestrator \
  -H "Content-Type: application/json" \
  -d '{
    "selected_queries": [
      {
        "product_id": "test-123",
        "new_queries": ["Test query"]
      }
    ]
  }'
```

## Security Considerations

1. **IAM Roles**: Use least privilege principle
2. **Secrets**: Store database credentials in AWS Secrets Manager
3. **VPC**: Consider running batch jobs in VPC for additional security
4. **Logging**: Enable CloudTrail for audit trails

## Cost Optimization

1. **Spot Instances**: Use spot instances for batch jobs
2. **Instance Types**: Choose appropriate instance types
3. **Job Timeout**: Set reasonable job timeouts
4. **Resource Allocation**: Optimize vCPU and memory allocation

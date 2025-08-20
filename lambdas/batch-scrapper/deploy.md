# Batch Scraper Deployment Guide

## 📋 **Overview**
This guide walks you through deploying the Dockerized batch scraper to AWS Batch for scalable web scraping operations.

## 🏗️ **Architecture**
```
Web Scraper Lambda → AWS Batch Job → Batch Scraper Container
                                    ↓
                               Database + S3 Storage
```

## 🚀 **Deployment Steps**

### **1. Build and Push Docker Image to ECR**

```bash
# Navigate to batch scraper directory
cd lambda/batch/batch_scrapper

# Get AWS ECR login token
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 127214200395.dkr.ecr.us-east-1.amazonaws.com

# Create ECR repository (if it doesn't exist)
aws ecr create-repository --repository-name bodhium-batch-scraper --region us-east-1

# Build Docker image for linux/amd64 platform
docker build --platform linux/amd64 --provenance false -t bodhium-batch-scraper .

# Tag the image
docker tag bodhium-batch-scraper:latest 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest

# Push to ECR
docker push 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest
```

### **2. Create AWS Batch Resources**

#### **A. Create Compute Environment**
```bash
aws batch create-compute-environment \
  --compute-environment-name bodhium-scraper-compute \
  --type MANAGED \
  --state ENABLED \
  --compute-resources '{
    "type": "EC2",
    "minvCpus": 0,
    "maxvCpus": 100,
    "desiredvCpus": 0,
    "instanceTypes": ["m5.large", "m5.xlarge"],
    "subnets": ["subnet-xxxxxxxxx", "subnet-yyyyyyyyy"],
    "securityGroupIds": ["sg-xxxxxxxxx"],
    "instanceRole": "arn:aws:iam::127214200395:instance-profile/ecsInstanceRole",
    "tags": {
      "Project": "Bodhium",
      "Component": "BatchScraper"
    }
  }' \
  --service-role arn:aws:iam::127214200395:role/AWSBatchServiceRole
```

#### **B. Create Job Queue**
```bash
aws batch create-job-queue \
  --job-queue-name bodhium-scraper-queue \
  --state ENABLED \
  --priority 1 \
  --compute-environment-order '[{
    "order": 1,
    "computeEnvironment": "bodhium-scraper-compute"
  }]'
```

#### **C. Create Job Definition**
```bash
aws batch register-job-definition \
  --job-definition-name bodhium-batch-scraper \
  --type container \
  --container-properties '{
    "image": "127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest",
    "vcpus": 2,
    "memory": 4096,
    "jobRoleArn": "arn:aws:iam::127214200395:role/BatchJobExecutionRole",
    "environment": [
      {"name": "AWS_DEFAULT_REGION", "value": "us-east-1"},
      {"name": "S3_BUCKET_NAME", "value": "bodhium-data"},
      {"name": "DYNAMODB_TABLE_NAME", "value": "OrchestrationLogs"},
      {"name": "GEMINI_SECRET_NAME", "value": "Gemini-API-ChatGPT"},
      {"name": "GEMINI_SECRET_REGION", "value": "us-east-1"},
      {"name": "DB_HOST", "value": "bodhium-dev.cmhacmc4ox7v.us-east-1.rds.amazonaws.com"},
      {"name": "DB_NAME", "value": "bodhium-dev"},
      {"name": "DB_USER", "value": "postgres"},
      {"name": "DB_PORT", "value": "5432"},
      {"name": "DB_SSLMODE", "value": "require"}
    ]
  }' \
  --timeout '{"attemptDurationSeconds": 3600}' \
  --retry-strategy '{"attempts": 1}'
```

### **3. Update Web Scraper Lambda Environment Variables**

Update your Web Scraper Lambda environment variables:

```bash
aws lambda update-function-configuration \
  --function-name bodhium-webscrapper \
  --environment Variables='{
    "BATCH_JOB_QUEUE": "bodhium-scraper-queue",
    "BATCH_JOB_DEFINITION": "bodhium-batch-scraper",
    "BATCH_JOB_NAME_PREFIX": "bodhium-scrapper",
    "S3_BUCKET_NAME": "bodhium-data",
    "DYNAMODB_TABLE_NAME": "OrchestrationLogs",
    "GEMINI_SECRET_NAME": "Gemini-API-ChatGPT",
    "GEMINI_SECRET_REGION": "us-east-1"
  }'
```

### **4. Create Required IAM Roles**

#### **A. Batch Job Execution Role**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::bodhium-data/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:127214200395:secret:Gemini-API-ChatGPT-*",
        "arn:aws:secretsmanager:us-east-1:127214200395:secret:dev/rds-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem"
      ],
      "Resource": "arn:aws:dynamodb:us-east-1:127214200395:table/OrchestrationLogs"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "*"
    }
  ]
}
```

#### **B. ECS Instance Role (for compute environment)**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeAccountAttributes",
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceAttribute",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeKeyPairs",
        "ec2:DescribeImages",
        "ec2:DescribeImageAttribute",
        "ec2:DescribeSpotInstanceRequests",
        "ec2:DescribeSpotFleetInstances",
        "ec2:DescribeSpotFleetRequests",
        "ec2:DescribeSpotPriceHistory",
        "ec2:DescribeVpcClassicLink",
        "ec2:DescribeLaunchTemplateVersions",
        "ec2:CreateLaunchTemplate",
        "ec2:DeleteLaunchTemplate",
        "ec2:RequestSpotFleet",
        "ec2:CancelSpotFleetRequests",
        "ec2:ModifySpotFleetRequest",
        "ec2:RegisterImage",
        "ec2:CreateTags",
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ecs:CreateCluster",
        "ecs:DeregisterContainerInstance",
        "ecs:DescribeClusters",
        "ecs:DescribeContainerInstances",
        "ecs:DescribeTaskDefinition",
        "ecs:DescribeTasks",
        "ecs:ListClusters",
        "ecs:ListContainerInstances",
        "ecs:ListTaskDefinitionFamilies",
        "ecs:ListTaskDefinitions",
        "ecs:ListTasks",
        "ecs:RegisterContainerInstance",
        "ecs:RegisterTaskDefinition",
        "ecs:RunTask",
        "ecs:StartTask",
        "ecs:StopTask",
        "ecs:UpdateContainerAgent",
        "ecs:UpdateService",
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams"
      ],
      "Resource": "*"
    }
  ]
}
```

## 🧪 **Testing Deployment**

### **Test via Web Scraper Lambda**
```bash
aws lambda invoke \
  --function-name bodhium-webscrapper \
  --payload '{
    "body": {
      "url": "https://store.mathongo.com/categories",
      "brand_name": "MathonGo Store"
    }
  }' \
  response.json && cat response.json
```

### **Monitor Batch Job**
```bash
# List running jobs
aws batch list-jobs --job-queue bodhium-scraper-queue --job-status RUNNING

# Describe specific job
aws batch describe-jobs --jobs <job-id>

# Check logs
aws logs get-log-events \
  --log-group-name /aws/batch/job \
  --log-stream-name <job-id>
```

## 📊 **Environment Variables Passed to Container**

The Web Scraper Lambda will pass these environment variables to your batch job:

- `CRAWL_URL`: Website URL to scrape
- `AWS_BATCH_JOB_ID`: Consistent job ID for tracking
- `BRAND_NAME`: Brand name for product categorization
- `AWS_REGION`: AWS region
- `S3_BUCKET_NAME`: S3 bucket for outputs
- `DYNAMODB_TABLE_NAME`: DynamoDB table for logs
- `GEMINI_SECRET_NAME`: Gemini API secret name
- `GEMINI_SECRET_REGION`: Secret region

## 🔧 **Configuration Notes**

### **Resource Specifications**
- **vCPUs**: 2 (can handle concurrent crawling)
- **Memory**: 4096 MB (for large web pages and AI processing)
- **Timeout**: 3600 seconds (1 hour for comprehensive crawling)
- **Instance Types**: m5.large, m5.xlarge (balanced compute and memory)

### **Scaling**
- **Min vCPUs**: 0 (cost-effective)
- **Max vCPUs**: 100 (high throughput)
- **Desired vCPUs**: 0 (auto-scale based on demand)

### **Networking**
- Place in **private subnets** with NAT Gateway for internet access
- Include RDS security group for database connectivity
- Ensure ECR and S3 VPC endpoints for efficient container access

## 🚀 **Quick Deploy Script**

Create a deployment script:

```bash
#!/bin/bash
# deploy-batch-scraper.sh

set -e

echo "🚀 Deploying Batch Scraper..."

# Build and push Docker image
echo "📦 Building Docker image..."
docker build --platform linux/amd64 --provenance false -t bodhium-batch-scraper .

echo "🔐 Logging into ECR..."
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 127214200395.dkr.ecr.us-east-1.amazonaws.com

echo "🏷️ Tagging and pushing image..."
docker tag bodhium-batch-scraper:latest 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest
docker push 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest

echo "✅ Deployment complete!"
echo "📋 Next steps:"
echo "   1. Create/update AWS Batch resources (compute environment, job queue, job definition)"
echo "   2. Update Web Scraper Lambda environment variables"
echo "   3. Test with a sample job"
```

Run with:
```bash
chmod +x deploy-batch-scraper.sh
./deploy-batch-scraper.sh
```

This deployment will provide you with a scalable, containerized batch scraping solution that maintains consistency with your job IDs and brand names! 🎯

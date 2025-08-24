# Bodhium Lambda Deployment Guide

This guide explains how to use the Python deployment script (`deploy.py`) to manage Bodhium Lambda functions.

## Features

- **Dual Deployment Types**: Supports both Lambda code deployments (ZIP) and container deployments (ECR)
- **Auto-Tagging**: Generates version tags with timestamps and git commit hashes
- **Revert Functionality**: Rollback single or multiple Lambda functions to previous versions
- **Persistent Configuration**: Saves Lambda configurations to avoid repeated setup
- **Deployment History**: Tracks all deployments with success/failure status
- **ECR Integration**: Automatically builds, tags, and pushes Docker images

## Prerequisites

1. **AWS CLI configured** with appropriate permissions
2. **Docker installed** and running (for container deployments)
3. **Git repository** (for auto-tagging with commit hashes)
4. **Python 3.7+** with required dependencies

## Installation

```bash
# Install dependencies
pip install -r requirements-deploy.txt

# Make script executable
chmod +x deploy.py

# Initialize configuration file
./deploy.py init-config
```

## Configuration

The script requires a `deployment_config.json` configuration file. Create it using the init command:

```json
{
  "aws_region": "us-east-1",
  "ecr_registry": "127214200395.dkr.ecr.us-east-1.amazonaws.com",
  "lambdas": {
    "orchestrator": {
      "function_name": "bodhium-llm-orchestrator",
      "deployment_type": "code",
      "source_path": "lambdas/llm-orchestrator",
      "handler": "lambda_function.lambda_handler",
      "runtime": "python3.9",
      "timeout": 300,
      "memory_size": 512
    },
    "perplexity": {
      "function_name": "bodhium-llm-perplexityapi",
      "deployment_type": "code",
      "source_path": "lambdas/workers/perplexity",
      "handler": "lambda_function.lambda_handler",
      "runtime": "python3.9",
      "timeout": 300,
      "memory_size": 512
    },
    "aio": {
      "function_name": "BodhiumAiOverview",
      "deployment_type": "container",
      "source_path": "lambdas/workers/aio",
      "ecr_repo": "bodhium-aio-worker",
      "timeout": 900,
      "memory_size": 3008
    },
    "aim": {
      "function_name": "bodhium-aimode-brightdata",
      "deployment_type": "container",
      "source_path": "lambdas/workers/aim",
      "ecr_repo": "bodhium-aim-worker",
      "timeout": 900,
      "memory_size": 3008
    },
    "chatgpt": {
      "function_name": "bodhium-llm-chatgpt-v1",
      "deployment_type": "container",
      "source_path": "lambdas/workers/ChatGPT",
      "ecr_repo": "bodhium-chatgpt-worker",
      "timeout": 900,
      "memory_size": 3008
    }
  }
}
```

## Usage

### Initialize Configuration (Required First Step)

```bash
# Create template configuration file
./deploy.py init-config

# Edit the configuration as needed
nano deployment_config.json
```

### Deploy All Lambda Functions

```bash
# Deploy all functions with auto-generated version tag
./deploy.py deploy-all

# Deploy all functions with custom version tag
./deploy.py deploy-all --version v1.2.3
```

### Deploy Single Lambda Function

```bash
# Deploy orchestrator
./deploy.py deploy --lambda orchestrator

# Deploy with custom version
./deploy.py deploy --lambda aio --version v2.0.0

# Deploy perplexity worker
./deploy.py deploy --lambda perplexity
```

### Revert Lambda Functions

```bash
# Revert single lambda (interactive version selection)
./deploy.py revert --lambda orchestrator

# Revert to specific version
./deploy.py revert --lambda aio --target-version v20241201-123456-abc123

# Revert multiple lambdas
./deploy.py revert --multiple orchestrator perplexity aio

# Revert multiple to specific version
./deploy.py revert --multiple aim chatgpt --target-version v20241201-100000-def456
```

### View Deployment History

```bash
# Show recent deployment history
./deploy.py history
```

### View Current Configuration

```bash
# Display current configuration
./deploy.py config

# Initialize new configuration file
./deploy.py init-config
```

## Deployment Types

### Code Deployment (ZIP)
- **Used for**: `orchestrator`, `perplexity`
- **Process**: Creates ZIP file → Uploads to Lambda → Publishes version → Creates alias
- **Advantages**: Faster deployment, smaller package size
- **Use case**: Pure Python functions without heavy dependencies

### Container Deployment (ECR)
- **Used for**: `aio`, `aim`, `chatgpt`
- **Process**: Builds Docker image → Pushes to ECR → Updates Lambda → Publishes version → Creates alias
- **Advantages**: Full control over runtime environment, supports complex dependencies
- **Use case**: Functions with heavy dependencies (crawl4ai, playwright, etc.)

## Version Tagging

The script automatically generates version tags in the format:
```
v{YYYYMMDD-HHMMSS}-{git_hash}
```

Example: `v20241201-143022-a1b2c3d`

- **Timestamp**: Ensures chronological ordering
- **Git Hash**: Links deployment to specific code commit
- **Prefix**: `v` for easy identification

## Revert Process

1. **List Available Versions**: Shows all published versions and aliases
2. **Select Target**: Choose version number or alias name
3. **Update Function**: Points function to selected version
4. **Create Alias**: Creates/updates 'current' alias for tracking
5. **Record Action**: Logs revert in deployment history

## Error Handling

The script includes comprehensive error handling:

- **AWS Credential Issues**: Validates credentials on startup
- **Missing Files**: Checks for source paths and Dockerfiles
- **ECR Repository**: Creates repositories if they don't exist
- **Docker Build Failures**: Captures and displays build errors
- **Lambda Update Failures**: Provides detailed error messages
- **Network Issues**: Handles timeout and connectivity problems

## Monitoring and Logging

### Deployment History
All deployments are tracked in `deployment_config.json`:
```json
{
  "deployment_history": [
    {
      "lambda_name": "orchestrator",
      "version": "v20241201-143022-a1b2c3d",
      "deployment_type": "code",
      "timestamp": "2024-12-01T14:30:22.123456",
      "success": true
    }
  ]
}
```

### Console Output
The script provides colored, structured output:
- 🔵 **INFO**: General information
- 🟢 **SUCCESS**: Successful operations
- 🟡 **WARNING**: Non-critical issues
- 🔴 **ERROR**: Critical failures

## AWS Permissions Required

The deployment script requires the following AWS permissions:

### Lambda Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:UpdateFunctionCode",
        "lambda:PublishVersion",
        "lambda:CreateAlias",
        "lambda:UpdateAlias",
        "lambda:GetFunction",
        "lambda:ListVersionsByFunction",
        "lambda:ListAliases"
      ],
      "Resource": "arn:aws:lambda:*:*:function:bodhium-*"
    }
  ]
}
```

### ECR Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecr:GetAuthorizationToken",
        "ecr:BatchCheckLayerAvailability",
        "ecr:GetDownloadUrlForLayer",
        "ecr:BatchGetImage",
        "ecr:DescribeRepositories",
        "ecr:CreateRepository",
        "ecr:PutImage",
        "ecr:InitiateLayerUpload",
        "ecr:UploadLayerPart",
        "ecr:CompleteLayerUpload"
      ],
      "Resource": "*"
    }
  ]
}
```

### STS Permissions
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    }
  ]
}
```

## Troubleshooting

### Common Issues

1. **Docker Build Fails**
   ```bash
   # Check Docker is running
   docker ps
   
   # Check Dockerfile exists
   ls lambdas/workers/aio/Dockerfile
   ```

2. **ECR Login Fails**
   ```bash
   # Check AWS credentials
   aws sts get-caller-identity
   
   # Manual ECR login
   aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 127214200395.dkr.ecr.us-east-1.amazonaws.com
   ```

3. **Lambda Function Not Found**
   ```bash
   # List Lambda functions
   aws lambda list-functions --query 'Functions[?starts_with(FunctionName, `bodhium`)].FunctionName'
   ```

4. **Permission Denied**
   ```bash
   # Check IAM permissions
   aws iam get-user
   aws iam list-attached-user-policies --user-name YOUR_USERNAME
   ```

### Debug Mode

For detailed debugging, modify the script to enable debug logging:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Examples

### Complete Deployment Workflow

```bash
# 0. Initialize configuration (first time only)
./deploy.py init-config

# 1. Deploy all functions
./deploy.py deploy-all

# 2. Check deployment history
./deploy.py history

# 3. If something fails, revert specific function
./deploy.py revert --lambda aio

# 4. Deploy single function after fix
./deploy.py deploy --lambda aio
```

### Batch Operations

```bash
# Deploy only container-based workers
./deploy.py deploy --lambda aio
./deploy.py deploy --lambda aim  
./deploy.py deploy --lambda chatgpt

# Or revert multiple at once
./deploy.py revert --multiple aio aim chatgpt --target-version v20241201-120000-xyz789
```

### Version Management

```bash
# Deploy with semantic versioning
./deploy.py deploy-all --version v2.1.0

# Deploy hotfix
./deploy.py deploy --lambda orchestrator --version v2.1.1-hotfix

# Revert to stable version
./deploy.py revert --lambda orchestrator --target-version v2.1.0
```

## Best Practices

1. **Always test in staging first** before production deployment
2. **Use semantic versioning** for major releases
3. **Keep deployment history** for audit trails
4. **Monitor Lambda metrics** after deployment
5. **Have rollback plan** ready for critical deployments
6. **Use aliases** for traffic management
7. **Tag deployments** with meaningful descriptions
8. **Backup configuration** before major changes

## Integration with CI/CD

The script can be integrated into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Deploy Lambda Functions
  run: |
    python deploy.py deploy-all --version ${{ github.sha }}
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

## Support

For issues or questions:
1. Check the troubleshooting section
2. Review AWS CloudWatch logs
3. Examine deployment history
4. Verify AWS permissions
5. Test with single function deployment first

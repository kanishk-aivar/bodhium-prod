#!/bin/bash
# deploy-batch-scraper.sh

set -e

echo "🚀 Deploying Batch Scraper..."

# Build and push Docker image

echo "🔐 Logging into ECR..."
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 127214200395.dkr.ecr.us-east-1.amazonaws.com

echo "📦 Building Docker image..."
docker build --platform linux/amd64 --provenance false -t bodhium-batch-scraper .

echo "🏷️ Tagging and pushing image..."
docker tag bodhium-batch-scraper:latest 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest
docker push 127214200395.dkr.ecr.us-east-1.amazonaws.com/bodhium-batch-scraper:latest

echo "✅ Deployment complete!"
echo "📋 Next steps:"
echo "   1. Create/update AWS Batch resources (compute environment, job queue, job definition)"
echo "   2. Update Web Scraper Lambda environment variables"
echo "   3. Test with a sample job"
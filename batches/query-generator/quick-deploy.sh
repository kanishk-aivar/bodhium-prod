#!/bin/bash

# Query Generator - Quick Deploy Script
# Simplified deployment for development and testing

set -e

# Configuration
ECR_REGISTRY="127214200395.dkr.ecr.us-east-1.amazonaws.com"
ECR_REPOSITORY="dev-querygen"
AWS_REGION="us-east-1"

# Generate simple tag
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "dev")
IMAGE_TAG="${TIMESTAMP}-${GIT_COMMIT}"

echo "🚀 Quick Deploy: ${ECR_REPOSITORY}:${IMAGE_TAG}"

# ECR Login
echo "🔐 ECR Login..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}

# Build and Tag
echo "🔨 Building..."
docker build --platform linux/amd64 --provenance false -t dev-querygen .

echo "🏷️  Tagging..."
docker tag dev-querygen:latest ${ECR_REGISTRY}/${ECR_REPOSITORY}:latest
docker tag dev-querygen:latest ${ECR_REGISTRY}/${ECR_REPOSITORY}:${IMAGE_TAG}

# Push
echo "📤 Pushing..."
docker push ${ECR_REGISTRY}/${ECR_REPOSITORY}:latest
docker push ${ECR_REGISTRY}/${ECR_REPOSITORY}:${IMAGE_TAG}

# Cleanup
echo "🧹 Cleanup..."
docker rmi dev-querygen:latest 2>/dev/null || true
docker rmi ${ECR_REGISTRY}/${ECR_REPOSITORY}:latest 2>/dev/null || true
docker rmi ${ECR_REGISTRY}/${ECR_REPOSITORY}:${IMAGE_TAG} 2>/dev/null || true

echo "✅ Deployed: ${ECR_REGISTRY}/${ECR_REPOSITORY}:${IMAGE_TAG}"
echo "📅 Tag: ${IMAGE_TAG}"

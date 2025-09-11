#!/bin/bash

# Configuration
REGION=us-east-1  # Change to your preferred region
REPO_NAME=appv3
IMAGE_NAME=appv3

# Get AWS account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

if [ $? -ne 0 ]; then
    echo "❌ Error: AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

echo "✅ AWS Account ID: $ACCOUNT_ID"

# Build the image
echo "Building image..."
docker build -t $IMAGE_NAME .

if [ $? -ne 0 ]; then
    echo "❌ Error: Failed to build image"
    exit 1
fi

# Create ECR repository if it doesn't exist
echo "🏗️ Creating ECR repository..."
aws ecr create-repository --repository-name $REPO_NAME --region $REGION 2>/dev/null || echo "Repository already exists or error occurred"

# Get login token and login to ECR
echo "🔐 Logging in to ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

if [ $? -ne 0 ]; then
    echo "❌ Failed to login to ECR"
    exit 1
fi

# Tag the image
ECR_URI=$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPO_NAME:latest
echo "🏷️ Tagging image as $ECR_URI"
docker tag $IMAGE_NAME:latest $ECR_URI

# Push the image
echo "🚀 Pushing image to ECR..."
docker push $ECR_URI

if [ $? -eq 0 ]; then
    echo "✅ Successfully pushed to ECR!"
    echo "📝 ECR URI: $ECR_URI"
    echo ""
    echo "🐳 To pull and run this image:"
    echo "   docker pull $ECR_URI"
    echo "   docker run -p 80:80 --env-file .env $ECR_URI"
else
    echo "❌ Failed to push to ECR"
    exit 1
fi

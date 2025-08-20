#!/bin/bash

# Bodhium Query Generator Lambda Deployment Script
# Builds and deploys Docker image to AWS ECR

set -e  # Exit on any error

# Configuration
ECR_REGISTRY="127214200395.dkr.ecr.us-east-1.amazonaws.com"
AWS_REGION="us-east-1"
SERVICE_NAME="Query Generator"
ECR_REPO="bodhium-query-ui"
BUILD_DIR="."

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to login to ECR
ecr_login() {
    log_info "Logging into AWS ECR..."
    if aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY; then
        log_success "Successfully logged into ECR"
    else
        log_error "Failed to login to ECR"
        exit 1
    fi
}

# Function to build and push the service
deploy_query_generator() {
    log_info "=== Deploying $SERVICE_NAME ==="
    log_info "Build directory: $BUILD_DIR"
    log_info "ECR repository: $ECR_REPO"
    
    # Check if build directory exists
    if [ ! -d "$BUILD_DIR" ]; then
        log_error "Build directory $BUILD_DIR does not exist"
        exit 1
    fi
    
    # Check if Dockerfile exists
    if [ ! -f "Dockerfile" ]; then
        log_error "Dockerfile not found in current directory"
        exit 1
    fi
    
    # Check if lambda_function.py exists
    if [ ! -f "lambda_function.py" ]; then
        log_error "lambda_function.py not found in current directory"
        exit 1
    fi
    
    # Check if requirements.txt exists
    if [ ! -f "requirements.txt" ]; then
        log_error "requirements.txt not found in current directory"
        exit 1
    fi
    
    log_info "Building Docker image for $SERVICE_NAME..."
    if docker build --platform linux/amd64 --provenance false -t $ECR_REPO .; then
        log_success "Successfully built $SERVICE_NAME image"
    else
        log_error "Failed to build $SERVICE_NAME image"
        exit 1
    fi
    
    log_info "Tagging image for ECR..."
    if docker tag $ECR_REPO:latest $ECR_REGISTRY/$ECR_REPO:latest; then
        log_success "Successfully tagged $SERVICE_NAME image"
    else
        log_error "Failed to tag $SERVICE_NAME image"
        exit 1
    fi
    
    log_info "Pushing image to ECR..."
    if docker push $ECR_REGISTRY/$ECR_REPO:latest; then
        log_success "Successfully pushed $SERVICE_NAME to ECR"
    else
        log_error "Failed to push $SERVICE_NAME to ECR"
        exit 1
    fi
    
    log_success "$SERVICE_NAME deployment completed successfully!"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Deploy Bodhium Query Generator Lambda to AWS ECR"
    echo ""
    echo "Options:"
    echo "  deploy        Deploy the Query Generator Lambda (default)"
    echo "  build         Build Docker image only (no ECR push)"
    echo "  help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0            # Deploy to ECR (default action)"
    echo "  $0 deploy     # Deploy to ECR"
    echo "  $0 build      # Build image only"
    echo ""
    echo "Prerequisites:"
    echo "  - Docker installed and running"
    echo "  - AWS CLI installed and configured"
    echo "  - Valid AWS credentials with ECR access"
}

# Function to build image only (no ECR operations)
build_only() {
    log_info "=== Building $SERVICE_NAME (Local Only) ==="
    
    # Check if Dockerfile exists
    if [ ! -f "Dockerfile" ]; then
        log_error "Dockerfile not found in current directory"
        exit 1
    fi
    
    # Check if lambda_function.py exists
    if [ ! -f "lambda_function.py" ]; then
        log_error "lambda_function.py not found in current directory"
        exit 1
    fi
    
    # Check if requirements.txt exists
    if [ ! -f "requirements.txt" ]; then
        log_error "requirements.txt not found in current directory"
        exit 1
    fi
    
    log_info "Building Docker image for $SERVICE_NAME..."
    if docker build --platform linux/amd64 --provenance false -t $ECR_REPO .; then
        log_success "Successfully built $SERVICE_NAME image locally"
        log_info "Image tagged as: $ECR_REPO:latest"
    else
        log_error "Failed to build $SERVICE_NAME image"
        exit 1
    fi
}

# Function to check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Docker is installed and running
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    # Check if AWS CLI is installed (only for deploy operations)
    if [ "${1:-deploy}" = "deploy" ]; then
        if ! command -v aws &> /dev/null; then
            log_error "AWS CLI is not installed or not in PATH"
            exit 1
        fi
        
        # Check AWS credentials
        if ! aws sts get-caller-identity &> /dev/null; then
            log_error "AWS credentials not configured or invalid"
            exit 1
        fi
    fi
    
    log_success "All prerequisites satisfied"
}

# Function to show deployment summary
show_deployment_info() {
    echo ""
    log_info "=== Deployment Information ==="
    log_info "Service: $SERVICE_NAME"
    log_info "ECR Registry: $ECR_REGISTRY"
    log_info "ECR Repository: $ECR_REPO"
    log_info "AWS Region: $AWS_REGION"
    log_info "Build Directory: $BUILD_DIR"
    echo ""
}

# Main script logic
main() {
    echo "========================================================"
    echo "     Bodhium Query Generator Lambda Deployment Script"
    echo "========================================================"
    echo ""
    
    # Parse command line arguments
    ACTION="${1:-deploy}"
    
    # Check prerequisites based on action
    check_prerequisites "$ACTION"
    echo ""
    
    # Show deployment info
    show_deployment_info
    
    case "$ACTION" in
        "deploy"|"")
            ecr_login
            echo ""
            deploy_query_generator
            ;;
        "build")
            build_only
            ;;
        "help"|"--help"|"-h")
            show_usage
            ;;
        *)
            log_error "Invalid option: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
    
    echo ""
    log_success "Deployment script completed!"
}

# Run main function with all arguments
main "$@"

#!/bin/bash

# Bodhium Lambda Workers Deployment Script
# Builds and deploys Docker images to AWS ECR

set -e  # Exit on any error

# Configuration
ECR_REGISTRY="127214200395.dkr.ecr.us-east-1.amazonaws.com"
AWS_REGION="us-east-1"

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

# Function to build and push a service
deploy_service() {
    local service_name=$1
    local ecr_repo=$2
    local build_dir=$3
    
    log_info "Deploying $service_name..."
    log_info "Build directory: $build_dir"
    log_info "ECR repository: $ecr_repo"
    
    # Check if build directory exists
    if [ ! -d "$build_dir" ]; then
        log_error "Build directory $build_dir does not exist"
        return 1
    fi
    
    # Change to build directory
    cd "$build_dir"
    
    # Check if Dockerfile exists
    if [ ! -f "Dockerfile" ]; then
        log_error "Dockerfile not found in $build_dir"
        cd - > /dev/null
        return 1
    fi
    
    log_info "Building Docker image for $service_name..."
    if docker build --platform linux/amd64 --provenance false -t $ecr_repo .; then
        log_success "Successfully built $service_name image"
    else
        log_error "Failed to build $service_name image"
        cd - > /dev/null
        return 1
    fi
    
    log_info "Tagging image for ECR..."
    if docker tag $ecr_repo:latest $ECR_REGISTRY/$ecr_repo:latest; then
        log_success "Successfully tagged $service_name image"
    else
        log_error "Failed to tag $service_name image"
        cd - > /dev/null
        return 1
    fi
    
    log_info "Pushing image to ECR..."
    if docker push $ECR_REGISTRY/$ecr_repo:latest; then
        log_success "Successfully pushed $service_name to ECR"
    else
        log_error "Failed to push $service_name to ECR"
        cd - > /dev/null
        return 1
    fi
    
    # Return to original directory
    cd - > /dev/null
    
    log_success "$service_name deployment completed successfully!"
}

# Function to deploy AI Mode (AIM)
deploy_ai_mode() {
    log_info "=== Deploying AI Mode Worker ==="
    deploy_service "AI Mode" "bodhium-aimode-brightdata" "aim"
}

# Function to deploy ChatGPT
deploy_chatgpt() {
    log_info "=== Deploying ChatGPT Worker ==="
    deploy_service "ChatGPT" "bodhium-gpt-v1" "ChatGPT"
}

# Function to deploy AI Overview (AIO)
deploy_ai_overview() {
    log_info "=== Deploying AI Overview Worker ==="
    deploy_service "AI Overview" "bodhium-ai-overview" "aio"
}

# Function to deploy all services
deploy_all() {
    log_info "=== Deploying All Lambda Workers ==="
    
    ecr_login
    
    log_info "Starting deployment of all services..."
    
    deploy_ai_mode
    echo ""
    
    deploy_chatgpt
    echo ""
    
    deploy_ai_overview
    echo ""
    
    log_success "All services deployed successfully!"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Deploy Bodhium Lambda Workers to AWS ECR"
    echo ""
    echo "Options:"
    echo "  all           Deploy all workers (AI Mode, ChatGPT, AI Overview)"
    echo "  ai-mode       Deploy AI Mode worker only"
    echo "  chatgpt       Deploy ChatGPT worker only"
    echo "  ai-overview   Deploy AI Overview worker only"
    echo "  help          Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all                # Deploy all workers"
    echo "  $0 ai-mode           # Deploy only AI Mode worker"
    echo "  $0 chatgpt           # Deploy only ChatGPT worker"
    echo "  $0 ai-overview       # Deploy only AI Overview worker"
    echo ""
    echo "Note: Orchestrator and Perplexity workers are deployed manually via AWS Console"
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
    
    # Check if AWS CLI is installed
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed or not in PATH"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or invalid"
        exit 1
    fi
    
    log_success "All prerequisites satisfied"
}

# Main script logic
main() {
    echo "================================================"
    echo "     Bodhium Lambda Workers Deployment Script"
    echo "================================================"
    echo ""
    
    # Check prerequisites
    check_prerequisites
    echo ""
    
    # Parse command line arguments
    case "${1:-help}" in
        "all")
            deploy_all
            ;;
        "ai-mode")
            ecr_login
            echo ""
            deploy_ai_mode
            ;;
        "chatgpt")
            ecr_login
            echo ""
            deploy_chatgpt
            ;;
        "ai-overview")
            ecr_login
            echo ""
            deploy_ai_overview
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

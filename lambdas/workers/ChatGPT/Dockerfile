# Use Microsoft Playwright base image with all dependencies
FROM mcr.microsoft.com/playwright/python:v1.53.0-noble

# Set working directory
WORKDIR /app

# Set environment variables for crawl4ai and Python output EARLY
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV CRAWL4AI_DB_PATH=/tmp/.crawl4ai
ENV CRAWL4AI_CACHE_DIR=/tmp/.crawl4ai_cache
ENV CRAWL4AI_BASE_DIRECTORY=/tmp

# Update system packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with exact versions
RUN pip install --no-cache-dir -r requirements.txt

# Install playwright-stealth for anti-detection
RUN pip install playwright-stealth==1.0.5

# Install AWS Lambda Runtime Interface Client (critical!)
RUN pip install awslambdaric==2.2.0

# Install boto3 for S3 operations
RUN pip install boto3==1.35.0

# CRITICAL: Install browsers explicitly with system dependencies
RUN playwright install --with-deps chromium

# Switch to root temporarily to create directories
USER root

# Create necessary directories and set permissions
RUN mkdir -p /tmp/.crawl4ai /tmp/.crawl4ai_cache && \
    chown -R pwuser:pwuser /tmp/.crawl4ai /tmp/.crawl4ai_cache

# Use the existing pwuser from Playwright image
USER pwuser

# Copy the main function file (matching your screenshot filename)
COPY --chown=pwuser:pwuser brightdata.py .

# CRITICAL: Use ENTRYPOINT for Lambda runtime interface
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["brightdata.lambda_handler"]

# For local testing, you can override with:
# docker run -it <image> python brightdata.py

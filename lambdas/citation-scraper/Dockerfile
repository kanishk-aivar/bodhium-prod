# Dockerfile for AWS Lambda container image running Crawl4AI 0.7.2 + Playwright 1.53.0
FROM mcr.microsoft.com/playwright/python:v1.53.0-noble

WORKDIR /app

# Essentials
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONIOENCODING=utf-8
ENV DEBIAN_FRONTEND=noninteractive

# Crawl4AI runtime directories
ENV CRAWL4AI_DB_PATH=/tmp/.crawl4ai
ENV CRAWL4AI_CACHE_DIR=/tmp/.crawl4ai_cache
ENV CRAWL4AI_BASE_DIRECTORY=/tmp
ENV HOME=/tmp

# AWS envs (override at deploy time)
ENV AWS_DEFAULT_REGION=us-east-1
ENV S3_BUCKET_NAME=
ENV S3_PREFIX=sources/

# Playwright settings
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

# Minimal build tools
RUN apt-get update && apt-get install -y \
    gcc g++ curl wget ca-certificates zip unzip \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

# Requirements first for better cache
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Ensure Chromium installed with system deps
RUN playwright install --with-deps chromium

# Pre-create writable /tmp dirs and set ownership
RUN mkdir -p /tmp/.crawl4ai /tmp/.crawl4ai_cache /tmp/.crawl4ai_user_data \
             /tmp/parallel_results /tmp/crawler_logs /tmp/chatgpt_responses \
  && chmod 777 /tmp/.crawl4ai /tmp/.crawl4ai_cache /tmp/.crawl4ai_user_data \
               /tmp/parallel_results /tmp/crawler_logs /tmp/chatgpt_responses

# Use non-root user provided by Playwright image
USER pwuser

# Copy Lambda code
COPY --chown=pwuser:pwuser parallel_crawler_lambda.py .

# Lambda runtime interface entrypoint
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["parallel_crawler_lambda.lambda_handler"]

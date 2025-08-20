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

# Update system packages and install dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    wget \
    gnupg \
    ca-certificates \
    libpq-dev \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    libjpeg-dev \
    libpng-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies with exact versions (all packages from requirements.txt)
RUN pip install --no-cache-dir -r requirements.txt

# CRITICAL: Install browsers explicitly with system dependencies
RUN playwright install --with-deps chromium

# Create necessary directories and set permissions (as root)
RUN mkdir -p /tmp/.crawl4ai /tmp/.crawl4ai_cache /tmp/screenshots && \
    chmod 777 /tmp/.crawl4ai /tmp/.crawl4ai_cache /tmp/screenshots

# Switch to pwuser for security
USER pwuser

# Copy the main function file (matching your screenshot filename)
COPY --chown=pwuser:pwuser crawlforai.py .

# CRITICAL: Use ENTRYPOINT for Lambda runtime interface
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["crawlforai.lambda_handler"]

# For local testing, you can override with:
# docker run -it <image> python brightdata.py

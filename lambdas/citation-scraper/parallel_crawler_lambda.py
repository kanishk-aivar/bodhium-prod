import asyncio
import json
import os
import time
import gc
import psutil
import boto3
import uuid
import logging
import traceback
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse, urljoin
from base64 import b64decode

# ========= Logging =========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)
logger.info("🚀 Parallel crawler Lambda initializing...")

# ========= Pre-init directories (/tmp) =========
os.environ.setdefault('PYTHONUNBUFFERED', '1')
os.environ.setdefault('CRAWL4AI_DB_PATH', '/tmp/.crawl4ai')
os.environ.setdefault('CRAWL4AI_CACHE_DIR', '/tmp/.crawl4ai_cache')
os.environ.setdefault('CRAWL4AI_BASE_DIRECTORY', '/tmp')
os.environ.setdefault('HOME', '/tmp')

for d in ['/tmp/.crawl4ai', '/tmp/.crawl4ai_cache', '/tmp/.crawl4ai_user_data', '/tmp/parallel_results']:
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass

# ========= Defensive imports =========
AsyncWebCrawler = None
BrowserConfig = None
CrawlerRunConfig = None
CacheMode = None
RateLimiter = None
MemoryAdaptiveDispatcher = None
AsyncUrlSeeder = None
SeedingConfig = None

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, RateLimiter
    from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
    from crawl4ai import AsyncUrlSeeder, SeedingConfig
    logger.info("✅ crawl4ai components imported")
except Exception as e:
    logger.error(f"❌ Failed to import crawl4ai: {e}")

# ========= Env parsing helpers =========
def _get_env_str(name: str, default: str = "") -> str:
    val = os.environ.get(name)
    if val is None:
        return default
    return str(val).strip()

def _get_env_float(name: str, default: float, min_val: Optional[float] = None, max_val: Optional[float] = None) -> float:
    raw = os.environ.get(name)
    try:
        v = float(raw) if raw is not None else float(default)
    except Exception:
        v = float(default)
    if min_val is not None:
        v = max(min_val, v)
    if max_val is not None:
        v = min(max_val, v)
    return v

def _get_env_int(name: str, default: int, min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
    raw = os.environ.get(name)
    try:
        v = int(raw) if raw is not None else int(default)
    except Exception:
        v = int(default)
    if min_val is not None:
        v = max(min_val, v)
    if max_val is not None:
        v = min(max_val, v)
    return v

def _normalize_prefix(prefix: str) -> str:
    if not prefix:
        return ""
    return prefix if prefix.endswith('/') else prefix + '/'

def _safe(s: str, max_len: int = 120) -> str:
    if not s:
        return "na"
    allowed = "".join(ch for ch in s if ch.isalnum() or ch in (' ', '-', '_'))
    cleaned = "_".join(allowed.strip().split())
    return cleaned[:max_len] if cleaned else "na"

def filter_youtube_urls(urls: List[str]) -> List[str]:
    """
    Remove YouTube URLs from the list of URLs to crawl.
    
    Args:
        urls: List of URLs to filter
        
    Returns:
        List of URLs with YouTube URLs removed
    """
    filtered_urls = []
    youtube_urls = []
    
    for url in urls:
        if url and isinstance(url, str):
            # Check for various YouTube domain patterns
            if re.search(r'(youtube\.com|youtu\.be|m\.youtube\.com|www\.youtube\.com)', url.lower()):
                youtube_urls.append(url)
                logger.info(f"🚫 Filtered out YouTube URL: {url}")
            else:
                filtered_urls.append(url)
    
    if youtube_urls:
        logger.info(f"🔍 Filtered out {len(youtube_urls)} YouTube URLs from {len(urls)} total URLs")
    else:
        logger.info(f"✅ No YouTube URLs found in {len(urls)} URLs")
    
    return filtered_urls

def extract_query_text_for_s3_path(content: Any) -> str:
    """
    Standardized function to extract query text for S3 path generation.
    This ensures all lambdas use the same logic for consistent S3 paths.
    
    Args:
        content: The content to extract query text from
    
    Returns:
        Sanitized query text suitable for S3 path (no length limits)
    """
    query_text = "unknown_query"
    
    if isinstance(content, dict) and 'query' in content:
        query_text = content['query']
    elif isinstance(content, str) and len(content) > 0:
        # Extract first few words as query text (same logic as worker lambdas)
        query_text = content.split('\n')[0] if '\n' in content else content
    
    # No length limits - preserve the full query text
    query_text_safe = re.sub(r'[^a-zA-Z0-9\s]', '', query_text).replace(' ', '_').strip()
    
    return query_text_safe

def get_domain_name(url: str) -> str:
    """Extract domain name from URL"""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if domain.startswith('www.'):
        domain = domain[4:]
    if ':' in domain:
        domain = domain.split(':')[0]
    logger.debug(f"🔍 Extracted domain '{domain}' from URL: {url}")
    return domain

async def discover_urls_for_parallel_crawl(root_url: str) -> List[str]:
    """
    Comprehensive URL discovery using multiple methods from batch-scrapper
    """
    logger.info(f"🔍 STARTING URL DISCOVERY: {root_url}")
    domain = get_domain_name(root_url)
    discovered_urls = []
    
    try:
        max_urls = _get_env_int("MAX_URLS", 100)
        logger.info(f"🔧 Configuring URL seeder with max_urls={max_urls}")
        
        # Method 1: Try sitemap discovery
        logger.info("🔄 Trying URL Seeding with sitemap...")
        async with AsyncUrlSeeder() as seeder:
            sitemap_config = SeedingConfig(
                source="sitemap",
                extract_head=True,
                live_check=False,
                max_urls=max_urls,
                verbose=False,
                force=True
            )
            try:
                sitemap_urls = await seeder.urls(domain, sitemap_config)
                if sitemap_urls:
                    logger.info(f"✅ Found {len(sitemap_urls)} URLs via sitemap")
                    for url_info in sitemap_urls:
                        discovered_urls.append(url_info['url'])
                else:
                    logger.warning("⚠️ No URLs found via sitemap")
            except Exception as e:
                logger.warning(f"⚠️ URL seeding with sitemap failed: {e}")

        # Method 2: Try Common Crawl as backup
        if not discovered_urls:
            logger.info("🔄 Trying Common Crawl as backup...")
            async with AsyncUrlSeeder() as seeder:
                cc_config = SeedingConfig(
                    source="cc",
                    extract_head=False,
                    max_urls=max_urls,
                    verbose=False
                )
                try:
                    cc_urls = await seeder.urls(domain, cc_config)
                    if cc_urls:
                        logger.info(f"✅ Found {len(cc_urls)} URLs via Common Crawl")
                        for url_info in cc_urls:
                            discovered_urls.append(url_info['url'])
                    else:
                        logger.warning("⚠️ No URLs found via Common Crawl")
                except Exception as e:
                    logger.warning(f"⚠️ Common Crawl failed: {e}")

        # Method 3: Manual fallback with common paths
        if not discovered_urls:
            logger.info("🔄 Using manual URL discovery as fallback...")
            common_paths = [
                "", "/collections/all", "/collections/skincare", "/collections/haircare", "/collections/bundles",
                "/pages/about", "/pages/ingredients", "/blogs/news", "/search", "/products", "/shop", "/categories",
                "/new-arrivals", "/sale"
            ]
            for path in common_paths:
                full_url = urljoin(root_url, path)
                discovered_urls.append(full_url)
            logger.info(f"✅ Added {len(discovered_urls)} URLs via manual discovery")

        # Validate and deduplicate URLs
        seen_urls = set()
        unique_urls = []
        base_domain = domain
        
        for url in discovered_urls:
            if not url.startswith('http'):
                url = f"https://{url}"
            
            try:
                url_domain = urlparse(url).netloc.replace('www.', '')
                if base_domain in url_domain and url not in seen_urls:
                    seen_urls.add(url)
                    unique_urls.append(url)
            except Exception:
                continue
        
        logger.info(f"🎯 Discovered {len(unique_urls)} unique URLs from domain {domain}")
        return unique_urls[:max_urls]
        
    except Exception as e:
        logger.error(f"❌ Error discovering URLs: {str(e)}")
        return [root_url]  # Fallback to root URL

def validate_and_deduplicate_urls(urls: List[str], domain: str) -> List[str]:
    """Validate URLs and remove duplicates"""
    valid_urls = []
    seen_urls = set()
    
    for url in urls:
        if not url.startswith('http'):
            url = f"https://{url}"
        
        try:
            url_domain = urlparse(url).netloc.replace('www.', '')
            if domain in url_domain and url not in seen_urls:
                seen_urls.add(url)
                valid_urls.append(url)
        except Exception:
            continue
    
    logger.info(f"✅ Validated {len(valid_urls)} unique URLs for domain {domain}")
    return valid_urls

# ========= S3 results helper =========
class S3Results:
    """
    Structure:
      s3://{bucket}/brand_name/job_id/product_name/query_text/mode/citation_{i}.md
    """
    def __init__(self, job_id: str, mode: str, query: str, product_id: str = None, 
                 brand_name: str = None, product_name: str = None):
        self.job_id = job_id
        self.product_id = product_id
        self.query = query
        self.client = boto3.client('s3')
        self.bucket = _get_env_str('S3_BUCKET', '')
        
        # Use passed context if available, otherwise fall back to database lookup
        self.brand_name = brand_name if brand_name else self._get_brand_name_from_db(job_id)
        self.product_name = product_name if product_name else self._get_product_name_from_db(product_id) if product_id else 'Unknown Product'
        
        # Sanitize names for S3 path (replace spaces with underscores, remove special chars)
        brand_name_safe = re.sub(r'[^a-zA-Z0-9\s]', '', self.brand_name).replace(' ', '_').strip()
        product_name_safe = re.sub(r'[^a-zA-Z0-9\s]', '', self.product_name).replace(' ', '_').strip()
        
        # Use standardized query text extraction for consistent S3 paths
        query_text_safe = extract_query_text_for_s3_path(self.query)
        
        # Create new S3 prefix following the required structure: brand_name/job_id/product_name/query_text/mode/
        self.prefix = f"{brand_name_safe}/{job_id}/{product_name_safe}/{query_text_safe}/{mode}/"
        
        logger.info(f"📦 S3 -> bucket='{self.bucket}', prefix='{self.prefix}'")
        logger.info(f"🔍 Context -> brand_name='{self.brand_name}', product_name='{self.product_name}', mode='{mode}'")
        logger.info(f"📝 Query Processing -> original='{self.query}', safe='{query_text_safe}'")
        if self.bucket:
            try:
                self.client.head_bucket(Bucket=self.bucket)
                logger.info(f"✅ S3 bucket '{self.bucket}' is accessible")
            except Exception as e:
                logger.error(f"❌ S3 bucket '{self.bucket}' not accessible: {e}")
    
    def _get_brand_name_from_db(self, job_id: str) -> str:
        """Fetch brand name from scrapejobs table using job_id"""
        try:
            # Import here to avoid circular imports
            import psycopg2
            from botocore.exceptions import ClientError
            
            # Get database connection details from environment
            secret_name = os.environ.get("RDS_SECRET_NAME", "dev/rds")
            region_name = os.environ.get("secret_region", "us-east-1")
            
            session = boto3.session.Session()
            client = session.client(service_name='secretsmanager', region_name=region_name)
            
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(get_secret_value_response['SecretString'])
            
            conn = psycopg2.connect(
                host=secret['DB_HOST'],
                database=secret['DB_NAME'],
                user=secret['DB_USER'],
                password=secret['DB_PASSWORD'],
                port=secret['DB_PORT']
            )
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT brand_name FROM scrapejobs WHERE job_id = %s",
                (job_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0]:
                brand_name = result[0]
                logger.info(f"Found brand name: {brand_name} for job_id: {job_id}")
                return brand_name
            else:
                logger.warning(f"Brand name not found for job_id: {job_id}")
                return 'Unknown Brand'
                
        except Exception as e:
            logger.error(f"Error fetching brand name for job_id {job_id}: {e}")
            return 'Unknown Brand'
    
    def _get_product_name_from_db(self, product_id: str) -> str:
        """Fetch product name from products table"""
        try:
            # Import here to avoid circular imports
            import psycopg2
            from botocore.exceptions import ClientError
            
            # Get database connection details from environment
            secret_name = os.environ.get("RDS_SECRET_NAME", "dev/rds")
            region_name = os.environ.get("secret_region", "us-east-1")
            
            session = boto3.session.Session()
            client = session.client(service_name='secretsmanager', region_name=region_name)
            
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = json.loads(get_secret_value_response['SecretString'])
            
            conn = psycopg2.connect(
                host=secret['DB_HOST'],
                database=secret['DB_NAME'],
                user=secret['DB_USER'],
                password=secret['DB_PASSWORD'],
                port=secret['DB_PORT']
            )
            
            cursor = conn.cursor()
            cursor.execute(
                "SELECT product_data FROM products WHERE product_id = %s",
                (product_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0]:
                product_data = result[0]
                # Extract productname from JSON data
                product_name = product_data.get('productname', 'Unknown Product')
                logger.info(f"Found product name: {product_name} for product_id: {product_id}")
                return product_name
            else:
                logger.warning(f"Product not found for product_id: {product_id}")
                return 'Unknown Product'
                
        except Exception as e:
            logger.error(f"Error fetching product name for product_id {product_id}: {e}")
            return 'Unknown Product'

    def save_citation_markdown(self, content: str, idx: int) -> Optional[str]:
        if not self.bucket:
            logger.warning("⚠️ No S3_BUCKET set; skipping save.")
            return None
        key = f"{self.prefix}citation_{idx}.md"
        header = (
            f"# Citation {idx}\n\n"
            f"**Saved At:** {datetime.now(timezone.utc).isoformat()}\n\n"
            f"**Content Length:** {len(content or '')} characters\n\n"
            f"**Job ID:** {self.job_id}\n\n"
            "---\n\n"
        )
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=(header + (content or "")).encode('utf-8'),
                ContentType='text/markdown'
            )
            logger.info(f"✅ Saved citation to s3://{self.bucket}/{key}")
            return key
        except Exception as e:
            logger.error(f"❌ Failed to save citation to S3: {e}")
            logger.error(traceback.format_exc())
            return None

    def save_citations_json(self, urls: List[str]) -> Optional[str]:
        """Save citations.json with simple format for adhoc-results to read"""
        if not self.bucket:
            return None
        key = f"{self.prefix}citations.json"
        citations_data = {
            "citations": urls
        }
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(citations_data, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"✅ Saved citations.json to s3://{self.bucket}/{key}")
            return key
        except Exception as e:
            logger.error(f"❌ Failed to save citations.json: {e}")
            return None

    def save_session_summary(self, meta: Dict[str, Any]) -> Optional[str]:
        if not self.bucket:
            return None
        key = f"{self.prefix}session_summary.json"
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(meta, indent=2, default=str).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"✅ Saved session summary to s3://{self.bucket}/{key}")
            return key
        except Exception as e:
            logger.error(f"❌ Failed to save session summary: {e}")
            return None

    def list_results(self) -> Dict[str, Any]:
        if not self.bucket:
            return {"note": "No bucket configured"}
        try:
            resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
            files = resp.get('Contents', []) or []
            total_size = sum(o.get('Size', 0) for o in files)
            return {
                "total_files": len(files),
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "s3_prefix": f"s3://{self.bucket}/{self.prefix}",
                "files": [o['Key'] for o in files],
            }
        except Exception as e:
            logger.error(f"❌ Failed to list S3 results: {e}")
            return {"error": str(e), "bucket_attempted": self.bucket, "prefix_attempted": self.prefix}

# ========= Config builders (env-driven) =========
def create_browser_config() -> BrowserConfig:
    # Use the same browser configuration as the working batch-scrapper
    args = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-features=VizDisplayCompositor",
        "--disable-extensions",
        "--disable-plugins",
        "--disable-images",  # Speed up loading
        "--disable-javascript",  # For basic content extraction
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    logger.info(f"🔧 Browser args count: {len(args)}")
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=False,  # Reduced verbosity for stability
        use_persistent_context=False,
        extra_args=args,
        viewport_width=1920,
        viewport_height=1080,
        user_data_dir="/tmp/.crawl4ai_user_data",
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        },
    )

def create_dispatcher() -> MemoryAdaptiveDispatcher:
    # Sequential processing for maximum stability with real URLs
    max_session_permit = _get_env_int("MAX_CONCURRENT", 1, min_val=1, max_val=100)  # Single session for stability
    if max_session_permit == 1:  # If MAX_CONCURRENT wasn't set (using default)
        max_session_permit = _get_env_int("MAX_SESSION_PERMIT", 1, min_val=1, max_val=100)  # Single session
    
    # Very lenient memory settings for local testing
    mem_threshold = _get_env_float("MEMORY_THRESHOLD_PERCENT", 95.0, min_val=50.0, max_val=95.0)  # Very high threshold
    mem_wait = _get_env_float("MEMORY_WAIT_TIMEOUT", 5.0, min_val=5.0, max_val=600.0)  # Short wait time

    # Conservative retry settings
    base_delay_min = _get_env_float("BASE_DELAY_MIN", 0.5, min_val=0.0, max_val=30.0)  # Reasonable delays
    base_delay_max = _get_env_float("BASE_DELAY_MAX", 2.0, min_val=base_delay_min, max_val=120.0)  # Reasonable delays
    max_retries = _get_env_int("MAX_RETRIES", 3, min_val=0, max_val=10)  # Moderate retries

    logger.info(
        f"🎛️ Single-session Dispatcher cfg -> max_session_permit={max_session_permit}, "
        f"mem_threshold={mem_threshold}%, mem_wait={mem_wait}s, "
        f"delay=({base_delay_min},{base_delay_max}), retries={max_retries}"
    )

    return MemoryAdaptiveDispatcher(
        memory_threshold_percent=mem_threshold,
        max_session_permit=max_session_permit,
        memory_wait_timeout=mem_wait,
        rate_limiter=RateLimiter(
            base_delay=(base_delay_min, base_delay_max),
            max_delay=_get_env_float("RATE_MAX_DELAY", 30.0, 1.0, 600.0),  # Reduced max delay
            max_retries=max_retries,
            rate_limit_codes=[429, 503, 502, 504, 500, 520, 521, 522, 524],
        ),
    )

def create_crawler_config() -> CrawlerRunConfig:
    # Use the same crawler configuration as the working batch-scrapper
    page_timeout = _get_env_int("PAGE_TIMEOUT", 60000, min_val=5000, max_val=240000)  # 60s timeout
    word_thresh = _get_env_int("WORD_COUNT_THRESHOLD", 10, min_val=0, max_val=10000)  # Low threshold
    exclude_external = _get_env_str("EXCLUDE_EXTERNAL_LINKS", "true").lower() in ("1", "true", "yes")
    
    logger.info(f"🔧 Batch-scrapper compatible config -> timeout={page_timeout}ms, word_threshold={word_thresh}")
    
    return CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        screenshot=True,  # Enable screenshots like batch-scrapper
        word_count_threshold=word_thresh,
        only_text=False,  # Get full content including HTML structure
        process_iframes=True,  # Enable iframe processing like batch-scrapper
        wait_for_images=True,  # Wait for images like batch-scrapper
        page_timeout=page_timeout,
        exclude_external_links=exclude_external,
        excluded_tags=['script', 'style', 'nav', 'footer', 'header'],  # Same as batch-scrapper
        remove_overlay_elements=True,  # Remove overlays that might block content
        verbose=False
    )


# ========= Helper functions =========
async def _process_successful_result(result, s3: S3Results, successes: int, saved_files: List[str], 
                                   job_id: str, mode: str, query: str):
    """Process a successful crawl result - matching batch-scrapper approach"""
    # Extract content using the same approach as batch-scrapper
    markdown_content = ""
    
    # Get markdown content (same as batch-scrapper)
    if result and result.markdown:
        markdown_content = result.markdown
    else:
        logger.warning(f"⚠️ No markdown content from {result.url}")
        markdown_content = "No content extracted"
    
    # Create citation content with proper formatting (same as batch-scrapper)
    citation_content = f"# Content from {result.url}\n\n**URL:** [{result.url}]({result.url})\n\n---\n\n{markdown_content}"
    
    # Add metadata
    citation_content = f"# Citation {successes}\n\n"
    citation_content += f"**URL:** [{result.url}]({result.url})\n\n"
    citation_content += f"**Title:** {getattr(result, 'title', 'N/A')}\n\n"
    citation_content += f"**Crawled At:** {datetime.now(timezone.utc).isoformat()}\n\n"
    citation_content += f"**Content Length:** {len(markdown_content)} characters\n\n"
    citation_content += f"**Job ID:** {job_id}\n\n"
    citation_content += f"**Mode:** {mode}\n\n"
    citation_content += f"**Query:** {query}\n\n"
    citation_content += f"**Success:** {result.success}\n\n"
    
    # Add screenshot info if available
    if hasattr(result, "screenshot") and result.screenshot:
        citation_content += f"**Screenshot:** Available (base64 encoded)\n\n"
    
    citation_content += "---\n\n"
    citation_content += markdown_content
    
    # Save the citation
    s3_key = s3.save_citation_markdown(citation_content, successes)
    if s3_key:
        saved_files.append(s3_key)
    
    logger.info(f"✅ SUCCESS {successes}: {result.url} ({len(citation_content)} chars)")
    logger.info(f"📄 Content preview: {citation_content[:200].replace(chr(10), ' ')}...")

async def _process_failed_result(result, failed: List[Dict[str, str]]):
    """Process a failed crawl result"""
    error_msg = getattr(result, "error_message", "Unknown error")
    failed.append({"url": result.url, "error": error_msg})
    logger.warning(f"❌ FAIL: {result.url} -> {error_msg}")

# ========= Crawl runner =========
async def run_crawl(urls: List[str], job_id: str, mode: str, query: str, product_id: str = None, 
                   brand_name: str = None, product_name: str = None) -> Dict[str, Any]:
    start = time.time()
    s3 = S3Results(job_id=job_id, mode=mode, query=query, product_id=product_id, 
                   brand_name=brand_name, product_name=product_name)
    successes = 0
    failures = 0
    saved_files: List[str] = []
    failed: List[Dict[str, str]] = []

    browser_cfg = create_browser_config()
    crawler_cfg = create_crawler_config()
    dispatcher = create_dispatcher()

    try:
        # Use the same approach as batch-scrapper - crawl URLs sequentially
        async with AsyncWebCrawler(
            verbose=False,
            browser_type="chromium",
            headless=True,
            browser_args=browser_cfg.extra_args
        ) as crawler:
            logger.info(f"🔄 Starting to crawl {len(urls)} URLs sequentially...")
            
            for i, url in enumerate(urls, 1):
                logger.info(f"[{i}/{len(urls)}] 🔄 Processing: {url}")
                
                try:
                    result = await crawler.arun(url=url, config=crawler_cfg)
                    
                    if result and result.markdown:
                        successes += 1
                        await _process_successful_result(result, s3, successes, saved_files, job_id, mode, query)
                    else:
                        failures += 1
                        error_msg = "No content returned" if result else "Crawl failed"
                        failed.append({"url": url, "error": error_msg})
                        logger.warning(f"❌ FAIL {failures}: {url} -> {error_msg}")
                    
                    # Memory management
                    if successes % 2 == 0:  # Every 2 successes
                        gc.collect()
                        mem_percent = psutil.virtual_memory().percent
                        logger.info(f"🧠 Memory after {successes} successes: {mem_percent:.1f}%")
                        
                        if mem_percent > dispatcher.memory_threshold_percent:
                            logger.warning(f"⚠️ Memory threshold exceeded: {mem_percent:.1f}% > {dispatcher.memory_threshold_percent}%")
                            await asyncio.sleep(dispatcher.memory_wait_timeout)
                            gc.collect()
                    
                    # Small delay between requests (same as batch-scrapper)
                    if i < len(urls):  # Don't wait after the last URL
                        logger.info(f"⏱️ Waiting 1 second before next URL...")
                        await asyncio.sleep(1)
                        
                except Exception as e:
                    failures += 1
                    failed.append({"url": url, "error": str(e)})
                    logger.error(f"❌ ERROR for {url}: {e}")
                    gc.collect()  # Force garbage collection after errors
                    
    except MemoryError as e:
        logger.warning(f"⚠️ Memory limit reached, but continuing with partial results: {e}")
        # Don't treat memory errors as critical failures - continue with what we have
    except Exception as e:
        logger.error(f"💥 Critical crawl error: {e}")
        logger.error(traceback.format_exc())
        


    duration = round(time.time() - start, 2)
    s3_summary = s3.list_results()
    
    # Save citations.json for adhoc-results to read
    s3.save_citations_json(urls)
    
    s3.save_session_summary({
        "job_id": job_id,
        "mode": mode,
        "query": query,
        "env_loaded": {
            "S3_BUCKET": _get_env_str("S3_BUCKET", ""),
            "S3_PREFIX": _get_env_str("S3_PREFIX", ""),
            "MEMORY_SIZE": _get_env_int("MEMORY_SIZE", _get_env_int("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", 0)),
            "MAX_SESSION_PERMIT": _get_env_int("MAX_SESSION_PERMIT", 0),
            "PAGE_TIMEOUT": _get_env_int("PAGE_TIMEOUT", 0),
        },
        "metrics": {
            "total_urls": len(urls),
            "successful_crawls": successes,
            "failed_crawls": failures,
            "success_rate_percent": round((successes / max(1, len(urls))) * 100, 2),
            "total_duration_seconds": duration,
        },
        "saved_files": saved_files,
        "failed": failed,
        "s3_results": s3_summary,
    })

    return {
        "job_id": job_id,
        "status": "completed",
        "mode": mode,
        "query": query,
        "metrics": {
            "total_urls": len(urls),
            "successful_crawls": successes,
            "failed_crawls": failures,
            "success_rate_percent": round((successes / max(1, len(urls))) * 100, 2),
            "total_duration_seconds": duration,
            "urls_per_second": round(len(urls) / max(0.001, duration), 2),
        },
        "saved_files": saved_files,
        "failed": failed,
        "s3_results": s3_summary,
        "crawler_config": {
            "headless": True,
            "max_concurrent": dispatcher.max_session_permit,
            "page_timeout_ms": _get_env_int("PAGE_TIMEOUT", 30000),
            "memory_threshold_percent": _get_env_float("MEMORY_THRESHOLD_PERCENT", 75.0),
            "persistent_context": False,
            "http2_disabled": True,
        },
    }

# ========= Lambda handler =========
def lambda_handler(event, context):
    logger.info("🚀 Crawler Lambda handler started")

    if AsyncWebCrawler is None or CrawlerRunConfig is None:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "crawl4ai components not available"}),
        }

    try:
        logger.info(f"🔧 Loaded ENV -> "
                    f"S3_BUCKET='{_get_env_str('S3_BUCKET','')}', "
                    f"S3_PREFIX='{_get_env_str('S3_PREFIX','')}', "
                    f"MEMORY_SIZE='{_get_env_int('MEMORY_SIZE', _get_env_int('AWS_LAMBDA_FUNCTION_MEMORY_SIZE', 0))}', "
                    f"MAX_CONCURRENT='{_get_env_int('MAX_CONCURRENT', 12)}', "
                    f"MAX_SESSION_PERMIT='{_get_env_int('MAX_SESSION_PERMIT', 0)}', "
                    f"PAGE_TIMEOUT='{_get_env_int('PAGE_TIMEOUT', 0)}'")
        logger.info(f"📨 Event: {json.dumps(event, indent=2)}")
        
        # Log the context parameters
        logger.info(f"🔍 Context Parameters -> "
                    f"job_id='{event.get('job_id')}', "
                    f"product_id='{event.get('product_id')}', "
                    f"mode='{event.get('mode')}', "
                    f"query='{event.get('query')}', "
                    f"brand_name='{event.get('brand_name')}', "
                    f"product_name='{event.get('product_name')}'")

        urls = event.get('urls') or event.get('urls_to_crawl')
        job_id = event.get('job_id', f"job-{str(uuid.uuid4())[:8]}")
        mode = event.get('mode', 'default')
        query = event.get('query', 'na')
        product_id = event.get('product_id')  # Extract product_id from event
        brand_name = event.get('brand_name')  # Extract brand_name from event
        product_name = event.get('product_name')  # Extract product_name from event
        root_url = event.get('root_url') or event.get('url')  # For URL discovery

        # If no URLs provided, try to discover them from root_url
        if not urls or not isinstance(urls, list) or len(urls) == 0:
            if root_url:
                logger.info(f"🔍 No URLs provided, discovering from root: {root_url}")
                try:
                    urls = asyncio.run(discover_urls_for_parallel_crawl(root_url))
                    if not urls:
                        return {
                            "statusCode": 400,
                            "headers": {"Content-Type": "application/json"},
                            "body": json.dumps({"error": "Failed to discover URLs from root_url", "job_id": job_id}),
                        }
                    logger.info(f"✅ Discovered {len(urls)} URLs for crawling")
                except Exception as e:
                    logger.error(f"❌ URL discovery failed: {e}")
                    return {
                        "statusCode": 500,
                        "headers": {"Content-Type": "application/json"},
                        "body": json.dumps({"error": f"URL discovery failed: {str(e)}", "job_id": job_id}),
                    }
            else:
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Provide 'urls', 'urls_to_crawl', or 'root_url' for discovery", "job_id": job_id}),
                }
        
        # Filter out YouTube URLs and validate/deduplicate URLs
        urls = filter_youtube_urls(urls)
        logger.info(f"🔍 After YouTube filtering: {len(urls)} URLs remaining")
        
        # Validate and deduplicate URLs if we have a root_url for domain validation
        if root_url:
            domain = get_domain_name(root_url)
            urls = validate_and_deduplicate_urls(urls, domain)

        results = asyncio.run(
            run_crawl(urls=urls, job_id=job_id, mode=mode, query=query, product_id=product_id,
                     brand_name=brand_name, product_name=product_name)
        )
        logger.info(f"🎉 Completed crawl with {results['metrics']['successful_crawls']} successes")
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "X-Job-ID": job_id,
                "X-Mode": mode,
                "X-Success-Rate": str(results['metrics']['success_rate_percent']),
            },
            "body": json.dumps(results),
        }

    except Exception as e:
        logger.error(f"❌ Handler error: {e}")
        logger.error(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": f"Lambda execution failed: {str(e)}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }),
        }

# ========= Local test =========
if __name__ == "__main__":
    # Use environment variables without hardcoding defaults
    # If environment variables aren't set, the defaults in the getter functions will be used

    test_event = {
        # Option 1: Provide specific URLs (real Crawl4AI documentation)
        "urls": [
            "https://docs.crawl4ai.com/",
            "https://crawl4ai.com/",
            "https://github.com/unclecode/crawl4ai",
        ],
        # Option 2: Provide root_url for discovery (uncomment to test discovery)
        # "root_url": "https://docs.crawl4ai.com/",
        "job_id": "local-test-crawl4ai-docs",
        "mode": "gpt",
        "query": "Crawl4AI documentation and features",
        "product_id": "test-product-123",
        "brand_name": "Test Brand",
        "product_name": "Test Product"
    }

    class MockCtx:
        function_name = "local"
        function_version = "1"
        def get_remaining_time_in_millis(self): return 300000

    print(json.dumps(lambda_handler(test_event, MockCtx()), indent=2))
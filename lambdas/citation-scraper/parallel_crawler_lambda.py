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

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode, RateLimiter
    from crawl4ai.async_dispatcher import MemoryAdaptiveDispatcher
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
    args = [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--single-process",
        "--no-zygote",
        "--disable-http2",  # optional: can be removed if HTTP/2 desired
        "--disable-quic",
        "--disable-software-rasterizer",
        "--disable-blink-features=AutomationControlled",
        "--disable-web-security",
        "--disable-background-networking",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-backgrounding-occluded-windows",
        "--disable-extensions",
        "--disable-default-apps",
        "--no-first-run",
        "--mute-audio",
    ]
    logger.info(f"🔧 Browser args count: {len(args)}")
    return BrowserConfig(
        browser_type="chromium",
        headless=True,
        verbose=True,
        use_persistent_context=False,
        extra_args=args,
        viewport_width=1920,
        viewport_height=1080,
        user_data_dir="/tmp/.crawl4ai_user_data",
        user_agent=_get_env_str("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Cache-Control": "max-age=0",
        },
    )

def create_dispatcher() -> MemoryAdaptiveDispatcher:
    # Use MAX_CONCURRENT env var if set, otherwise fall back to MAX_SESSION_PERMIT
    max_session_permit = _get_env_int("MAX_CONCURRENT", 12, min_val=1, max_val=100)
    if max_session_permit == 12:  # If MAX_CONCURRENT wasn't set (using default)
        max_session_permit = _get_env_int("MAX_SESSION_PERMIT", 8, min_val=1, max_val=100)
    
    mem_threshold = _get_env_float("MEMORY_THRESHOLD_PERCENT", 75.0, min_val=50.0, max_val=95.0)
    mem_wait = _get_env_float("MEMORY_WAIT_TIMEOUT", 60.0, min_val=5.0, max_val=600.0)

    base_delay_min = _get_env_float("BASE_DELAY_MIN", 0.5, min_val=0.0, max_val=30.0)
    base_delay_max = _get_env_float("BASE_DELAY_MAX", 2.0, min_val=base_delay_min, max_val=120.0)
    max_retries = _get_env_int("MAX_RETRIES", 3, min_val=0, max_val=10)

    logger.info(
        f"🎛️ Dispatcher cfg -> max_session_permit={max_session_permit}, "
        f"mem_threshold={mem_threshold}%, mem_wait={mem_wait}s, "
        f"delay=({base_delay_min},{base_delay_max}), retries={max_retries}"
    )

    return MemoryAdaptiveDispatcher(
        memory_threshold_percent=mem_threshold,
        max_session_permit=max_session_permit,
        memory_wait_timeout=mem_wait,
        rate_limiter=RateLimiter(
            base_delay=(base_delay_min, base_delay_max),
            max_delay=_get_env_float("RATE_MAX_DELAY", 60.0, 1.0, 600.0),
            max_retries=max_retries,
            rate_limit_codes=[429, 503, 502, 504, 500, 520, 521, 522, 524],
        ),
    )

def create_crawler_config() -> CrawlerRunConfig:
    page_timeout = _get_env_int("PAGE_TIMEOUT", 30000, min_val=5000, max_val=240000)
    word_thresh = _get_env_int("WORD_COUNT_THRESHOLD", 50, min_val=0, max_val=10000)
    exclude_external = _get_env_str("EXCLUDE_EXTERNAL_LINKS", "true").lower() in ("1", "true", "yes")
    return CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        stream=True,
        word_count_threshold=word_thresh,
        page_timeout=page_timeout,
        exclude_external_links=exclude_external,
    )

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
        async with AsyncWebCrawler(config=browser_cfg) as crawler:
            async for result in await crawler.arun_many(urls=urls, config=crawler_cfg, dispatcher=dispatcher):
                if result.success:
                    successes += 1
                    md = ""
                    if getattr(result, "markdown", None):
                        md = getattr(result.markdown, "raw_markdown", "") or str(result.markdown) or ""
                    if not md:
                        md = getattr(result, "cleaned_html", "") or getattr(result, "html", "") or ""
                    s3_key = s3.save_citation_markdown(md, successes)
                    if s3_key:
                        saved_files.append(s3_key)
                    logger.info(f"✅ SUCCESS {successes}: {result.url} ({len(md)} chars)")
                    if successes % 3 == 0:
                        gc.collect()
                        logger.info(f"🧹 Memory used: {psutil.virtual_memory().percent}%")
                else:
                    failures += 1
                    failed.append({"url": result.url, "error": result.error_message})
                    logger.warning(f"❌ FAIL {failures}: {result.url} -> {result.error_message}")
    except Exception as e:
        logger.error(f"💥 Critical crawl error: {e}")
        logger.error(traceback.format_exc())

    duration = round(time.time() - start, 2)
    s3_summary = s3.list_results()
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

        if not urls or not isinstance(urls, list) or len(urls) == 0:
            return {
                "statusCode": 400,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": "Provide 'urls' or 'urls_to_crawl' as a non-empty list", "job_id": job_id}),
            }

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
        "urls": [
            "https://www.gap.com/",
            "https://www2.hm.com/en_in/kids/shop-by-product/clothing.html",
        ],
        "job_id": "local-test",
        "mode": "gpt",
        "query": "which is best skincare brands in USA?",
        "product_id": "test-product-123",
        "brand_name": "Test Brand",
        "product_name": "Test Product"
    }

    class MockCtx:
        function_name = "local"
        function_version = "1"
        def get_remaining_time_in_millis(self): return 300000

    print(json.dumps(lambda_handler(test_event, MockCtx()), indent=2))

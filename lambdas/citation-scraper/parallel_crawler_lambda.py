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
import aiohttp
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

def is_reddit_url(url: str) -> bool:
    """Check if URL is a Reddit URL"""
    domain = get_domain_name(url)
    return 'reddit.com' in domain or 'redd.it' in domain

def convert_reddit_url_to_json(url: str) -> str:
    """Convert Reddit URL to JSON API URL"""
    if not is_reddit_url(url):
        return url
    
    # Remove trailing slash and add .json
    if url.endswith('/'):
        url = url[:-1]
    
    # Add .json to the end
    json_url = url + '.json'
    logger.info(f"🔄 Converted Reddit URL to JSON: {url} -> {json_url}")
    return json_url

async def fetch_reddit_json(url: str) -> Optional[Dict[str, Any]]:
    """Fetch Reddit JSON data from URL"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"✅ Successfully fetched Reddit JSON data from {url}")
                    return data
                else:
                    logger.warning(f"⚠️ Failed to fetch Reddit JSON: {response.status} - {url}")
                    return None
    except Exception as e:
        logger.error(f"❌ Error fetching Reddit JSON from {url}: {e}")
        return None

def parse_reddit_post(post_data: Dict[str, Any]) -> str:
    """Parse Reddit post data and convert to markdown"""
    try:
        if 'data' not in post_data:
            return "No post data found"
        
        post = post_data['data']
        
        # Extract post information
        title = post.get('title', 'No Title')
        author = post.get('author', 'Unknown')
        subreddit = post.get('subreddit', 'Unknown')
        score = post.get('score', 0)
        num_comments = post.get('num_comments', 0)
        created_utc = post.get('created_utc', 0)
        selftext = post.get('selftext', '')
        url = post.get('url', '')
        
        # Convert timestamp
        created_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Build markdown content
        markdown_content = f"""# {title}

**Subreddit:** r/{subreddit}  
**Author:** u/{author}  
**Score:** {score} points  
**Comments:** {num_comments}  
**Posted:** {created_date}  
**URL:** {url}

---

"""
        
        # Add post content if it's a text post
        if selftext:
            markdown_content += f"## Post Content\n\n{selftext}\n\n---\n\n"
        
        return markdown_content
        
    except Exception as e:
        logger.error(f"❌ Error parsing Reddit post: {e}")
        return f"Error parsing Reddit post: {str(e)}"

def parse_reddit_comments(comments_data: List[Dict[str, Any]], max_depth: int = 3) -> str:
    """Parse Reddit comments and convert to markdown"""
    try:
        if not comments_data:
            return ""
        
        markdown_content = "## Comments\n\n"
        
        def parse_comment(comment: Dict[str, Any], depth: int = 0) -> str:
            if depth > max_depth:
                return ""
            
            if 'data' not in comment:
                return ""
            
            comment_data = comment['data']
            
            # Skip deleted/removed comments
            if comment_data.get('body') in ['[deleted]', '[removed]', None]:
                return ""
            
            author = comment_data.get('author', 'Unknown')
            body = comment_data.get('body', '')
            score = comment_data.get('score', 0)
            created_utc = comment_data.get('created_utc', 0)
            
            # Convert timestamp
            created_date = datetime.fromtimestamp(created_utc, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
            
            # Indent based on depth
            indent = "  " * depth
            header_prefix = "#" * min(depth + 3, 6)  # Limit header depth
            
            comment_md = f"{indent}**{header_prefix} u/{author}** ({score} points) - {created_date}\n\n{indent}{body}\n\n"
            
            # Parse replies
            replies = comment_data.get('replies', {})
            if replies and isinstance(replies, dict) and 'data' in replies:
                children = replies['data'].get('children', [])
                for reply in children:
                    if reply.get('kind') == 't1':  # Comment type
                        comment_md += parse_comment(reply, depth + 1)
            
            return comment_md
        
        # Parse top-level comments
        for comment in comments_data:
            if comment.get('kind') == 't1':  # Comment type
                markdown_content += parse_comment(comment)
        
        return markdown_content
        
    except Exception as e:
        logger.error(f"❌ Error parsing Reddit comments: {e}")
        return f"Error parsing Reddit comments: {str(e)}"

async def crawl_reddit_url(url: str) -> Optional[str]:
    """Crawl Reddit URL using JSON API and convert to markdown"""
    try:
        # Convert to JSON URL
        json_url = convert_reddit_url_to_json(url)
        
        # Fetch JSON data
        json_data = await fetch_reddit_json(json_url)
        if not json_data:
            return None
        
        # Parse the JSON response
        markdown_content = ""
        
        # Reddit JSON response is typically a list with post and comments
        if isinstance(json_data, list) and len(json_data) >= 1:
            # First item is the post
            post_data = json_data[0]
            if 'data' in post_data and 'children' in post_data['data']:
                children = post_data['data']['children']
                if children and len(children) > 0:
                    post = children[0]
                    markdown_content += parse_reddit_post(post)
            
            # Second item (if exists) contains comments
            if len(json_data) >= 2:
                comments_data = json_data[1]
                if 'data' in comments_data and 'children' in comments_data['data']:
                    comments = comments_data['data']['children']
                    markdown_content += parse_reddit_comments(comments)
        
        logger.info(f"✅ Successfully parsed Reddit content: {len(markdown_content)} characters")
        return markdown_content
        
    except Exception as e:
        logger.error(f"❌ Error crawling Reddit URL {url}: {e}")
        return None

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
      s3://{bucket}/brand_name/job_id/session_id/product_name/query_text/mode/citation_{i}.md
    """
    def __init__(self, job_id: str, mode: str, query: str, product_id: str = None, 
                 brand_name: str = None, product_name: str = None, session_id: str = None):
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
        
        # Create new S3 prefix following the required structure: brand_name/job_id/session_id/product_name/query_text/mode/
        self.prefix = f"{brand_name_safe}/{job_id}/{session_id}/{product_name_safe}/{query_text_safe}/{mode}/"
        
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
        # Create local results directory with S3 structure
        results_dir = f"results/{self.prefix}"
        os.makedirs(results_dir, exist_ok=True)
        
        # Save locally with S3 structure
        local_file = f"{results_dir}citation_{idx}.md"
        header = (
            f"# Citation {idx}\n\n"
            f"**Saved At:** {datetime.now(timezone.utc).isoformat()}\n\n"
            f"**Content Length:** {len(content or '')} characters\n\n"
            f"**Job ID:** {self.job_id}\n\n"
            "---\n\n"
        )
        
        try:
            with open(local_file, 'w', encoding='utf-8') as f:
                f.write(header + (content or ""))
            logger.info(f"✅ Saved citation locally to {local_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save citation locally: {e}")
        
        # Also save to S3 if bucket is configured
        if not self.bucket:
            logger.warning("⚠️ No S3_BUCKET set; saved locally only.")
            return local_file
            
        key = f"{self.prefix}citation_{idx}.md"
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
            return local_file

    def save_citations_json(self, urls: List[str]) -> Optional[str]:
        """Save citations.json with simple format for adhoc-results to read"""
        # Create local results directory with S3 structure
        results_dir = f"results/{self.prefix}"
        os.makedirs(results_dir, exist_ok=True)
        
        citations_data = {
            "citations": urls
        }
        
        # Save locally with S3 structure
        local_file = f"{results_dir}citations.json"
        try:
            with open(local_file, 'w', encoding='utf-8') as f:
                json.dump(citations_data, f, indent=2)
            logger.info(f"✅ Saved citations.json locally to {local_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save citations.json locally: {e}")
        
        # Also save to S3 if bucket is configured
        if not self.bucket:
            logger.warning("⚠️ No S3_BUCKET set; saved locally only.")
            return local_file
            
        key = f"{self.prefix}citations.json"
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
            return local_file

    def save_session_summary(self, meta: Dict[str, Any]) -> Optional[str]:
        # Create local results directory with S3 structure
        results_dir = f"results/{self.prefix}"
        os.makedirs(results_dir, exist_ok=True)
        
        # Save locally with S3 structure
        local_file = f"{results_dir}session_summary.json"
        try:
            with open(local_file, 'w', encoding='utf-8') as f:
                json.dump(meta, f, indent=2, default=str)
            logger.info(f"✅ Saved session summary locally to {local_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save session summary locally: {e}")
        
        # Also save to S3 if bucket is configured
        if not self.bucket:
            logger.warning("⚠️ No S3_BUCKET set; saved locally only.")
            return local_file
            
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
            return local_file

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
    # Use the exact same browser configuration as the working copy.py (Lambda-tested)
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
    # Use the exact same crawler configuration as the working copy.py (Lambda-tested)
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
                   brand_name: str = None, product_name: str = None, session_id: str = None) -> Dict[str, Any]:
    start = time.time()
    s3 = S3Results(job_id=job_id, mode=mode, query=query, product_id=product_id, 
                   brand_name=brand_name, product_name=product_name, session_id=session_id)
    successes = 0
    failures = 0
    saved_files: List[str] = []
    failed: List[Dict[str, str]] = []

    browser_cfg = create_browser_config()
    crawler_cfg = create_crawler_config()
    dispatcher = create_dispatcher()

    try:
        # Separate Reddit URLs from regular URLs
        reddit_urls = [url for url in urls if is_reddit_url(url)]
        regular_urls = [url for url in urls if not is_reddit_url(url)]
        
        logger.info(f"🔍 Processing {len(reddit_urls)} Reddit URLs and {len(regular_urls)} regular URLs")
        
        # Process Reddit URLs with JSON API
        for reddit_url in reddit_urls:
            try:
                logger.info(f"🔄 Processing Reddit URL: {reddit_url}")
                reddit_content = await crawl_reddit_url(reddit_url)
                if reddit_content:
                    successes += 1
                    s3_key = s3.save_citation_markdown(reddit_content, successes)
                    if s3_key:
                        saved_files.append(s3_key)
                    logger.info(f"✅ REDDIT SUCCESS {successes}: {reddit_url} ({len(reddit_content)} chars)")
                else:
                    failures += 1
                    failed.append({"url": reddit_url, "error": "Failed to fetch Reddit JSON content"})
                    logger.warning(f"❌ REDDIT FAIL {failures}: {reddit_url} -> Failed to fetch JSON content")
            except Exception as e:
                failures += 1
                failed.append({"url": reddit_url, "error": str(e)})
                logger.error(f"❌ REDDIT ERROR {failures}: {reddit_url} -> {e}")
        
        # Process regular URLs with crawl4ai
        if regular_urls:
            async with AsyncWebCrawler(config=browser_cfg) as crawler:
                async for result in await crawler.arun_many(urls=regular_urls, config=crawler_cfg, dispatcher=dispatcher):
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

def filter_youtube_urls(urls: List[str]) -> List[str]:
    """Filter out YouTube URLs as they're not suitable for crawling"""
    filtered_urls = []
    for url in urls:
        if 'youtube.com' not in url.lower() and 'youtu.be' not in url.lower():
            filtered_urls.append(url)
    
    removed_count = len(urls) - len(filtered_urls)
    if removed_count > 0:
        logger.info(f"🚫 Removed {removed_count} YouTube URLs from crawling list")
    else:
        logger.info(f"✅ No YouTube URLs found in {len(urls)} URLs")
    
    return filtered_urls

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
                    f"product_name='{event.get('product_name')}', "
                    f"session_id='{event.get('session_id')}'")

        urls = event.get('urls') or event.get('urls_to_crawl')
        job_id = event.get('job_id', f"job-{str(uuid.uuid4())[:8]}")
        mode = event.get('mode', 'default')
        query = event.get('query', 'na')
        product_id = event.get('product_id')  # Extract product_id from event
        brand_name = event.get('brand_name')  # Extract brand_name from event
        product_name = event.get('product_name')  # Extract product_name from event
        session_id = event.get('session_id')  # Extract session_id from event
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
        
        # Filter out YouTube URLs
        urls = filter_youtube_urls(urls)
        
        # Validate and deduplicate URLs if we have a root_url for domain validation
        if root_url:
            domain = get_domain_name(root_url)
            urls = validate_and_deduplicate_urls(urls, domain)

        results = asyncio.run(
            run_crawl(urls=urls, job_id=job_id, mode=mode, query=query, product_id=product_id,
                     brand_name=brand_name, product_name=product_name, session_id=session_id)
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
        # Option 1: Provide specific URLs (Reddit test)
        "urls": [
            "https://www.reddit.com/r/AWSCertifications/comments/1loonak/passed_the_aws_saa_with_861_my_thoughts/",
            "https://www.reddit.com/r/india/comments/uanza0/what_are_some_good_quality_and_affordable_daily",
            "https://www.reddit.com/r/wicked_edge/comments/196xu35/best_premium_brand",
            "https://www.reddit.com/r/Frugal/comments/6byzuv/male_groominghygiene_products_best_bangforbuck",
            "https://www.reddit.com/r/frugalmalefashion/comments/a60gnc/male_grooming_products"
        ],
        # Option 2: Provide root_url for discovery (uncomment to test discovery)
        # "root_url": "https://docs.crawl4ai.com/",
        "job_id": "test-reddit-aws-789",
        "mode": "pplx",
        "query": "AWS SAA certification Reddit discussions and search results",
        "product_id": "reddit-aws-123",
        "brand_name": "Reddit",
        "product_name": "AWS Certification Discussion",
        "session_id": "test-session-123"
    }

    class MockCtx:
        function_name = "local"
        function_version = "1"
        def get_remaining_time_in_millis(self): return 300000

    print(json.dumps(lambda_handler(test_event, MockCtx()), indent=2))
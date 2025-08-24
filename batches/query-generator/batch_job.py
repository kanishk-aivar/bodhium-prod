#!/usr/bin/env python3
"""
AWS Batch Job for Query Generation
Converts Lambda function to batch processing for long-running workloads
"""

import json
import uuid
import os
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any, Optional

print(f"[INIT] Starting Batch job initialization at {datetime.utcnow().isoformat()}")

# Import AWS services first (fastest)
import boto3
print(f"[INIT] boto3 imported at {datetime.utcnow().isoformat()}")

# ────────────────────────────────────────────────────────────────────────────────
# Environment variables with defaults and validation
# ────────────────────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ.get("S3_BUCKET", "bodhium-query")
DYNAMODB_TABLE_NAME = os.environ.get("ORCHESTRATION_LOGS_TABLE", "OrchestrationLogs")
SECRET_NAME_GEMINI = os.environ.get("SECRET_NAME", "Gemini-API-ChatGPT")
SECRET_NAME_RDS = os.environ.get("RDS_SECRET", "dev/rds")
SECRET_REGION = os.environ.get("SECRET_REGION", "us-east-1")

# Batch-specific environment variables
JOB_ID = os.environ.get("JOB_ID")
PRODUCTS_JSON = os.environ.get("PRODUCTS_JSON")
NUM_QUESTIONS = int(os.environ.get("NUM_QUESTIONS", "25"))
INPUT_S3_BUCKET = os.environ.get("INPUT_S3_BUCKET")
INPUT_S3_KEY = os.environ.get("INPUT_S3_KEY")

# Threading configuration
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "25"))
THREAD_TIMEOUT = int(os.environ.get("THREAD_TIMEOUT", "300"))  # 5 minutes per thread

print(f"[INIT] Environment variables loaded:")
print(f"  S3_BUCKET: {S3_BUCKET}")
print(f"  DYNAMODB_TABLE_NAME: {DYNAMODB_TABLE_NAME}")
print(f"  SECRET_NAME_GEMINI: {SECRET_NAME_GEMINI}")
print(f"  SECRET_NAME_RDS: {SECRET_NAME_RDS}")
print(f"  SECRET_REGION: {SECRET_REGION}")
print(f"  JOB_ID: {JOB_ID}")
print(f"  NUM_QUESTIONS: {NUM_QUESTIONS}")
print(f"  INPUT_S3_BUCKET: {INPUT_S3_BUCKET}")
print(f"  INPUT_S3_KEY: {INPUT_S3_KEY}")
print(f"  MAX_WORKERS: {MAX_WORKERS}")
print(f"  THREAD_TIMEOUT: {THREAD_TIMEOUT}s")

# ────────────────────────────────────────────────────────────────────────────────
# AWS clients and resources (initialize once)
# ────────────────────────────────────────────────────────────────────────────────
try:
    print(f"[INIT] Initializing AWS clients at {datetime.utcnow().isoformat()}")
    secrets_client = boto3.client("secretsmanager", region_name=SECRET_REGION)
    dynamodb = boto3.resource("dynamodb", region_name=SECRET_REGION)
    s3_client = boto3.client("s3", region_name=SECRET_REGION)
    orchestration_logs_table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    print(f"[INIT] AWS clients initialized successfully")
except Exception as e:
    print(f"[INIT ERROR] Failed to initialize AWS clients: {str(e)}")
    sys.exit(1)

# Lazy imports for heavy dependencies
psycopg = None
dict_row = None
genai = None

# Cache for API keys and configurations (fetched once per job to avoid repeated Secrets Manager calls)
# This is especially important for batch jobs processing many products
_api_key_cache = None
_rds_config_cache = None

def lazy_import_psycopg():
    """Lazy import psycopg to reduce cold start time"""
    global psycopg, dict_row
    if psycopg is None:
        print(f"[LAZY_IMPORT] Importing psycopg at {datetime.utcnow().isoformat()}")
        import psycopg as pg
        from psycopg.rows import dict_row as dr
        psycopg = pg
        dict_row = dr
        print(f"[LAZY_IMPORT] psycopg imported successfully")

def lazy_import_genai():
    """Lazy import Google Generative AI to reduce cold start time"""
    global genai
    if genai is None:
        print(f"[LAZY_IMPORT] Importing google.genai at {datetime.utcnow().isoformat()}")
        from google import genai as gai
        from google.genai import types
        genai = gai
        print(f"[LAZY_IMPORT] google.genai imported successfully")

print(f"[INIT] Batch job initialization completed at {datetime.utcnow().isoformat()}")

# ────────────────────────────────────────────────────────────────────────────────
# Helper functions (same as Lambda version)
# ────────────────────────────────────────────────────────────────────────────────
def get_secret(secret_name: str) -> dict:
    """Retrieve a secret from AWS Secrets Manager."""
    print(f"[SECRET] Retrieving secret: {secret_name}")
    try:
        start_time = time.time()
        resp = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(resp["SecretString"])
        elapsed = time.time() - start_time
        print(f"[SECRET] Successfully retrieved secret {secret_name} in {elapsed:.2f}s")
        return secret_data
    except Exception as exc:
        print(f"[SECRET ERROR] Error retrieving secret {secret_name}: {exc}")
        raise RuntimeError(f"Error retrieving secret {secret_name}: {exc}") from exc


def log_to_dynamodb(job_id: str, event: str, details: dict):
    """Write an event to the OrchestrationLogs table."""
    print(f"[DYNAMO] Logging event '{event}' for job {job_id}")
    try:
        start_time = time.time()
        ts = datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
        event_id = str(uuid.uuid4())[:8]
        sk = f"{ts}#{event_id}"

        # Convert floats → Decimal for DynamoDB
        def _convert(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            if isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_convert(v) for v in obj]
            return obj

        orchestration_logs_table.put_item(
            Item={
                "pk": job_id,
                "sk": sk,
                "job_id": job_id,
                "event_timestamp_id": sk,
                "eventName": event,
                "details": _convert(details),
            }
        )
        elapsed = time.time() - start_time
        print(f"[DYNAMO] Successfully logged event '{event}' in {elapsed:.2f}s")
    except Exception as exc:
        print(f"[DYNAMO ERROR] DynamoDB log failed for event '{event}': {exc}")


def get_rds_config():
    """Get RDS configuration from cache or fetch from Secrets Manager once."""
    global _rds_config_cache
    
    if _rds_config_cache is None:
        print(f"[DB] Fetching RDS configuration from Secrets Manager (first time)")
        _rds_config_cache = get_secret(SECRET_NAME_RDS)
        print(f"[DB] RDS configuration cached for session")
    
    return _rds_config_cache


def get_gemini_api_key():
    """Get Gemini API key from cache or fetch from Secrets Manager once."""
    global _api_key_cache
    
    if _api_key_cache is None:
        print(f"[AI] Fetching Gemini API key from Secrets Manager (first time)")
        gem_cfg = get_secret(SECRET_NAME_GEMINI)
        _api_key_cache = gem_cfg["GEMINI_API_KEY"]
        print(f"[AI] Gemini API key cached for session")
    
    return _api_key_cache


def initialize_secrets():
    """Pre-fetch and cache all secrets at the start of the job for better performance."""
    print(f"[INIT] Pre-fetching all secrets for caching...")
    start_time = time.time()
    
    # Pre-fetch Gemini API key
    get_gemini_api_key()
    
    # Pre-fetch RDS configuration  
    get_rds_config()
    
    elapsed = time.time() - start_time
    print(f"[INIT] All secrets cached successfully in {elapsed:.2f}s")


def get_db_connection():
    """Return a psycopg3 connection using cached credentials."""
    print(f"[DB] Getting database connection")
    lazy_import_psycopg()
    
    try:
        start_time = time.time()
        cfg = get_rds_config()  # Use cached config
        print(f"[DB] Using cached RDS credentials, connecting to host: {cfg.get('DB_HOST', 'unknown')}")
        
        conn = psycopg.connect(
            host=cfg["DB_HOST"],
            port=cfg["DB_PORT"],
            dbname=cfg["DB_NAME"],
            user=cfg["DB_USER"],
            password=cfg["DB_PASSWORD"],
            autocommit=False,
        )
        elapsed = time.time() - start_time
        print(f"[DB] Database connection established in {elapsed:.2f}s")
        return conn
    except Exception as exc:
        print(f"[DB ERROR] Failed to connect to database: {exc}")
        raise


def get_product_ids_from_job(job_id: str) -> Dict[str, int]:
    """Get existing products from a specific job_id instead of creating new ones."""
    print(f"[PRODUCTS] Getting products for job_id: {job_id}")
    
    try:
        start_time = time.time()
        with get_db_connection() as conn:
            product_id_map: Dict[str, int] = {}
            with conn.cursor(row_factory=dict_row) as cur:
                # Get all products linked to this job
                cur.execute(
                    """
                    SELECT 
                        p.product_id,
                        p.product_data->>'productname' as product_name,
                        p.brand_name
                    FROM products p
                    INNER JOIN jobselectedproducts jsp ON p.product_id = jsp.product_id
                    WHERE jsp.job_id = %s
                    """,
                    (job_id,),
                )
                rows = cur.fetchall()
                
                if not rows:
                    print(f"[PRODUCTS WARNING] No products found for job_id: {job_id}")
                    return {}
                
                for row in rows:
                    product_name = row["product_name"] or ""
                    brand_name = row["brand_name"] or ""
                    prod_key = f"{product_name}_{brand_name}"
                    product_id_map[prod_key] = row["product_id"]
                    print(f"[PRODUCTS] Found product ID {row['product_id']}: {product_name}")
                
                elapsed = time.time() - start_time
                print(f"[PRODUCTS] Retrieved {len(product_id_map)} products for job {job_id} in {elapsed:.2f}s")
                return product_id_map
                
    except Exception as exc:
        print(f"[PRODUCTS ERROR] Failed to get products for job {job_id}: {exc}")
        raise


def get_product_info_from_rds(product_ids: List[int]) -> List[dict]:
    """Fetch product information from RDS database using product IDs."""
    print(f"[PRODUCTS] Fetching product information for {len(product_ids)} product IDs")
    
    try:
        start_time = time.time()
        with get_db_connection() as conn:
            products = []
            with conn.cursor(row_factory=dict_row) as cur:
                # Process product IDs in batches for better performance
                batch_size = 50
                for i in range(0, len(product_ids), batch_size):
                    batch_ids = product_ids[i:i + batch_size]
                    print(f"[PRODUCTS] Processing batch {i//batch_size + 1}/{(len(product_ids) + batch_size - 1)//batch_size}")
                    
                    # Use parameterized query with IN clause
                    placeholders = ','.join(['%s'] * len(batch_ids))
                    cur.execute(
                        f"""
                        SELECT 
                            product_id,
                            product_hash,
                            product_data,
                            source_url,
                            first_scraped_at,
                            brand_name
                        FROM products
                        WHERE product_id IN ({placeholders})
                        ORDER BY product_id
                        """,
                        batch_ids
                    )
                    
                    batch_products = cur.fetchall()
                    products.extend(batch_products)
                    
                    print(f"[PRODUCTS] Retrieved {len(batch_products)} products from batch")
                
            elapsed = time.time() - start_time
            print(f"[PRODUCTS] Successfully fetched {len(products)} products in {elapsed:.2f}s")
            return products
    except Exception as exc:
        print(f"[PRODUCTS ERROR] Failed to fetch product information: {exc}")
        raise


def get_product_ids_from_rds(products: List[dict]) -> Dict[str, int]:
    """DEPRECATED: Use get_product_info_from_rds instead. Insert new products (if needed) and return a map of composite key → product_id."""
    print(f"[PRODUCTS] Processing {len(products)} products")
    
    try:
        start_time = time.time()
        with get_db_connection() as conn:
            product_id_map: Dict[str, int] = {}
            with conn.cursor(row_factory=dict_row) as cur:
                for i, prod in enumerate(products):
                    print(f"[PRODUCTS] Processing product {i+1}/{len(products)}: {prod.get('name', 'unknown')}")
                    
                    name = prod.get("name", "")
                    brand = prod.get("brand", "")
                    prod_key = f"{name}_{brand}"

                    # Try to find an existing product
                    cur.execute(
                        """
                        SELECT product_id
                        FROM products
                        WHERE product_data->>'productname' ILIKE %s
                          AND brand_name ILIKE %s
                        LIMIT 1
                        """,
                        (f"%{name}%", f"%{brand}%"),
                    )
                    row = cur.fetchone()

                    if row:
                        product_id_map[prod_key] = row["product_id"]
                        print(f"[PRODUCTS] Found existing product with ID: {row['product_id']}")
                    else:
                        print(f"[PRODUCTS] Creating new product for: {name} - {brand}")
                        cur.execute(
                            """
                            INSERT INTO products (product_data, brand_name, product_hash)
                            VALUES (%s, %s, %s)
                            RETURNING product_id
                            """,
                            (
                                json.dumps(prod),
                                brand,
                                str(hash(json.dumps(prod, sort_keys=True))),
                            ),
                        )
                        new_id = cur.fetchone()["product_id"]
                        product_id_map[prod_key] = new_id
                        print(f"[PRODUCTS] Created new product with ID: {new_id}")
            
            conn.commit()
            elapsed = time.time() - start_time
            print(f"[PRODUCTS] Processed all products in {elapsed:.2f}s")
            return product_id_map
    except Exception as exc:
        print(f"[PRODUCTS ERROR] Failed to process products: {exc}")
        raise


def save_queries_to_rds(queries: List[tuple]) -> List[int]:
    """Bulk-insert generated questions and return their query_id values."""
    print(f"[QUERIES] Saving {len(queries)} queries to database")
    
    try:
        start_time = time.time()
        with get_db_connection() as conn:
            ids: List[int] = []
            with conn.cursor() as cur:
                # Process queries in batches for better performance
                batch_size = 50
                for i in range(0, len(queries), batch_size):
                    batch = queries[i:i + batch_size]
                    print(f"[QUERIES] Processing batch {i//batch_size + 1}/{(len(queries) + batch_size - 1)//batch_size}")
                    
                    for query_data in batch:
                        try:
                            product_id, query_text, query_type = query_data
                            print(f"[QUERIES] Inserting query with product_id={product_id}, query_type='{query_type}'")
                            
                            cur.execute(
                                """
                                INSERT INTO queries (product_id, query_text, query_type, is_active)
                                VALUES (%s, %s, %s, TRUE)
                                RETURNING query_id
                                """,
                                (product_id, query_text, query_type),
                            )
                            result = cur.fetchone()
                            if result:
                                ids.append(result[0])
                                print(f"[QUERIES] Successfully inserted query with ID: {result[0]}, type: '{query_type}'")
                        except Exception as e:
                            print(f"[QUERIES ERROR] Failed to insert query {query_data}: {e}")
                            # Continue with other queries instead of failing completely
                            continue
                        
            conn.commit()
            elapsed = time.time() - start_time
            print(f"[QUERIES] Successfully saved {len(ids)} out of {len(queries)} queries in {elapsed:.2f}s")
            return ids
    except Exception as exc:
        print(f"[QUERIES ERROR] Failed to save queries: {exc}")
        raise


def save_to_s3(data: dict, job_id: str) -> str:
    """Persist JSON output to S3 and return the s3:// path."""
    print(f"[S3] Saving results to S3 for job {job_id}")
    
    try:
        start_time = time.time()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        key = f"query/{job_id}_{timestamp}_queries.json"
        
        # Convert data to JSON string with proper serialization
        def json_serializer(obj):
            """Custom JSON serializer for non-serializable objects"""
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            if hasattr(obj, '__dict__'):
                return obj.__dict__
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        json_string = json.dumps(data, indent=2, default=json_serializer)
        json_data = json_string.encode('utf-8')
        
        print(f"[S3] Uploading {len(json_data)} bytes to s3://{S3_BUCKET}/{key}")
        
        # Add error handling for S3 upload
        try:
            response = s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json_data,
                ContentType="application/json",
                Metadata={
                    'job_id': job_id,
                    'timestamp': timestamp,
                    'content_length': str(len(json_data))
                }
            )
            print(f"[S3] S3 put_object response: {response.get('ResponseMetadata', {}).get('HTTPStatusCode', 'unknown')}")
        except Exception as s3_error:
            print(f"[S3 ERROR] S3 upload failed: {s3_error}")
            # Try alternative key in case of naming issues
            alternative_key = f"query/backup_{uuid.uuid4().hex[:8]}_{timestamp}.json"
            print(f"[S3] Trying alternative key: {alternative_key}")
            response = s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=alternative_key,
                Body=json_data,
                ContentType="application/json"
            )
            key = alternative_key
        
        s3_path = f"s3://{S3_BUCKET}/{key}"
        elapsed = time.time() - start_time
        print(f"[S3] Successfully saved to {s3_path} in {elapsed:.2f}s")
        return s3_path
        
    except Exception as exc:
        print(f"[S3 ERROR] Failed to save to S3: {exc}")
        print(f"[S3 ERROR] Exception type: {type(exc).__name__}")
        import traceback
        print(f"[S3 ERROR] Full traceback:")
        traceback.print_exc()
        raise


# ────────────────────────────────────────────────────────────────────────────────
# AI-powered question generation (same as Lambda version)
# ────────────────────────────────────────────────────────────────────────────────
def generate_questions(product_data: dict, num_questions: int = 25) -> Dict[str, List[str]]:
    """Generate product-specific and market-specific questions via Gemini."""
    print(f"[AI] Generating {num_questions} questions for product: {product_data.get('productname', 'unknown')}")
    
    lazy_import_genai()
    
    try:
        # Get cached Gemini API key (fetched once per job session)
        start_time = time.time()
        api_key = get_gemini_api_key()  # Use cached API key
        
        print(f"[AI] Using cached Gemini API key for model configuration")
        
        # Use the new Gemini API pattern
        from google.genai import types
        
        client = genai.Client(api_key=api_key)
        model = "gemini-2.5-flash"
        
        # Create product summary from product_data only
        summary = "\n".join(f"{k}: {v}" for k, v in product_data.items())
        print(f"[AI] Product summary length: {len(summary)} characters")

        # Prompts
        prod_prompt = (
            f"You are a product analyst assistant. "
            f"Based on the following product information, generate {num_questions} intelligent, varied, and helpful "
            f"natural-language questions a user might ask **about this specific product**.\n\n"
            f"Product Information:\n{summary}\n\n"
            f"Output: numbered list of {num_questions} unique questions."
            f"You should mention the product name and brand name in the question."
        )

        market_prompt = (
            f"You are a product market analyst. "
            f"Based on the following product information, generate {num_questions} questions about this product's "
            f"ranking, reputation, or market position (no direct competitor comparisons).\n\n"
            f"Product Information:\n{summary}\n\n"
            f"Output: numbered list of {num_questions} unique questions."
            f"You should mention the product name and brand name in the question."
        )

        def _extract(text: str) -> List[str]:
            questions = [
                line.lstrip("0123456789. ").strip()
                for line in text.splitlines()
                if line.strip() and any(ch.isalpha() for ch in line)
            ]
            print(f"[AI] Extracted {len(questions)} questions from response")
            return questions

        print(f"[AI] Generating product-specific questions...")
        prod_start = time.time()
        
        # Use new API pattern for product questions
        prod_contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=prod_prompt),
                ],
            ),
        ]
        prod_generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_budget=0,
            ),
            response_mime_type="application/json",
        )
        
        prod_resp = client.models.generate_content(
            model=model,
            contents=prod_contents,
            config=prod_generate_content_config,
        )
        
        prod_elapsed = time.time() - prod_start
        print(f"[AI] Product questions generated in {prod_elapsed:.2f}s")
        
        print(f"[AI] Generating market-specific questions...")
        market_start = time.time()
        
        # Use new API pattern for market questions
        market_contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=market_prompt),
                ],
            ),
        ]
        market_generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_budget=0,
            ),
            response_mime_type="application/json",
        )
        
        market_resp = client.models.generate_content(
            model=model,
            contents=market_contents,
            config=market_generate_content_config,
        )
        
        market_elapsed = time.time() - market_start
        print(f"[AI] Market questions generated in {market_elapsed:.2f}s")
        
        result = {
            "product_questions": _extract(prod_resp.text or ""),
            "market_questions": _extract(market_resp.text or ""),
        }
        
        total_elapsed = time.time() - start_time
        total_questions = len(result["product_questions"]) + len(result["market_questions"])
        print(f"[AI] Generated {total_questions} total questions in {total_elapsed:.2f}s")
        
        return result
        
    except Exception as exc:
        print(f"[AI ERROR] Gemini error: {exc}")
        raise RuntimeError(f"Gemini error: {exc}") from exc


def process_single_product(product: dict, num_questions: int, thread_id: int) -> tuple[bool, dict, List[tuple]]:
    """Process a single product in a thread-safe manner."""
    thread_name = f"Thread-{thread_id}"
    print(f"[{thread_name}] Starting processing for product ID: {product.get('product_id', 'unknown')}")
    
    try:
        start_time = time.time()
        
        # Extract product information
        prod_id = product['product_id']
        product_name = product.get('product_data', {}).get('productname', 'Unknown Product')
        brand_name = product.get('brand_name', 'Unknown Brand')
        
        print(f"[{thread_name}] Processing: {product_name} ({brand_name}) - ID: {prod_id}")
        
        # Extract product_data for question generation
        product_data = product.get('product_data', {})
        
        # Generate questions using only product_data
        questions = generate_questions(product_data, num_questions)
        
        # Prepare queries for database
        queries = []
        for q in questions["product_questions"]:
            queries.append((prod_id, q, "product_based"))
        for q in questions["market_questions"]:
            queries.append((prod_id, q, "market_based"))
        
        # Prepare result
        result = {
            "product": product,
            "product_id": prod_id,
            "product_name": product_name,
            "brand_name": brand_name,
            **questions,
        }
        
        elapsed = time.time() - start_time
        print(f"[{thread_name}] Completed product {prod_id} in {elapsed:.2f}s: {len(questions['product_questions'])} + {len(questions['market_questions'])} questions")
        
        return True, result, queries
        
    except Exception as exc:
        elapsed = time.time() - start_time if 'start_time' in locals() else 0
        print(f"[{thread_name}] ERROR processing product {product.get('product_id', 'unknown')}: {exc}")
        return False, {"error": str(exc), "product_id": product.get('product_id', 'unknown')}, []


def process_products_parallel(products: List[dict], num_questions: int) -> tuple[List[dict], List[tuple]]:
    """Process multiple products in parallel using ThreadPoolExecutor."""
    print(f"[THREADING] Starting parallel processing of {len(products)} products with {MAX_WORKERS} workers")
    
    start_time = time.time()
    all_queries = []
    output = []
    failed_products = []
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="QueryGen") as executor:
        # Submit all products for processing
        future_to_product = {
            executor.submit(process_single_product, product, num_questions, i): product 
            for i, product in enumerate(products)
        }
        
        print(f"[THREADING] Submitted {len(future_to_product)} products to thread pool")
        
        # Process completed futures
        completed_count = 0
        for future in as_completed(future_to_product, timeout=THREAD_TIMEOUT):
            product = future_to_product[future]
            completed_count += 1
            
            try:
                success, result, queries = future.result(timeout=THREAD_TIMEOUT)
                
                if success:
                    output.append(result)
                    all_queries.extend(queries)
                    print(f"[THREADING] Completed {completed_count}/{len(products)} products successfully")
                else:
                    failed_products.append(result)
                    print(f"[THREADING] Failed {completed_count}/{len(products)} products: {result.get('error', 'Unknown error')}")
                    
            except Exception as exc:
                failed_products.append({
                    "error": str(exc),
                    "product_id": product.get('product_id', 'unknown'),
                    "thread_error": True
                })
                print(f"[THREADING] Thread error for product {product.get('product_id', 'unknown')}: {exc}")
    
    elapsed = time.time() - start_time
    print(f"[THREADING] Parallel processing completed in {elapsed:.2f}s")
    print(f"[THREADING] Results: {len(output)} successful, {len(failed_products)} failed")
    
    if failed_products:
        print(f"[THREADING] Failed products: {[p.get('product_id', 'unknown') for p in failed_products]}")
    
    return output, all_queries


# ────────────────────────────────────────────────────────────────────────────────
# Input handling for batch job
# ────────────────────────────────────────────────────────────────────────────────
def load_input_data() -> tuple[List[dict], str]:
    """Load input data from various sources (env vars, S3, CLI args)"""
    print(f"[INPUT] Loading input data...")
    
    products = []
    job_id = JOB_ID or str(uuid.uuid4())
    
    # Method 1: From environment variable (JSON string)
    if PRODUCTS_JSON:
        print(f"[INPUT] Loading products from PRODUCTS_JSON environment variable")
        try:
            products = json.loads(PRODUCTS_JSON)
            print(f"[INPUT] Loaded {len(products)} products from environment variable")
        except json.JSONDecodeError as e:
            print(f"[INPUT ERROR] Failed to parse PRODUCTS_JSON: {e}")
            sys.exit(1)
    
    # Method 2: From S3
    elif INPUT_S3_BUCKET and INPUT_S3_KEY:
        print(f"[INPUT] Loading products from S3: s3://{INPUT_S3_BUCKET}/{INPUT_S3_KEY}")
        try:
            response = s3_client.get_object(Bucket=INPUT_S3_BUCKET, Key=INPUT_S3_KEY)
            data = json.loads(response['Body'].read())
            products = data.get('products', data) if isinstance(data, dict) else data
            print(f"[INPUT] Loaded {len(products)} products from S3")
        except Exception as e:
            print(f"[INPUT ERROR] Failed to load from S3: {e}")
            sys.exit(1)
    
    # Method 3: From command line arguments
    else:
        parser = argparse.ArgumentParser(description='Query Generation Batch Job')
        parser.add_argument('--products-file', type=str, help='Path to JSON file containing products')
        parser.add_argument('--job-id', type=str, help='Job ID for tracking')
        args = parser.parse_args()
        
        if args.job_id:
            job_id = args.job_id
            
        if args.products_file:
            print(f"[INPUT] Loading products from file: {args.products_file}")
            try:
                with open(args.products_file, 'r') as f:
                    data = json.load(f)
                    products = data.get('products', data) if isinstance(data, dict) else data
                    print(f"[INPUT] Loaded {len(products)} products from file")
            except Exception as e:
                print(f"[INPUT ERROR] Failed to load from file: {e}")
                sys.exit(1)
    
    if not products:
        print(f"[INPUT ERROR] No products found. Please provide input via:")
        print(f"  1. PRODUCTS_JSON environment variable")
        print(f"  2. INPUT_S3_BUCKET and INPUT_S3_KEY environment variables")
        print(f"  3. --products-file command line argument")
        sys.exit(1)
    
    print(f"[INPUT] Successfully loaded {len(products)} products with job_id: {job_id}")
    return products, job_id


# ────────────────────────────────────────────────────────────────────────────────
# Main batch job function
# ────────────────────────────────────────────────────────────────────────────────
def main():
    """Main batch job function - replaces lambda_handler"""
    job_start = time.time()
    
    print(f"[BATCH] Batch job execution started at {datetime.utcnow().isoformat()}")
    print(f"[BATCH] Process ID: {os.getpid()}")
    print(f"[BATCH] Current working directory: {os.getcwd()}")
    
    job_id = None
    
    try:
        # ── Initialize secrets cache ───────────────────────────────────────────
        initialize_secrets()
        
        # ── Load input data ─────────────────────────────────────────────────────
        products, job_id = load_input_data()
        num_questions = NUM_QUESTIONS
        
        print(f"[BATCH] Processing:")
        print(f"  Products count: {len(products)}")
        print(f"  Questions per product: {num_questions}")
        print(f"  Job ID: {job_id}")

        log_to_dynamodb(
            job_id,
            "QueryGenerationBatchStarted",
            {
                "products_count": len(products), 
                "num_questions_requested": num_questions,
                "process_id": os.getpid(),
                "execution_type": "batch"
            },
        )

        # ── Process products ───────────────────────────────────────────────────
        print(f"[BATCH] Processing product IDs from input")
        products_start = time.time()
        
        # Check if input contains product IDs (new format) or full product data (old format)
        if isinstance(products[0], int) or (isinstance(products[0], str) and products[0].isdigit()):
            # New format: Input contains only product IDs
            print(f"[BATCH] Input contains product IDs, fetching product information from database")
            
            # Convert string IDs to integers
            product_ids = []
            for prod_id in products:
                try:
                    if isinstance(prod_id, str):
                        product_ids.append(int(prod_id))
                    else:
                        product_ids.append(prod_id)
                except ValueError:
                    print(f"[BATCH ERROR] Invalid product_id format: {prod_id}")
                    continue
            
            print(f"[BATCH] Processing {len(product_ids)} product IDs: {product_ids}")
            
            # Fetch product information from database
            products = get_product_info_from_rds(product_ids)
            
            if not products:
                print(f"[BATCH ERROR] No products found in database for IDs: {product_ids}")
                sys.exit(1)
            
            # Create product_id_map for consistency
            product_id_map = {str(prod['product_id']): prod['product_id'] for prod in products}
            
        else:
            # Old format: Input contains full product data
            print(f"[BATCH] Input contains full product data, using existing logic")
            
            # Check if products in request already contain product_id
            product_id_map = {}
            has_product_ids = all(prod.get('product_id') for prod in products)
            
            if has_product_ids:
                print(f"[BATCH] Using product_ids from input data")
                for prod in products:
                    key = f"{prod.get('name', '')}_{prod.get('brand', '')}"
                    # Convert product_id to integer if it's a string
                    prod_id = prod['product_id']
                    if isinstance(prod_id, str):
                        try:
                            prod_id = int(prod_id)
                            print(f"[BATCH] Converted product_id '{prod['product_id']}' to integer {prod_id}")
                        except ValueError:
                            print(f"[BATCH ERROR] Invalid product_id format: {prod['product_id']}")
                            continue
                    product_id_map[key] = prod_id
                    print(f"[BATCH] Product ID {prod_id}: {prod.get('name', 'unknown')}")
            else:
                print(f"[BATCH] No product_ids in input, getting from job_id: {job_id}")
                # Use job-based product lookup instead of creating new products
                product_id_map = get_product_ids_from_job(job_id)
                
                # If no products found for the job, fall back to the old method
                if not product_id_map:
                    print(f"[BATCH] No products found for job_id, using product data from input")
                    product_id_map = get_product_ids_from_rds(products)
        
        products_elapsed = time.time() - products_start
        print(f"[BATCH] Products processed in {products_elapsed:.2f}s")

        # ── Generate questions ─────────────────────────────────────────────────
        print(f"[BATCH] Generating questions for all products...")
        generation_start = time.time()
        
        # Use parallel processing for multiple products
        if len(products) > 1:
            print(f"[BATCH] Using parallel processing with {MAX_WORKERS} workers")
            output, all_queries = process_products_parallel(products, num_questions)
        else:
            print(f"[BATCH] Single product detected, using sequential processing")
            # For single product, use the existing logic
            all_queries: List[tuple] = []
            output = []
            
            for i, prod in enumerate(products):
                prod_id = prod['product_id']
                product_name = prod.get('product_data', {}).get('productname', 'Unknown Product')
                brand_name = prod.get('brand_name', 'Unknown Brand')
                
                print(f"[BATCH] Processing product {i+1}/{len(products)}: {product_name} ({brand_name}) - ID: {prod_id}")
                
                # Extract product_data for question generation
                product_data = prod.get('product_data', {})
                
                # Generate questions using only product_data
                questions = generate_questions(product_data, num_questions)
                
                for q in questions["product_questions"]:
                    all_queries.append((prod_id, q, "product_based"))
                for q in questions["market_questions"]:
                    all_queries.append((prod_id, q, "market_based"))
                
                print(f"[BATCH] Added {len(questions['product_questions'])} 'product_based' and {len(questions['market_questions'])} 'market_based' queries for product_id {prod_id}")

                product_result = {
                    "product": prod,
                    "product_id": prod_id,
                    "product_name": product_name,
                    "brand_name": brand_name,
                    **questions,
                }
                output.append(product_result)
                
                print(f"[BATCH] Product {i+1} completed: {len(questions['product_questions'])} + {len(questions['market_questions'])} questions")

        generation_elapsed = time.time() - generation_start
        print(f"[BATCH] All questions generated in {generation_elapsed:.2f}s")
        print(f"[BATCH] Total queries to save: {len(all_queries)}")

        # ── Persist results ───────────────────────────────────────────────────
        print(f"[BATCH] Persisting results...")
        persist_start = time.time()
        
        # Save to database
        query_ids = []
        try:
            query_ids = save_queries_to_rds(all_queries)
            print(f"[BATCH] Successfully saved {len(query_ids)} queries to database")
        except Exception as db_error:
            print(f"[BATCH WARNING] Database save failed: {db_error}")
            # Continue with S3 save even if database fails
            query_ids = []
        
        # Prepare S3 data
        s3_data = {
            "job_id": job_id,
            "timestamp": datetime.utcnow().isoformat(),
            "results": output,
            "query_ids": query_ids,
            "execution_stats": {
                "products_processed": len(output),
                "total_queries_generated": len(all_queries),
                "queries_saved_to_db": len(query_ids),
                "products_processing_time": products_elapsed,
                "questions_generation_time": generation_elapsed,
            },
            "metadata": {
                "process_id": os.getpid(),
                "execution_type": "batch",
                "python_version": sys.version,
                "threading": {
                    "max_workers": MAX_WORKERS,
                    "thread_timeout": THREAD_TIMEOUT,
                    "parallel_processing": len(products) > 1
                }
            }
        }
        
        # Save to S3 (always attempt this)
        s3_path = ""
        try:
            s3_path = save_to_s3(s3_data, job_id)
            print(f"[BATCH] Successfully saved results to S3: {s3_path}")
        except Exception as s3_error:
            print(f"[BATCH ERROR] S3 save failed: {s3_error}")
            # Don't fail the entire job if S3 save fails
            s3_path = f"FAILED_TO_SAVE: {str(s3_error)}"
        
        persist_elapsed = time.time() - persist_start
        print(f"[BATCH] Results persistence completed in {persist_elapsed:.2f}s")

        # ── Final logging ─────────────────────────────────────────────────────
        total_elapsed = time.time() - job_start
        print(f"[BATCH] Total execution time: {total_elapsed:.2f}s")

        log_to_dynamodb(
            job_id,
            "QueryGenerationBatchCompleted",
            {
                "products_processed": len(output),
                "total_queries_generated": len(all_queries),
                "queries_saved_to_db": len(query_ids),
                "s3_output_path": s3_path,
                "execution_time_seconds": total_elapsed,
                "time_breakdown": {
                    "products_processing": products_elapsed,
                    "questions_generation": generation_elapsed,
                    "results_persistence": persist_elapsed
                },
                "threading": {
                    "max_workers": MAX_WORKERS,
                    "thread_timeout": THREAD_TIMEOUT,
                    "parallel_processing": len(products) > 1,
                    "threading_enabled": len(products) > 1
                }
            },
        )

        print(f"[BATCH] Job completed successfully!")
        print(f"[BATCH] Summary:")
        print(f"  - Products processed: {len(output)}")
        print(f"  - Queries generated: {len(all_queries)}")
        print(f"  - Queries saved to DB: {len(query_ids)}")
        print(f"  - S3 output: {s3_path}")
        print(f"  - Total time: {total_elapsed:.2f}s")
        print(f"  - Threading: {MAX_WORKERS} workers, {'enabled' if len(products) > 1 else 'disabled'}")
        if len(products) > 1:
            print(f"  - Parallel processing: {generation_elapsed:.2f}s vs estimated sequential: {generation_elapsed * len(products):.2f}s")
            print(f"  - Performance improvement: {((generation_elapsed * len(products) - generation_elapsed) / (generation_elapsed * len(products)) * 100):.1f}%")
        
        # Exit with success
        sys.exit(0)

    except Exception as exc:
        error_elapsed = time.time() - job_start
        print(f"[BATCH ERROR] Exception occurred after {error_elapsed:.2f}s: {exc}")
        print(f"[BATCH ERROR] Exception type: {type(exc).__name__}")
        
        import traceback
        print(f"[BATCH ERROR] Full traceback:")
        traceback.print_exc()
        
        # Try to save error data to S3 for debugging
        if job_id:
            try:
                error_data = {
                    "job_id": job_id,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "timestamp": datetime.utcnow().isoformat(),
                    "execution_time_seconds": error_elapsed,
                    "process_id": os.getpid(),
                    "traceback": traceback.format_exc()
                }
                error_s3_path = save_to_s3(error_data, f"ERROR_{job_id}")
                print(f"[BATCH] Error data saved to S3: {error_s3_path}")
            except Exception as s3_error:
                print(f"[BATCH] Failed to save error data to S3: {s3_error}")
        
            log_to_dynamodb(
                job_id, 
                "QueryGenerationBatchFailed", 
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "execution_time_seconds": error_elapsed,
                    "process_id": os.getpid()
                }
            )
        
        # Exit with error code
        sys.exit(1)


if __name__ == "__main__":
    main()

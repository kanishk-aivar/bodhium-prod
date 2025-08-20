import json
import uuid
import os
import time
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Any, Optional

print(f"[INIT] Starting Lambda initialization at {datetime.utcnow().isoformat()}")

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

print(f"[INIT] Environment variables loaded:")
print(f"  S3_BUCKET: {S3_BUCKET}")
print(f"  DYNAMODB_TABLE_NAME: {DYNAMODB_TABLE_NAME}")
print(f"  SECRET_NAME_GEMINI: {SECRET_NAME_GEMINI}")
print(f"  SECRET_NAME_RDS: {SECRET_NAME_RDS}")
print(f"  SECRET_REGION: {SECRET_REGION}")

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
    raise

# Lazy imports for heavy dependencies
psycopg = None
dict_row = None
genai = None

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
        print(f"[LAZY_IMPORT] Importing google.generativeai at {datetime.utcnow().isoformat()}")
        import google.generativeai as gai
        genai = gai
        print(f"[LAZY_IMPORT] google.generativeai imported successfully")

print(f"[INIT] Lambda initialization completed at {datetime.utcnow().isoformat()}")

# ────────────────────────────────────────────────────────────────────────────────
# Helper functions
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


def get_db_connection():
    """Return a psycopg3 connection using credentials from Secrets Manager."""
    print(f"[DB] Getting database connection")
    lazy_import_psycopg()
    
    try:
        start_time = time.time()
        cfg = get_secret(SECRET_NAME_RDS)
        print(f"[DB] Retrieved RDS credentials, connecting to host: {cfg.get('DB_HOST', 'unknown')}")
        
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


def get_product_ids_from_rds(products: List[dict]) -> Dict[str, int]:
    """DEPRECATED: Use get_product_ids_from_job instead. Insert new products (if needed) and return a map of composite key → product_id."""
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
# AI-powered question generation
# ────────────────────────────────────────────────────────────────────────────────
def generate_questions(product_info: dict, num_questions: int = 25) -> Dict[str, List[str]]:
    """Generate product-specific and market-specific questions via Gemini."""
    print(f"[AI] Generating {num_questions} questions for product: {product_info.get('name', 'unknown')}")
    
    lazy_import_genai()
    
    try:
        # Get Gemini API key from Secrets Manager
        start_time = time.time()
        gem_cfg = get_secret(SECRET_NAME_GEMINI)
        api_key = gem_cfg["GEMINI_API_KEY"]
        
        print(f"[AI] Configuring Gemini API")
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")

        summary = "\n".join(f"{k}: {v}" for k, v in product_info.items())
        print(f"[AI] Product summary length: {len(summary)} characters")

        # Prompts
        prod_prompt = (
            f"You are a product analyst assistant. "
            f"Based on the following product information, generate {num_questions} intelligent, varied, and helpful "
            f"natural-language questions a user might ask **about this specific product**.\n\n"
            f"Product Information:\n{summary}\n\n"
            f"Output: numbered list of {num_questions} unique questions."
        )

        market_prompt = (
            f"You are a product market analyst. "
            f"Based on the following product information, generate {num_questions} questions about this product's "
            f"ranking, reputation, or market position (no direct competitor comparisons).\n\n"
            f"Product Information:\n{summary}\n\n"
            f"Output: numbered list of {num_questions} unique questions."
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
        prod_resp = model.generate_content(prod_prompt)
        prod_elapsed = time.time() - prod_start
        print(f"[AI] Product questions generated in {prod_elapsed:.2f}s")
        
        print(f"[AI] Generating market-specific questions...")
        market_start = time.time()
        market_resp = model.generate_content(market_prompt)
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


# ────────────────────────────────────────────────────────────────────────────────
# Lambda handler
# ────────────────────────────────────────────────────────────────────────────────
def lambda_handler(event: Dict[str, Any], context: Any):
    """Main Lambda handler with comprehensive logging"""
    handler_start = time.time()
    
    print(f"[HANDLER] Lambda execution started at {datetime.utcnow().isoformat()}")
    print(f"[HANDLER] Request ID: {context.aws_request_id}")
    print(f"[HANDLER] Function name: {context.function_name}")
    print(f"[HANDLER] Function version: {context.function_version}")
    print(f"[HANDLER] Memory limit: {context.memory_limit_in_mb}MB")
    print(f"[HANDLER] Time remaining: {context.get_remaining_time_in_millis()}ms")

    try:
        # ── Parse input FIRST to get job_id ───────────────────────────────────
        print(f"[HANDLER] Parsing input event")
        print(f"[HANDLER] Event keys: {list(event.keys())}")
        
        body = (
            json.loads(event["body"])
            if isinstance(event.get("body"), str)
            else event.get("body", {})
        )
        
        products: List[dict] = body.get("products", [])
        num_questions = int(body.get("num_questions", 25))
        job_id = body.get("job_id", str(uuid.uuid4()))  # Use provided or generate UUID
        
        print(f"[HANDLER] Generated/received job_id: {job_id}")
        
        print(f"[HANDLER] Parsed input:")
        print(f"  Products count: {len(products)}")
        print(f"  Questions per product: {num_questions}")
        print(f"  Job ID: {job_id}")

        if not products:
            print(f"[HANDLER ERROR] No products provided in request")
            return _response(400, {"error": "No products provided in request body"})

        log_to_dynamodb(
            job_id,
            "QueryGenerationLambdaStarted",
            {
                "products_count": len(products), 
                "num_questions_requested": num_questions,
                "request_id": context.aws_request_id,
                "function_version": context.function_version
            },
        )

        # ── Process products ───────────────────────────────────────────────────
        print(f"[HANDLER] Processing product IDs from request")
        products_start = time.time()
        
        # Check if products in request already contain product_id
        product_id_map = {}
        has_product_ids = all(prod.get('product_id') for prod in products)
        
        if has_product_ids:
            print(f"[HANDLER] Using product_ids from request")
            for prod in products:
                key = f"{prod.get('name', '')}_{prod.get('brand', '')}"
                # Convert product_id to integer if it's a string
                prod_id = prod['product_id']
                if isinstance(prod_id, str):
                    try:
                        prod_id = int(prod_id)
                        print(f"[HANDLER] Converted product_id '{prod['product_id']}' to integer {prod_id}")
                    except ValueError:
                        print(f"[HANDLER ERROR] Invalid product_id format: {prod['product_id']}")
                        continue
                product_id_map[key] = prod_id
                print(f"[HANDLER] Product ID {prod_id}: {prod.get('name', 'unknown')}")
        else:
            print(f"[HANDLER] No product_ids in request, getting from job_id: {job_id}")
            # Use job-based product lookup instead of creating new products
            product_id_map = get_product_ids_from_job(job_id)
            
            # If no products found for the job, fall back to the old method
            if not product_id_map:
                print(f"[HANDLER] No products found for job_id, using product data from request")
                product_id_map = get_product_ids_from_rds(products)
        
        products_elapsed = time.time() - products_start
        print(f"[HANDLER] Products processed in {products_elapsed:.2f}s")

        # ── Generate questions ─────────────────────────────────────────────────
        print(f"[HANDLER] Generating questions for all products...")
        generation_start = time.time()
        all_queries: List[tuple] = []
        output = []
        
        for i, prod in enumerate(products):
            print(f"[HANDLER] Processing product {i+1}/{len(products)}: {prod.get('name', 'unknown')}")
            key = f"{prod.get('name', '')}_{prod.get('brand', '')}"
            prod_id = product_id_map.get(key)
            
            if not prod_id:
                print(f"[HANDLER WARNING] No product_id found for key: {key}")
                continue

            questions = generate_questions(prod, num_questions)
            
            for q in questions["product_questions"]:
                all_queries.append((prod_id, q, "product_based"))
            for q in questions["market_questions"]:
                all_queries.append((prod_id, q, "market_based"))
            
            print(f"[HANDLER] Added {len(questions['product_questions'])} 'product_based' and {len(questions['market_questions'])} 'market_based' queries for product_id {prod_id}")

            product_result = {
                "product": prod,
                "product_id": prod_id,
                **questions,
            }
            output.append(product_result)
            
            print(f"[HANDLER] Product {i+1} completed: {len(questions['product_questions'])} + {len(questions['market_questions'])} questions")

        generation_elapsed = time.time() - generation_start
        print(f"[HANDLER] All questions generated in {generation_elapsed:.2f}s")
        print(f"[HANDLER] Total queries to save: {len(all_queries)}")

        # ── Persist results ───────────────────────────────────────────────────
        print(f"[HANDLER] Persisting results...")
        persist_start = time.time()
        
        # Save to database
        query_ids = []
        try:
            query_ids = save_queries_to_rds(all_queries)
            print(f"[HANDLER] Successfully saved {len(query_ids)} queries to database")
        except Exception as db_error:
            print(f"[HANDLER WARNING] Database save failed: {db_error}")
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
                "request_id": context.aws_request_id,
                "function_name": context.function_name,
                "function_version": context.function_version
            }
        }
        
        # Save to S3 (always attempt this)
        s3_path = ""
        try:
            s3_path = save_to_s3(s3_data, job_id)
            print(f"[HANDLER] Successfully saved results to S3: {s3_path}")
        except Exception as s3_error:
            print(f"[HANDLER ERROR] S3 save failed: {s3_error}")
            # Don't fail the entire function if S3 save fails
            s3_path = f"FAILED_TO_SAVE: {str(s3_error)}"
        
        persist_elapsed = time.time() - persist_start
        print(f"[HANDLER] Results persistence completed in {persist_elapsed:.2f}s")

        # ── Final logging and response ─────────────────────────────────────────
        total_elapsed = time.time() - handler_start
        print(f"[HANDLER] Total execution time: {total_elapsed:.2f}s")
        print(f"[HANDLER] Time remaining: {context.get_remaining_time_in_millis()}ms")

        log_to_dynamodb(
            job_id,
            "QueryGenerationLambdaCompleted",
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
                }
            },
        )

        response_data = {
            "success": True,
            "job_id": job_id,
            "data": output,
            "total_products": len(output),
            "total_queries_generated": len(all_queries),
            "queries_saved_to_db": len(query_ids),
            "s3_output_path": s3_path,
            "query_ids": query_ids,
            "execution_time_seconds": total_elapsed,
        }
        
        print(f"[HANDLER] Returning successful response")
        return _response(200, response_data)

    except Exception as exc:
        error_elapsed = time.time() - handler_start
        print(f"[HANDLER ERROR] Exception occurred after {error_elapsed:.2f}s: {exc}")
        print(f"[HANDLER ERROR] Exception type: {type(exc).__name__}")
        
        import traceback
        print(f"[HANDLER ERROR] Full traceback:")
        traceback.print_exc()
        
        # Try to save error data to S3 for debugging
        try:
            error_data = {
                "job_id": job_id,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "timestamp": datetime.utcnow().isoformat(),
                "execution_time_seconds": error_elapsed,
                "request_id": context.aws_request_id,
                "traceback": traceback.format_exc()
            }
            error_s3_path = save_to_s3(error_data, f"ERROR_{job_id}")
            print(f"[HANDLER] Error data saved to S3: {error_s3_path}")
        except Exception as s3_error:
            print(f"[HANDLER] Failed to save error data to S3: {s3_error}")
        
        log_to_dynamodb(
            job_id, 
            "QueryGenerationLambdaFailed", 
            {
                "error": str(exc),
                "error_type": type(exc).__name__,
                "execution_time_seconds": error_elapsed,
                "request_id": context.aws_request_id
            }
        )
        
        return _response(
            500, 
            {
                "error": f"Internal server error: {exc}", 
                "job_id": job_id,
                "error_type": type(exc).__name__,
                "execution_time_seconds": error_elapsed
            }
        )


# ────────────────────────────────────────────────────────────────────────────────
# Utility: HTTP response builder
# ────────────────────────────────────────────────────────────────────────────────
def _response(status: int, body: dict):
    """Build HTTP response with proper headers"""
    print(f"[RESPONSE] Building response with status {status}")
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
        },
        "body": json.dumps(body),
    }
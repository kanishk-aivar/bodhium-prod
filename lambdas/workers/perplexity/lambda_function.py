import json
import datetime
from datetime import datetime as dt, timezone
import os
import boto3
import psycopg2
import uuid
import logging
import re
from botocore.exceptions import ClientError
import urllib.request
import urllib.error
import ssl
import time
from typing import Tuple, Any, Dict, List, Optional

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
DEFAULT_SECRET_NAME = 'Bodhium-PerplexityAPI'
DEFAULT_SECRET_REGION = 'us-east-1'
RDS_SECRET_NAME = 'dev/rds'
PERPLEXITY_API_URL = "https://api.perplexity.ai/chat/completions"
DEFAULT_MODEL = "sonar-pro"  # Adjust as needed
HTTP_TIMEOUT_SECONDS = 30
MAX_QUERY_LEN = 10000
DB_ERROR_MSG_MAX = 1000
S3_DEFAULT_BUCKET = 'bodhium-dev'
S3_DEFAULT_PATH = 'perplexity/'
NEW_BUCKET = 'bodhium-temp'
NEW_BUCKET_FILE_TEMPLATE = "perplexity_query_{query_id}.{ext}"

# -------- Secrets --------

def get_secret(secret_name: Optional[str] = None, region_name: Optional[str] = None) -> Any:
    if secret_name is None:
        secret_name = os.environ.get('secret_name', DEFAULT_SECRET_NAME)
    if region_name is None:
        region_name = os.environ.get('secret_region', DEFAULT_SECRET_REGION)
    logger.info(f"Fetching secret from {secret_name} in region {region_name}")

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response.get('SecretString')
        if secret is None and 'SecretBinary' in get_secret_value_response:
            # Fallback if secret is binary (rare)
            secret = get_secret_value_response['SecretBinary'].decode('utf-8')
        try:
            parsed = json.loads(secret)
            logger.info(f"Successfully retrieved secret as JSON from {secret_name}")
            return parsed
        except (json.JSONDecodeError, TypeError):
            logger.info(f"Successfully retrieved secret as string from {secret_name}")
            return secret
    except ClientError as e:
        logger.error(f"Secrets Manager fetch failed for {secret_name}: {str(e)}")
        raise Exception(f"Secrets Manager fetch failed: {str(e)}")
    except Exception as e:
        logger.error(f"Error loading secret from {secret_name}: {str(e)}")
        raise Exception(f"Error loading secret: {str(e)}")

# -------- Database --------

def get_db_connection():
    try:
        secret = get_secret(secret_name=RDS_SECRET_NAME, region_name=DEFAULT_SECRET_REGION)
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=secret['DB_PORT']
        )
        logger.info("Successfully connected to database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def get_product_name_from_db(product_id):
    """Fetch product name from products table"""
    try:
        conn = get_db_connection()
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

def create_llm_task(job_id: str, query_id: str, llm_model_name: str = "perplexity", product_id: str = None, product_name: str = None) -> str:
    logger.info(f"Creating LLM task for job_id: {job_id}, query_id: {query_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        task_id = str(uuid.uuid4())
        cursor.execute(
            """
            INSERT INTO llmtasks (
                task_id,
                job_id,
                query_id,
                llm_model_name,
                status,
                created_at,
                product_id,
                product_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                task_id,
                job_id,
                query_id,
                llm_model_name,
                "created",
                dt.now(timezone.utc),
                product_id,
                product_name
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Inserted LLM task to DB -- task_id: {task_id}, job_id: {job_id}, query_id: {query_id}")
        return task_id
    except psycopg2.IntegrityError as e:
        logger.error(f"Integrity error creating LLM task: {e}")
        raise
    except psycopg2.OperationalError as e:
        logger.error(f"Operational error creating LLM task: {e}")
        raise
    except Exception as e:
        logger.error(f"Error creating LLM task for job_id: {job_id}, query_id: {query_id}: {e}")
        raise

def _truncate_for_db(value: str, max_len: int = DB_ERROR_MSG_MAX) -> str:
    if value is None:
        return None
    value = str(value)
    if len(value) > max_len:
        return value[: max_len - 3] + "..."
    return value

def update_task_status(task_id: str, status: str, error_message: Optional[str] = None,
                       s3_output_path: Optional[str] = None, completed_at: Optional[dt] = None) -> bool:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = ["status = %s"]
        params = [status]

        if error_message is not None:
            error_message = _truncate_for_db(error_message, DB_ERROR_MSG_MAX)
            update_fields.append("error_message = %s")
            params.append(error_message)

        if s3_output_path is not None:
            update_fields.append("s3_output_path = %s")
            params.append(s3_output_path)

        if completed_at is not None:
            update_fields.append("completed_at = %s")
            params.append(completed_at)

        params.append(task_id)
        query = f"UPDATE llmtasks SET {', '.join(update_fields)} WHERE task_id = %s"
        cursor.execute(query, params)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Updated task {task_id} status to {status}")
        return True
    except Exception as e:
        logger.error(f"Error updating task status for task_id {task_id}: {e}")
        # Attempt a simplified update
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("UPDATE llmtasks SET status = %s WHERE task_id = %s", [status, task_id])
            conn.commit()
            cursor.close()
            conn.close()
            logger.info(f"Updated task {task_id} status to {status} (simplified)")
            return True
        except Exception as e2:
            logger.error(f"Failed to update task status even with simplified query: {e2}")
            return False

# -------- Perplexity API --------

def make_perplexity_api_call(user_query: str, api_key: str, model: str = DEFAULT_MODEL,
                             max_retries: int = 2, backoff_seconds: float = 1.5) -> Tuple[Dict[str, Any], int]:
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": user_query}
        ]
    }
    logger.info(f"Making API call to Perplexity for query (len={len(user_query)}): {user_query[:120]}...")
    data = json.dumps(payload).encode('utf-8')

    # Prepare request
    req = urllib.request.Request(PERPLEXITY_API_URL, data=data)
    req.add_header('Authorization', f'Bearer {api_key}')
    req.add_header('Content-Type', 'application/json')

    # TLS context for Python environments that require explicit SSL context
    context = ssl.create_default_context()

    attempt = 0
    while True:
        attempt += 1
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SECONDS, context=context) as response:
                response_data = response.read().decode('utf-8')
                logger.info("Perplexity API call successful")
                return json.loads(response_data), response.status
        except urllib.error.HTTPError as e:
            error_response = e.read().decode('utf-8') if hasattr(e, 'read') else str(e)
            logger.error(f"HTTP Error from Perplexity API: {e.code} - {error_response}")
            # Retry on 429/5xx
            if e.code in (429, 500, 502, 503, 504) and attempt <= max_retries:
                time.sleep(backoff_seconds * attempt)
                continue
            try:
                return json.loads(error_response), e.code
            except Exception:
                return {'error': error_response}, e.code
        except urllib.error.URLError as e:
            logger.error(f"URL Error connecting to Perplexity API: {str(e)}")
            if attempt <= max_retries:
                time.sleep(backoff_seconds * attempt)
                continue
            return {'error': f'URL Error: {str(e)}'}, 500
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error from Perplexity API response: {str(e)}")
            return {'error': f'Invalid JSON response: {str(e)}'}, 500
        except Exception as e:
            logger.error(f"Unexpected error in Perplexity API call: {str(e)}")
            if attempt <= max_retries:
                time.sleep(backoff_seconds * attempt)
                continue
            return {'error': f'Request failed: {str(e)}'}, 500

def extract_response_data(resp_json: Dict[str, Any]) -> Tuple[str, List[str], List[Dict[str, Any]]]:
    """
    Extracts main content, citations/sources URLs, and related questions.
    Handles Perplexity variants that use message.citations or message.sources or top-level citations.
    Returns:
        main_content: str
        citations: List[str] (URLs or identifiers)
        related_questions: List[Dict[str, Any]] (if present)
    """
    main_content = ""
    related_questions: List[Dict[str, Any]] = []
    citations: List[str] = []

    try:
        choices = resp_json.get("choices", [])
        if choices:
            message = choices[0].get("message", {}) or {}
            main_content = message.get("content", "") or ""

            # Handle multiple possible keys for citations/sources
            # Common shape: message.get("citations") as list of URLs or dicts
            possible_citation_keys = ["citations", "sources", "source_attributions"]
            for key in possible_citation_keys:
                raw = message.get(key)
                if raw and isinstance(raw, list):
                    for item in raw:
                        if isinstance(item, str):
                            citations.append(item)
                        elif isinstance(item, dict):
                            # Common dict patterns: {"url": "...", "title": "..."}
                            url = item.get("url") or item.get("source") or item.get("link")
                            if url:
                                citations.append(url)

            # Fallback: Some responses put citations at top-level
            if not citations:
                top_level_citations = resp_json.get("citations")
                if isinstance(top_level_citations, list):
                    for item in top_level_citations:
                        if isinstance(item, str):
                            citations.append(item)
                        elif isinstance(item, dict):
                            url = item.get("url") or item.get("source") or item.get("link")
                            if url:
                                citations.append(url)

            # Related questions may be in message.related_questions
            related = message.get("related_questions", [])
            if isinstance(related, list):
                related_questions = related

    except Exception as e:
        logger.warning(f"Could not fully extract response fields: {e}")

    # Deduplicate citations while preserving order
    seen = set()
    deduped = []
    for c in citations:
        if c not in seen:
            seen.add(c)
            deduped.append(c)

    return main_content, deduped, related_questions

# -------- S3 --------

def sanitize_filename_from_query(query: str, max_words: int = 3) -> str:
    sanitized = re.sub(r'[^a-zA-Z0-9\s]', '', query).lower()
    sanitized = re.sub(r'\s+', ' ', sanitized).strip().replace(' ', '_')
    if len(sanitized) > 50:
        words = sanitized.split('_')
        if len(words) > max_words:
            sanitized = '_'.join(words[:max_words])
        else:
            sanitized = sanitized[:50]
    return sanitized or 'query'

def save_to_s3(result: Dict[str, Any], query: str, bucket_name: str, s3_path: str, task_id: Optional[str] = None) -> str:
    try:
        s3_client = boto3.client('s3')
        sanitized_query = sanitize_filename_from_query(query)
        filename = f"perplexity_{sanitized_query}_{(task_id or str(uuid.uuid4()))[:8]}.json"
        s3_key = f"{s3_path.rstrip('/')}/{filename}"

        result_json = json.dumps(result, ensure_ascii=False, indent=2)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=result_json.encode('utf-8'),
            ContentType='application/json'
        )
        logger.info(f"Saved result to s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
    except Exception as e:
        logger.error(f"Error saving to S3 for query: {query}: {str(e)}")
        raise Exception(f"S3 save failed: {str(e)}")

def log_orchestration_event(job_id, event_name, details=None):
    """Log an orchestration event to DynamoDB."""
    table_name = os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    timestamp = dt.now(timezone.utc).isoformat()
    unique_id = str(uuid.uuid4())[:8]
    
    pk = f"JOB#{job_id}"
    sk = f"{timestamp}#{unique_id}"
    
    item = {
        'pk': pk,
        'sk': sk,
        'job_id': job_id,
        'event_timestamp_id': sk,
        'eventName': event_name,
        'details': details or {},
        'created_at': timestamp
    }
    
    try:
        table.put_item(Item=item)
        logger.info(f"Logged orchestration event: {event_name} for job_id: {job_id}")
        print(f"✅ Logged orchestration event to DynamoDB")
    except Exception as e:
        logger.error(f"Error logging orchestration event: {e}")
        print(f"❌ Failed to log orchestration event: {e}")

def upload_to_new_bucket_structure(content: Any, job_id: str, product_id: str, query_id: str,
                                   content_type: str = 'application/json', file_extension: str = 'json') -> Optional[str]:
    try:
        s3_client = boto3.client('s3')
        s3_key = f"{job_id}/{product_id}/" + NEW_BUCKET_FILE_TEMPLATE.format(query_id=query_id, ext=file_extension)

        if isinstance(content, str):
            body = content
        else:
            body = json.dumps(content, indent=2, ensure_ascii=False)

        s3_client.put_object(
            Bucket=NEW_BUCKET,
            Key=s3_key,
            Body=body.encode('utf-8') if isinstance(body, str) else body,
            ContentType=content_type
        )
        logger.info(f"Content uploaded to new bucket structure: s3://{NEW_BUCKET}/{s3_key}")
        return f"s3://{NEW_BUCKET}/{s3_key}"
    except Exception as e:
        logger.error(f"Error uploading to new bucket structure: {e}")
        return None

# -------- Lambda Handler --------

def _iso_now_tz() -> str:
    return dt.now(timezone.utc).isoformat()

def lambda_handler(event, context):
    logger.info("Lambda function invoked")
    task_id = None
    job_id = None
    query_id = None
    new_bucket_path = None

    try:
        print("🎯 Lambda function started (Perplexity API)")
        logger.info(f"Received event: {json.dumps(event)}")
        job_id = event.get('job_id')
        query_id = event.get('query_id', 1)  # Default to integer like other lambdas
        product_id = event.get('product_id')
        user_query = event.get('query')

        print(f"📊 Job ID: {job_id}, Query ID: {query_id}, Product ID: {product_id}")
        
        # Get product name from database
        product_name = get_product_name_from_db(product_id) if product_id else 'Unknown Product'

        # Validate input parameters
        if not job_id or not query_id:
            error_msg = "Missing required parameters: job_id and query_id are required"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg})
            }

        if not isinstance(user_query, str) or len(user_query.strip()) == 0:
            error_msg = "Invalid query: query must be a non-empty string"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
            }

        if len(user_query) > MAX_QUERY_LEN:
            error_msg = f"Query too long: maximum {MAX_QUERY_LEN} characters allowed"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
            }

        # Log orchestration event first (doesn't depend on database)
        try:
            log_orchestration_event(job_id, "task_started", {
                "query": user_query,
                "task_id": None,  # Will be updated later if task creation succeeds
                "product_id": product_id,  # Include product_id in logging
                "pipeline": "perplexity_api_extraction",
                "timestamp": dt.now().isoformat()
            })
        except Exception as log_error:
            print(f"⚠️ Failed to log orchestration event: {log_error}")

        logger.info(f"perplexity start -- job_id: {job_id}, query_id: {query_id}, product_id: {product_id}")

        # Database operations
        try:
            task_id = create_llm_task(job_id, query_id, "perplexity", product_id, product_name)
            logger.info(f"LLM task created and stored -- task_id: {task_id}, job_id: {job_id}, query_id: {query_id}")
            # Update orchestration event with task_id
            log_orchestration_event(job_id, "task_created", {
                "query": user_query,
                "task_id": task_id,
                "product_id": product_id,  # Include product_id in logging
                "pipeline": "perplexity_api_extraction",
                "timestamp": dt.now().isoformat()
            })
        except psycopg2.IntegrityError as e:
            error_msg = f"Database integrity error - duplicate task or invalid foreign key: {str(e)}"
            logger.error(error_msg)
            return {
                'statusCode': 409,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
            }
        except psycopg2.OperationalError as e:
            error_msg = f"Database connection error: {str(e)}"
            logger.error(error_msg)
            return {
                'statusCode': 503,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
            }
        except Exception as e:
            error_msg = f"Failed to create LLM task: {str(e)}"
            logger.error(error_msg)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
            }

        # Update task status to "task received"
        update_task_status(task_id, "task received")

        # Fetch Perplexity API key from Secrets Manager
        try:
            secret_val = get_secret()  # uses default name/region unless overridden by env
            if isinstance(secret_val, str):
                api_key = secret_val.strip()
            elif isinstance(secret_val, dict):
                # Try common keys, then any first value
                api_key = (secret_val.get(DEFAULT_SECRET_NAME)
                           or secret_val.get('PERPLEXITY_API_KEY')
                           or secret_val.get('api_key')
                           or next(iter(secret_val.values()), None))
                if isinstance(api_key, str):
                    api_key = api_key.strip()
            else:
                api_key = None
            if not api_key:
                raise ValueError("API key not found in secret payload")
            logger.info("Successfully retrieved Perplexity API key")
        except Exception as e:
            error_msg = f"Could not fetch Perplexity API key: {str(e)}"
            logger.error(error_msg)
            update_task_status(task_id, "failed", error_message=error_msg)
            return {
                'statusCode': 500,
                'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id, 'task_id': task_id})
            }

        # Get S3 configuration
        s3_bucket = os.environ.get('S3_BUCKET', S3_DEFAULT_BUCKET)
        s3_path = os.environ.get('S3_PATH', S3_DEFAULT_PATH)
        logger.info(f"Using S3 bucket: {s3_bucket}, path: {s3_path}")
        logger.info(f"Processing query: {user_query}")

        # Update task status to "llm task started"
        update_task_status(task_id, "llm task started")

        try:
            # Actual Perplexity API call
            update_task_status(task_id, "running")
            resp_json, status_code = make_perplexity_api_call(user_query, api_key, model=DEFAULT_MODEL)

            if status_code == 200:
                main_content, citations, related_questions = extract_response_data(resp_json)

                # Compose result
                timestamp = _iso_now_tz()
                result = {
                    'task_id': task_id,
                    'job_id': job_id,
                    'query_id': query_id,
                    'product_id': product_id,
                    'query': user_query,
                    'timestamp': timestamp,
                    'main_answer': main_content,
                    'citations': citations,
                    'related_questions': related_questions,
                    'full_response': resp_json
                }

                # Upload to new bucket structure (simplified content)
                new_bucket_path = None
                if product_id:
                    try:
                        perplexity_result = {
                            "job_id": job_id,
                            "product_id": product_id,
                            "query_id": query_id,
                            "query": user_query,
                            "timestamp": timestamp,
                            "model": "PERPLEXITY",
                            "content": main_content,
                            "citations": citations,
                            "related_questions": related_questions,
                            "status": "success"
                        }
                        new_bucket_path = upload_to_new_bucket_structure(
                            perplexity_result, job_id, product_id, query_id, 'application/json', 'json'
                        )
                        if new_bucket_path:
                            logger.info(f"Uploaded to new bucket structure: {new_bucket_path}")
                        else:
                            logger.warning("Failed to upload to new bucket structure")
                    except Exception as new_bucket_error:
                        logger.error(f"Error uploading to new bucket: {new_bucket_error}")

                # Save full result to legacy location
                try:
                    s3_output_path = save_to_s3(result, user_query, s3_bucket, s3_path, task_id)
                    logger.info(f"Successfully saved S3 result for job_id: {job_id}, query_id: {query_id}; S3 path: {s3_output_path}")
                except Exception as e:
                    error_msg = f"Error saving to S3: {str(e)}"
                    logger.error(error_msg)
                    update_task_status(task_id, "failed", error_message=error_msg)
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'error': error_msg,
                            'job_id': job_id,
                            'query_id': query_id,
                            'task_id': task_id
                        })
                    }

                now = dt.now(timezone.utc)
                # Use new bucket path instead of old S3 path (like other lambdas)
                final_s3_path = new_bucket_path if new_bucket_path else s3_output_path
                update_task_status(task_id, "completed", s3_output_path=final_s3_path, completed_at=now)
                logger.info(f"Completed perplexity for job_id: {job_id}, query_id: {query_id}, task_id: {task_id}")
                
                # Log completion event
                try:
                    log_orchestration_event(job_id, "task_completed", {
                        "query": user_query,
                        "task_id": task_id,
                        "product_id": product_id,  # Include product_id in logging
                        "success": True,
                        "pipeline": "perplexity_api_extraction",
                        "timestamp": dt.now().isoformat()
                    })
                except Exception as log_error:
                    print(f"⚠️ Failed to log completion event: {log_error}")

                # Return combined response
                if new_bucket_path:
                    result['new_bucket_path'] = new_bucket_path
                    
                print(f"🎯 Perplexity API extraction complete!")
                print(f"📊 Key Enhancement: New bucket structure upload to bodhium-temp bucket")
                print(f"📁 New S3 path: {new_bucket_path}" if new_bucket_path else "")

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'success': True,
                        'task_id': task_id,
                        'job_id': job_id,
                        'query_id': query_id,
                        'product_id': product_id,
                        'data': result,
                        's3_saved': True,
                        's3_path': s3_output_path,
                        'new_bucket_saved': bool(new_bucket_path),
                        'new_bucket_path': new_bucket_path
                    }, ensure_ascii=False)
                }

            else:
                # Non-200 from Perplexity
                api_error = resp_json.get('error', 'Unknown API error')
                error_msg = f'Perplexity API error (status {status_code}): {api_error}'
                logger.error(error_msg)
                update_task_status(task_id, "failed", error_message=error_msg)
                
                # Log failure event
                try:
                    log_orchestration_event(job_id, "task_failed", {
                        "error": error_msg,
                        "task_id": task_id,
                        "product_id": product_id,
                        "pipeline": "perplexity_api_extraction",
                        "timestamp": dt.now().isoformat()
                    })
                except:
                    pass
                
                return {
                    'statusCode': status_code,
                    'body': json.dumps({
                        'error': f'Perplexity API error: {status_code}',
                        'message': api_error,
                        'job_id': job_id,
                        'query_id': query_id,
                        'task_id': task_id
                    })
                }

        except Exception as e:
            error_msg = f'Error during API processing: {str(e)}'
            logger.error(error_msg, exc_info=True)
            update_task_status(task_id, "failed", error_message=error_msg)
            
            # Log failure event
            try:
                log_orchestration_event(job_id, "task_failed", {
                    "error": error_msg,
                    "task_id": task_id,
                    "product_id": product_id,
                    "pipeline": "perplexity_api_extraction",
                    "timestamp": dt.now().isoformat()
                })
            except:
                pass
            
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': error_msg,
                    'job_id': job_id,
                    'query_id': query_id,
                    'task_id': task_id
                })
            }

    except json.JSONDecodeError as e:
        error_msg = f"Invalid JSON in event: {str(e)}"
        logger.error(error_msg)
        if task_id:
            update_task_status(task_id, "failed", error_message=error_msg)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': error_msg})
        }

    except KeyError as e:
        error_msg = f"Missing required key in event: {str(e)}"
        logger.error(error_msg)
        if task_id:
            update_task_status(task_id, "failed", error_message=error_msg)
        return {
            'statusCode': 400,
            'body': json.dumps({'error': error_msg, 'job_id': job_id, 'query_id': query_id})
        }

    except Exception as e:
        error_msg = f'Unexpected error in lambda handler: {str(e)}'
        logger.error(error_msg, exc_info=True)
        print(f"❌ Lambda handler error: {e}")
        if task_id:
            try:
                update_task_status(task_id, "failed", error_message=error_msg)
            except:
                pass
        
        # Log failure event
        if job_id:
            try:
                log_orchestration_event(job_id, "task_failed", {
                    "error": error_msg,
                    "task_id": task_id,
                    "product_id": event.get('product_id'),  # Include product_id in failure logging
                    "pipeline": "perplexity_api_extraction",
                    "timestamp": dt.now().isoformat()
                })
            except:
                pass
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': error_msg,
                'job_id': job_id,
                'query_id': query_id,
                'task_id': task_id
            })
        }

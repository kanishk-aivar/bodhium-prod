import os
import boto3
import botocore
import logging
import re  # Added for query pattern matching
import json
import uuid
import psycopg2
import time
from decimal import Decimal
from datetime import datetime, timezone


# DynamoDB table for file tracking
FILE_TRACKING_TABLE = os.environ.get('FILE_TRACKING_TABLE', 'Adhoc-Query-Tracker')


# RDS/DB & test product-id settings - ALWAYS Adhoc_trigger mode
TEST_PRODUCT_ID = int(os.environ.get('TEST_PRODUCT_ID', 99999999))
FORCE_FANOUT = True  # Always force fanout for all 4 LLMs!


# S3 and job ID settings
JOB_ID_BUCKET = os.environ.get('JOB_ID_BUCKET')
JOB_ID_PATH = os.environ.get('JOB_ID_PATH')
# LAMBDA_RESULTS_FORMATTER = os.environ.get('LAMBDA_RESULTS_FORMATTER')
# RESULT_TRIGGER_TIME = int(os.environ.get('RESULT_TRIGGER_TIME', 13))  # Default 13 minutes


# Structured logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


# AWS Services
dynamodb = boto3.resource('dynamodb')
orchestration_table = dynamodb.Table(os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs'))
file_tracking_table = dynamodb.Table(FILE_TRACKING_TABLE)
s3_client = boto3.client('s3')


# Secrets Manager
SECRET_NAME = os.environ.get('RDS_DB', 'dev/rds')
REGION_NAME = os.environ.get('AWS_REGION', 'us-east-1')
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)


# Lambda ARNs (must never be blank)
TARGETS = {
    "chatgpt": os.environ["LAMBDA_CHATGPT_V1"],
    "aio": os.environ["LAMBDA_AIOVERVIEW"], 
    "aim": os.environ["LAMBDA_AIMODE"],
    "perplexity": os.environ["LAMBDA_PERPLEXITYAPI"]
}


lambda_boto_config = botocore.config.Config(read_timeout=900, connect_timeout=60)
lambda_client = boto3.client("lambda", config=lambda_boto_config)


def read_json_from_s3(bucket, key):
    """Read JSON content from S3 object"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.error(f"Failed to read JSON from S3 {bucket}/{key}: {e}")
        raise


def store_job_id_to_s3(job_id, bucket, path):
    """Store job_id to S3 at specified path"""
    try:
        if not bucket or not path:
            logger.warning("JOB_ID_BUCKET or JOB_ID_PATH not configured, skipping job_id storage")
            return
        
        # Create timestamp for the file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{path.rstrip('/')}/job_{job_id}_{timestamp}.json"
        
        job_data = {
            "job_id": job_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "created"
        }
        
        s3_client.put_object(
            Bucket=bucket,
            Key=filename,
            Body=json.dumps(job_data, indent=2),
            ContentType='application/json'
        )
        logger.info(f"Stored job_id {job_id} to S3: {bucket}/{filename}")
    except Exception as e:
        logger.error(f"Failed to store job_id to S3: {e}")


def check_and_mark_file_processed(bucket, key):
    """
    Check if file has already been processed based on S3 key and last modified timestamp.
    If not processed, mark it as processed. Returns True if already processed, False if new.
    """
    try:
        # Get S3 object metadata to extract last modified timestamp
        response = s3_client.head_object(Bucket=bucket, Key=key)
        last_modified = response['LastModified']
        
        # Format timestamp for consistency (ISO format)
        timestamp_str = last_modified.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Create DynamoDB keys
        pk = f"S3_OBJECT#{key}"
        sk = f"TIMESTAMP#{timestamp_str}"
        
        logger.info(f"Checking file processing status: {pk}, {sk}")
        
        # Check if this file+timestamp combination already exists
        try:
            existing_item = file_tracking_table.get_item(
                Key={
                    'pk': pk,
                    'sk': sk
                }
            )
            
            if existing_item.get('Item'):
                logger.info(f"File {key} with timestamp {timestamp_str} has already been processed. Skipping.")
                return True
                
        except Exception as e:
            logger.error(f"Error checking for existing file record: {str(e)}")
            # Continue processing even if check fails
            
        # File hasn't been processed, mark it as processed using conditional put
        try:
            file_tracking_table.put_item(
                Item={
                    'pk': pk,
                    'sk': sk,
                    'bucket': bucket,
                    'key': key,
                    'last_modified': timestamp_str,
                    'processed_at': datetime.now(timezone.utc).isoformat(),
                    'status': 'processing'
                },
                ConditionExpression='attribute_not_exists(pk)'  # Only insert if doesn't exist
            )
            logger.info(f"Marked file as processing: {key} with timestamp {timestamp_str}")
            return False
            
        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            # Another concurrent execution already marked this file
            logger.info(f"File {key} was concurrently marked by another execution. Skipping.")
            return True
            
    except Exception as e:
        logger.error(f"Error in file processing check: {str(e)}")
        # In case of error, allow processing to continue (fail-open approach)
        return False


def update_file_processing_status(bucket, key, status, job_id=None):
    """Update the processing status of a file"""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        last_modified = response['LastModified']
        timestamp_str = last_modified.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        pk = f"S3_OBJECT#{key}"
        sk = f"TIMESTAMP#{timestamp_str}"
        
        update_expression = "SET #status = :status, updated_at = :updated_at"
        expression_values = {
            ':status': status,
            ':updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        if job_id:
            update_expression += ", job_id = :job_id"
            expression_values[':job_id'] = job_id
            
        file_tracking_table.update_item(
            Key={'pk': pk, 'sk': sk},
            UpdateExpression=update_expression,
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues=expression_values
        )
        logger.info(f"Updated file status to {status}: {key}")
        
    except Exception as e:
        logger.error(f"Error updating file status: {str(e)}")


# def trigger_delayed_lambda(job_id, start_time):
#     """Trigger the results formatter Lambda after specified delay"""
#     try:
#         if not LAMBDA_RESULTS_FORMATTER:
#             logger.warning("LAMBDA_RESULTS_FORMATTER not configured, skipping delayed trigger")
#             return
#         
#         # Calculate delay in seconds
#         delay_seconds = RESULT_TRIGGER_TIME * 60  # Convert minutes to seconds
#         
#         # Calculate when to trigger (13th minute from start)
#         trigger_time = start_time + timedelta(minutes=RESULT_TRIGGER_TIME)
#         current_time = datetime.now(timezone.utc)
#         
#         # Wait until the specified time
#         wait_seconds = (trigger_time - current_time).total_seconds()
#         logger.info(f"Waiting {wait_seconds} seconds before triggering results formatter Lambda")
#         time.sleep(wait_seconds)
#         
#         # Trigger the results formatter Lambda
#         payload = {
#             "job_id": job_id,
#             "triggered_at": datetime.now(timezone.utc).isoformat(),
#             "source": "orchestrator_delayed_trigger"
#         }
#         
#         lambda_client.invoke(
#             FunctionName=LAMBDA_RESULTS_FORMATTER,
#             InvocationType="Event",
#             Payload=json.dumps(payload)
#         )
#         
#         logger.info(f"Triggered delayed Lambda {LAMBDA_RESULTS_FORMATTER} for job_id: {job_id}")
#         
#     except Exception as e:
#         logger.error(f"Failed to trigger delayed Lambda: {e}")


def get_rds_connection():
    try:
        secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret_response['SecretString'])
        logger.info(f"Fetched RDS secrets for {SECRET_NAME}")
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=secret['DB_PORT']
        )
        logger.info(f"Connected to RDS PostgreSQL {secret['DB_NAME']}@{secret['DB_HOST']}:{secret['DB_PORT']}")
        return conn
    except Exception as e:
        logger.error(f"Failed connecting to RDS: {e}")
        raise


def insert_scrapejob(job_id, source_url, status):
    conn = get_rds_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scrapejobs (job_id, source_url, status, brand_name, created_at, updated_at) VALUES (%s, %s, %s, %s, NOW(), NOW())",
                (job_id, source_url, status, "Ad-hoc")
            )
            logger.info(f"Inserted scrapejob: {job_id}, URL: {source_url}, Status: {status}, Brand: Ad-hoc")
    except Exception as e:
        logger.error(f"Failed to insert scrapejob: {e}")
        raise
    finally:
        conn.close()


def insert_query(query_text, query_type, product_id, is_active=True):
    conn = get_rds_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO queries (query_text, query_type, product_id, is_active) VALUES (%s, %s, %s, %s) RETURNING query_id",
                (query_text, query_type, product_id, is_active)
            )
            query_id = cur.fetchone()[0]
            logger.info(f"Inserted query: {query_text} type {query_type} for product_id {product_id}, query_id={query_id}")
            return query_id
    except Exception as e:
        logger.error(f"Failed to insert query: {e}")
        return None
    finally:
        conn.close()


class OrchestrationLogger:
    def __init__(self, table_name=None):
        self.table_name = table_name or os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')
        self.table = dynamodb.Table(self.table_name)
        logger.info(f"OrchestrationLogger initialized with table: {self.table_name}")


    def convert_to_dynamodb_format(self, data):
        if isinstance(data, dict):
            return {k: self.convert_to_dynamodb_format(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.convert_to_dynamodb_format(item) for item in data]
        elif isinstance(data, float):
            return Decimal(str(data))
        elif isinstance(data, int):
            return data
        elif data is None:
            return "null"
        else:
            return str(data)


    def create_event_timestamp_id(self):
        timestamp = datetime.now(timezone.utc).isoformat()
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}#{unique_id}"


    def log_event(self, job_id, event_name, details=None):
        try:
            event_timestamp_id = self.create_event_timestamp_id()
            item = {
                'pk': f"JOB#{job_id}",
                'sk': event_timestamp_id,
                'job_id': job_id,
                'event_timestamp_id': event_timestamp_id,
                'eventName': event_name,
                'details': self.convert_to_dynamodb_format(details or {}),
                'status': details.get('status', 'created') if details else 'created'
            }
            self.table.put_item(Item=item)
            logger.info(f"Logged event: {event_name} for job: {job_id} | {details}")
        except Exception as e:
            logger.error(f"Failed to log event: {e}")


orchestration_logger = OrchestrationLogger()


# Model matching patterns (replicated from main orchestrator)
MATCH_TABLE = [
    ("chatgpt", ["chatgpt"]),
    ("aio", ["aio based ai overview", "ai overview", "aioverview", "aio"]),
    ("aim", ["aim", "ai mode"]),
    ("perplexity", ["perplexity"])
]

def extract_models_from_query(query):
    """Extract models from query - replicated from main orchestrator"""
    query_lower = query.lower()
    models_found = []
    logger.info(f"Processing query: '{query}'")

    # Explicit ::tag parsing
    tag_match = re.search(r'::\s*([\w\s,]+)\$', query_lower)
    if tag_match:
        raw_models = tag_match.group(1)
        parsed_models = [m.strip() for m in raw_models.split(",")]
        logger.info(f"Found explicit model tags: {parsed_models}")
        for m in parsed_models:
            if m in TARGETS:
                models_found.append(m)
        if models_found:
            logger.info(f"Explicitly routed models: {models_found}")
            return models_found

    # Natural language keyword fallback
    logger.info("No explicit tags found. Trying keyword-based model inference.")
    for key, keywords in MATCH_TABLE:
        if any(kw in query_lower for kw in keywords):
            models_found.append(key)
    if models_found:
        unique_models = list(set(models_found))
        logger.info(f"Inferred models from keywords: {unique_models}")
        return unique_models

    # No match → Fan-out fallback (ALWAYS for Adhoc_trigger mode)
    logger.info("No explicit or inferred models. Triggering fan-out to all (Adhoc mode default).")
    return []

def get_model_name_from_arn(lambda_arn: str) -> str:
    """Extract model name from lambda ARN for event naming - replicated from main orchestrator"""
    function_name = lambda_arn.split(":")[-1]
    
    # Map function names to model names for consistent event naming
    model_mapping = {
        "bodhium-llm-chatgpt-v1": "Chatgpt",
        "bodhium-llm-perplexityapi": "Perplexity", 
        "bodhium-aimode-brightdata": "Aim",
        "BodhiumAiOverview": "Aio"
    }
    
    return model_mapping.get(function_name, function_name.replace("bodhium-llm-", "").title())

def trigger_lambda(lambda_arn, payload_dict, job_id: str, query_id: int = None):
    """Trigger lambda without waiting for response - replicated from main orchestrator"""
    try:
        model_name = get_model_name_from_arn(lambda_arn)
        
        # Map lambda ARN to model key
        model_key = None
        for key, arn in TARGETS.items():
            if arn == lambda_arn:
                model_key = key
                break
        
        payload = json.dumps(payload_dict)
        
        logger.info(f"Triggering Lambda: {lambda_arn} with payload: {payload}")
        orchestration_logger.log_event(job_id, f"{model_name}LambdaTriggered", {
            "lambda_arn": lambda_arn,
            "function_name": lambda_arn.split(":")[-1],
            "payload_size": len(str(payload)),
            "query_id": query_id
        })
        
        # Trigger lambda asynchronously (Event invocation type)
        lambda_client.invoke(
            FunctionName=lambda_arn,
            InvocationType="Event",  # Async call - don't wait for response
            Payload=payload
        )
        
        logger.info(f"Lambda triggered successfully: {lambda_arn}")
        return True
        
    except Exception as e:
        model_name = get_model_name_from_arn(lambda_arn)
        logger.error(f"Error triggering Lambda {lambda_arn}: {e}")
        orchestration_logger.log_event(job_id, f"{model_name}LambdaTriggerFailed", {
            "lambda_arn": lambda_arn,
            "function_name": lambda_arn.split(":")[-1],
            "error": str(e),
            "query_id": query_id
        })
        return False


def process_queries(job_id, query_objs, options, static_product_id=None, start_time=None):
    # Log processing start with product information
    orchestration_logger.log_event(job_id, "ProductProcessingStarted", {
        "product_id": static_product_id,
        "query_count": len(query_objs),
        "existing_queries_count": 0,  # No existing queries in Adhoc mode
        "new_queries_count": len(query_objs),
        "old_format_queries_count": 0  # Using new format
    })
    
    orchestration_logger.log_event(job_id, "AsyncProcessingStarted", {
        "queries_count": len(query_objs),
        "options": options,
        "mode": "Adhoc_trigger",
        "status": "processing"
    })
    results = []
    for i, query_obj in enumerate(query_objs):
        q, query_id = query_obj
        
        # Always use TEST_PRODUCT_ID for Adhoc_trigger mode
        product_id = TEST_PRODUCT_ID
        
        orchestration_logger.log_event(job_id, "AsyncQueryProcessingStarted", {
            "query_id": query_id,
            "product_id": product_id,  # Include product_id for consistency
            "query": q,
            "query_index": i + 1,
            "total_queries": len(query_objs),
            "mode": "Adhoc_trigger",
            "status": "processing"
        })
        model_keys_fanned_out = []
        fanout_results = []
        
        # Model selection using replicated logic (but always fanout in Adhoc mode)
        selected_models = extract_models_from_query(q)
        
        # OVERRIDE: Always fanout in Adhoc_trigger mode regardless of query content
        logger.info(f"Adhoc mode: Forcing fanout to all models regardless of query content for query {query_id}")
        selected_models = []  # Empty list forces fanout to all
        
        orchestration_logger.log_event(job_id, "ModelSelectionCompleted", {
            "query_id": query_id,
            "product_id": product_id,
            "query": q,
            "query_type": "Adhoc_trigger",
            "selected_models": selected_models,
            "selection_method": "fanout_forced_adhoc_mode"
        })
        
        # Log fanout start
        orchestration_logger.log_event(job_id, "FanoutStarted", {
            "query_id": query_id,
            "product_id": product_id,
            "query": q,
            "query_type": "Adhoc_trigger",
            "target_count": len(TARGETS)
        })
        
        # Fan-out to all models (replicated pattern from main orchestrator)
        logger.info(f"Fan-out initiated for Adhoc query {query_id} (product {product_id}): '{q}'")
        
        for key, arn in TARGETS.items():
            model_keys_fanned_out.append(key)
            payload = {
                "query": q,
                "job_id": job_id,
                "query_id": query_id,
                "product_id": product_id
            }
            success = trigger_lambda(arn, payload, job_id, query_id)
            fanout_results.append({"function": key, "status": "invoked" if success else "failed"})
        
        orchestration_logger.log_event(job_id, "FanoutCompleted", {
            "query_id": query_id,
            "product_id": product_id,
            "query": q,
            "query_type": "Adhoc_trigger",
            "target_count": len(TARGETS)
        })
        results.append({
            "query": q,
            "fanout_results": fanout_results
        })


        # Add 3-second delay after each query's fanout (except for the last query)
        if i < len(query_objs) - 1:
            logger.info(f"[RATE_LIMIT] Applying 3-second delay after query {i + 1}/{len(query_objs)} fanout batch")
            orchestration_logger.log_event(job_id, "RateLimitDelayStarted", {
                "query_id": query_id,
                "query_index": i + 1,
                "total_queries": len(query_objs),
                "delay_seconds": 3,
                "reason": "API rate limit prevention",
                "status": "delaying"
            })
            
            time.sleep(3)  # 3-second delay
            
            orchestration_logger.log_event(job_id, "RateLimitDelayCompleted", {
                "query_id": query_id,
                "query_index": i + 1,
                "total_queries": len(query_objs),
                "delay_seconds": 3,
                "reason": "API rate limit prevention",
                "status": "delay_completed"
            })
            logger.info(f"[RATE_LIMIT] Completed 3-second delay after query {i + 1}/{len(query_objs)}")


    # Store job_id to S3
    store_job_id_to_s3(job_id, JOB_ID_BUCKET, JOB_ID_PATH)
    
    # Trigger delayed Lambda for results formatting
    # if start_time:
    #     trigger_delayed_lambda(job_id, start_time)
    
    final_response = {
        "job_id": job_id,
        "queries_processed": len(query_objs),
        "results": results
    }
    # Log product processing completed
    orchestration_logger.log_event(job_id, "ProductProcessingCompleted", {
        "product_id": static_product_id,
        "queries_processed": len(query_objs),
        "existing_queries_processed": 0,
        "new_queries_processed": len(query_objs),
        "old_format_queries_processed": 0
    })
    
    orchestration_logger.log_event(job_id, "AsyncProcessingCompleted", {
        "queries_processed": len(query_objs),
        "total_results": len(results),
        "final_response_size": len(json.dumps(final_response)),
        "mode": "Adhoc_trigger",
        "status": "completed"
    })
    logger.info(f"[ASYNC] Completed processing for job {job_id}. Results: {json.dumps(final_response)}")
    return final_response


def lambda_handler(event, context):
    # Record start time for delayed Lambda trigger (commented out for now)
    # start_time = datetime.now(timezone.utc)
    
    # ALWAYS Adhoc_trigger mode - no other mode supported
    mode = "Adhoc_trigger"
    job_id = str(uuid.uuid4())


    logger.info(f"Received event: {event}")
    
    orchestration_logger.log_event(job_id, "OrchestrationStarted", {
        "event": event,
        "context": {
            "function_name": context.function_name,
            "function_version": context.function_version,
            "invoked_function_arn": context.invoked_function_arn,
            "memory_limit_in_mb": context.memory_limit_in_mb,
            "remaining_time_in_millis": context.get_remaining_time_in_millis()
        },
        "mode": mode,
        "status": "created"
    })
    
    queries = []
    options = {}
    bucket = None
    key = None
    
    try:
        # Handle S3 event (file upload)
        if 'Records' in event and event['Records']:
            s3_record = event['Records'][0]
            if s3_record.get('eventName', '').startswith('ObjectCreated'):
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                
                logger.info(f"Processing S3 upload: {bucket}/{key}")
                
                # CRITICAL: Check if file has already been processed
                if check_and_mark_file_processed(bucket, key):
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': 'File already processed',
                            'bucket': bucket,
                            'key': key,
                            'job_id': job_id,
                            'mode': mode
                        })
                    }
                
                # Read JSON content from S3
                s3_data = read_json_from_s3(bucket, key)
                queries = s3_data.get("queries", [])
                options = s3_data.get("options", {})
                
                logger.info(f"Read {len(queries)} queries from S3 file")
        
        # Fallback to direct event input (for backward compatibility)
        if not queries:
            if 'body' in event and event['body']:
                body = event['body']
                if isinstance(body, str): 
                    body = json.loads(body)
                queries = body.get("queries")
                options = body.get("options", {})
            else:
                queries = event.get("queries") or event.get("query", [])
                options = event.get("options", {})
                
    except (json.JSONDecodeError, AttributeError) as e:
        orchestration_logger.log_event(job_id, "OrchestrationFailed", {
            "error": f"Failed to parse request body: {e}",
            "mode": mode,
            "status": "failed"
        })
        logger.error(f"Failed to parse request body: {e}")
        
        # Update file status to failed if it was an S3 trigger
        if bucket and key:
            update_file_processing_status(bucket, key, "failed", job_id)
            
        return {"error": "Invalid JSON in request body"}
    
    if not queries:
        orchestration_logger.log_event(job_id, "OrchestrationFailed", {
            "error": "No queries provided in the event",
            "mode": mode,
            "status": "failed"
        })
        logger.error("No queries provided")
        
        # Update file status to failed if it was an S3 trigger
        if bucket and key:
            update_file_processing_status(bucket, key, "failed", job_id)
            
        return {"error": "No queries provided"}
    
    if not isinstance(queries, list):
        queries = [queries]
    
    # Always use TEST_PRODUCT_ID for Adhoc_trigger mode
    static_product_id = TEST_PRODUCT_ID
    query_objs = []
    
    try:
        insert_scrapejob(job_id, event.get("source_url", "test_source_url"), "llm_generated")
        for q in queries:
            # Always use "Adhoc_trigger" as query_type
            query_id = insert_query(q, "Adhoc_trigger", static_product_id, True)
            query_objs.append((q, query_id))
        logger.info(f"[ADHOC_TRIGGER MODE] Created scrapejob {job_id} and queries {[t[1] for t in query_objs]}")
        
        # Update file status to processing
        if bucket and key:
            update_file_processing_status(bucket, key, "processing", job_id)
            
    except Exception as e:
        logger.error(f"[ADHOC_TRIGGER MODE] Error creating job/query: {str(e)}")
        orchestration_logger.log_event(job_id, "TestDataSetupFailed", {
            "error": str(e),
            "mode": mode,
            "status": "failed"
        })
        
        # Update file status to failed if it was an S3 trigger
        if bucket and key:
            update_file_processing_status(bucket, key, "failed", job_id)
            
        return {"error": f"Data setup failed: {str(e)}"}
    
    orchestration_logger.log_event(job_id, "TestDataSetupSucceeded", {
        "job_id": job_id,
        "query_ids": [t[1] for t in query_objs],
        "mode": mode,
        "status": "created"
    })
    
    # Process queries synchronously - always in Adhoc_trigger mode
    final_response = process_queries(job_id, query_objs, options, static_product_id, None)
    
    # Log orchestration completion (matching main orchestrator pattern)
    orchestration_logger.log_event(job_id, "OrchestrationCompleted", {
        "product_groups_processed": 1,  # Always 1 product group in Adhoc mode
        "total_queries": len(query_objs)
    })
    
    logger.info(f"Completed processing for job {job_id}. All lambdas triggered.")
    
    # Update file status to completed
    if bucket and key:
        update_file_processing_status(bucket, key, "completed", job_id)
    
    # Return 202 Accepted response (matching main orchestrator)
    return {
        "statusCode": 202,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "POST, OPTIONS"
        },
        "body": json.dumps({
            "status": "accepted",
            "message": "Job submitted successfully. All lambdas have been triggered.",
            "job_id": job_id,
            "product_groups_count": 1,
            "total_queries_count": len(query_objs),
            "mode": "Adhoc_trigger",
            "polling_info": {
                "table_name": os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs'),
                "query_example": f"SELECT * FROM {os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')} WHERE job_id = '{job_id}'"
            }
        })
    }
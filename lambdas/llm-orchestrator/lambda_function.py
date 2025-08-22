import os
import boto3
import logging
import re
import json
import uuid
import psycopg2
from datetime import datetime, timezone
from decimal import Decimal

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb')
orchestration_table = dynamodb.Table(os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs'))

# Target Lambda ARNs (set via environment variables)
TARGETS = {
    "chatgpt": os.environ.get("LAMBDA_CHATGPT", "arn:aws:lambda:us-east-1:127214200395:function:bodhium-llm-chatgpt-v1"),
    "aio": os.environ.get("LAMBDA_AIOVERVIEW", "arn:aws:lambda:us-east-1:127214200395:function:BodhiumAiOverview"),
    "aim": os.environ.get("LAMBDA_AIMODE", "arn:aws:lambda:us-east-1:127214200395:function:bodhium-aimode-brightdata"),
    "perplexity": os.environ.get("LAMBDA_PERPLEXITYAPI", "arn:aws:lambda:us-east-1:127214200395:function:bodhium-llm-perplexityapi")
}

MATCH_TABLE = [
    ("chatgpt", ["chatgpt"]),
    ("aio", ["aio based ai overview", "ai overview", "aioverview", "aio"]),
    ("aim", ["aim", "ai mode"]),
    ("perplexity", ["perplexity"])
]

# Configure boto3 Lambda client
lambda_client = boto3.client("lambda")

def get_db_connection():
    """Get database connection using AWS Secrets Manager"""
    try:
        secrets_client = boto3.client('secretsmanager')
        secret_name = os.environ.get('DB_SECRET_NAME', 'rds-db-credentials/cluster-bodhium/bodhium')
        
        secret_response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(secret_response['SecretString'])
        
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=secret.get('DB_PORT', 5432)
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def insert_new_query(product_id, query_text, query_type="custom"):
    """Insert a new query into the queries table and return the generated query_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO queries (product_id, query_text, query_type, is_active)
            VALUES (%s, %s, %s, %s)
            RETURNING query_id
            """,
            (product_id, query_text, query_type, True)
        )
        
        query_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Inserted new query into database: query_id={query_id}, product_id={product_id}")
        return query_id
        
    except Exception as e:
        logger.error(f"Failed to insert new query into database: {e}")
        raise

class OrchestrationLogger:
    """DynamoDB logger for orchestration workflow events"""
    def __init__(self, table_name: str = None):
        self.table_name = table_name or os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')
        self.table = dynamodb.Table(self.table_name)
        logger.info(f"OrchestrationLogger initialized with table: {self.table_name}")

    def convert_to_dynamodb_format(self, data: any) -> any:
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

    def create_event_timestamp_id(self) -> str:
        timestamp = datetime.now(timezone.utc).isoformat()
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}#{unique_id}"

    def log_event(self, job_id: str, event_name: str, details: dict = None) -> None:
        try:
            event_timestamp_id = self.create_event_timestamp_id()
            item = {
                'pk': f"JOB#{job_id}",
                'sk': event_timestamp_id,
                'job_id': job_id,
                'event_timestamp_id': event_timestamp_id,
                'eventName': event_name,
                'details': self.convert_to_dynamodb_format(details or {})
            }
            self.table.put_item(Item=item)
            logger.info(f"Logged event: {event_name} for job: {job_id}")
        except Exception as e:
            logger.error(f"Failed to log event: {e}")

# Initialize the logger
orchestration_logger = OrchestrationLogger()

def get_model_name_from_arn(lambda_arn: str) -> str:
    """Extract model name from lambda ARN for event naming"""
    function_name = lambda_arn.split(":")[-1]
    
    # Map function names to model names for consistent event naming
    model_mapping = {
        "bodhium-llm-chatgpt": "Chatgpt",
        "bodhium-llm-perplexityapi": "Perplexity", 
        "bodhium-llm-aimode": "Aim",
        "bodhium-llm-aioverview": "Aio"
    }
    
    return model_mapping.get(function_name, function_name.replace("bodhium-llm-", "").title())

def extract_models_from_query(query):
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

    # No match → Fan-out fallback
    logger.info("No explicit or inferred models. Triggering fan-out to all.")
    return []

def trigger_lambda(lambda_arn, payload_dict, job_id: str, query_id: int = None):
    """Trigger lambda without waiting for response"""
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

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    # Extract job_id from event or generate one
    job_id = None
    if 'body' in event and event['body']:
        body = event['body']
        if isinstance(body, str):
            body = json.loads(body)
        job_id = body.get("job_id")
    else:
        job_id = event.get("job_id")
    
    # Generate job_id if not provided
    if not job_id:
        job_id = str(uuid.uuid4())
        logger.info(f"Generated new job_id: {job_id}")
    else:
        logger.info(f"Using provided job_id: {job_id}")
    
    orchestration_logger.log_event(job_id, "OrchestrationStarted", {
        "event": event,
        "context": {
            "function_name": context.function_name,
            "function_version": context.function_version,
            "invoked_function_arn": context.invoked_function_arn,
            "memory_limit_in_mb": context.memory_limit_in_mb,
            "remaining_time_in_millis": context.get_remaining_time_in_millis()
        }
    })

    # Parse the JSON body from API Gateway event
    try:
        if 'body' in event and event['body']:
            body = event['body']
            if isinstance(body, str):
                body = json.loads(body)
            
            # NEW FORMAT: Check for selected_queries
            selected_queries = body.get("selected_queries")
            options = body.get("options", {})
            
            # BACKWARDS COMPATIBILITY: Support old format
            if not selected_queries:
                queries = body.get("queries")
                if queries:
                    # Convert old format to new format
                    selected_queries = [{
                        "product_id": str(uuid.uuid4()),  # Generate product_id for backwards compatibility
                        "queries": queries if isinstance(queries, list) else [queries]
                    }]
        else:
            # Fallback for direct Lambda invocation
            selected_queries = event.get("selected_queries")
            if not selected_queries:
                queries = event.get("query") or event.get("queries")
                if queries:
                    selected_queries = [{
                        "product_id": str(uuid.uuid4()),
                        "queries": queries if isinstance(queries, list) else [queries]
                    }]
            options = event.get("options", {})
    except (json.JSONDecodeError, AttributeError) as e:
        logger.error(f"Failed to parse request body: {e}")
        orchestration_logger.log_event(job_id, "OrchestrationFailed", {
            "error": f"Failed to parse request body: {e}"
        })
        return {"error": "Invalid JSON in request body"}

    if not selected_queries:
        logger.warning("No selected_queries provided in the event.")
        orchestration_logger.log_event(job_id, "OrchestrationFailed", {
            "error": "No selected_queries provided in the event"
        })
        return {"error": "No selected_queries provided"}

    # Process selected_queries and trigger lambdas
    # Calculate total queries (both existing and new)
    total_queries = 0
    for sq in selected_queries:
        total_queries += len(sq.get("queries", []))  # Old format compatibility
        total_queries += len(sq.get("existing_queries", []))  # New format: existing queries
        total_queries += len(sq.get("new_queries", []))  # New format: new queries
    
    logger.info(f"Processing {len(selected_queries)} product groups with {total_queries} total queries for job {job_id}")
    
    # Find highest existing query_id to avoid conflicts when generating new ones
    max_existing_query_id = 0
    for product_group in selected_queries:
        existing_queries = product_group.get("existing_queries", [])
        for eq in existing_queries:
            if isinstance(eq, dict) and "query_id" in eq:
                max_existing_query_id = max(max_existing_query_id, eq["query_id"])
    
    query_counter = max_existing_query_id  # Start new query IDs after existing ones
    
    for product_group in selected_queries:
        product_id = product_group.get("product_id")
        
        # Handle different query formats
        old_format_queries = product_group.get("queries", [])  # Backward compatibility
        existing_queries = product_group.get("existing_queries", [])
        new_queries = product_group.get("new_queries", [])
        
        if not product_id:
            logger.warning(f"Skipping product group without product_id: {product_group}")
            continue
            
        total_product_queries = len(old_format_queries) + len(existing_queries) + len(new_queries)
        if total_product_queries == 0:
            logger.warning(f"Skipping product_id {product_id} with no queries")
            continue
            
        logger.info(f"Processing product_id {product_id} with {total_product_queries} queries (existing: {len(existing_queries)}, new: {len(new_queries)}, old_format: {len(old_format_queries)})")
        
        orchestration_logger.log_event(job_id, "ProductProcessingStarted", {
            "product_id": product_id,
            "query_count": total_product_queries,
            "existing_queries_count": len(existing_queries),
            "new_queries_count": len(new_queries),
            "old_format_queries_count": len(old_format_queries)
        })
        
        # Process existing queries (with their original query_ids)
        for existing_query in existing_queries:
            if isinstance(existing_query, dict) and "query_id" in existing_query and "query_text" in existing_query:
                query_id = existing_query["query_id"]
                query_text = existing_query["query_text"]
                logger.info(f"Processing existing query {query_id} for product {product_id}: '{query_text}'")
                
                selected_models = extract_models_from_query(query_text)
                orchestration_logger.log_event(job_id, "ModelSelectionCompleted", {
                    "query_id": query_id,
                    "product_id": product_id,
                    "query": query_text,
                    "query_type": "existing",
                    "selected_models": selected_models,
                    "selection_method": "explicit" if selected_models else "fanout"
                })

                if selected_models:
                    # Route to specific models
                    logger.info(f"Routing existing query {query_id} (product {product_id}) to models: {selected_models}")
                    for model in selected_models:
                        lambda_arn = TARGETS[model]
                        trigger_lambda(
                            lambda_arn, 
                            {
                                "query": query_text, 
                                "job_id": job_id, 
                                "query_id": query_id,
                                "product_id": product_id
                            }, 
                            job_id,
                            query_id
                        )
                else:
                    # Fan-out to all models
                    logger.info(f"Fan-out initiated for existing query {query_id} (product {product_id}): '{query_text}'")
                    orchestration_logger.log_event(job_id, "FanoutStarted", {
                        "query_id": query_id,
                        "product_id": product_id,
                        "query": query_text,
                        "query_type": "existing",
                        "target_count": len(TARGETS)
                    })
                    
                    for key, arn in TARGETS.items():
                        trigger_lambda(
                            arn,
                            {
                                "query": query_text, 
                                "job_id": job_id, 
                                "query_id": query_id,
                                "product_id": product_id
                            },
                            job_id,
                            query_id
                        )
                    
                    orchestration_logger.log_event(job_id, "FanoutCompleted", {
                        "query_id": query_id,
                        "product_id": product_id,
                        "query": query_text,
                        "query_type": "existing",
                        "target_count": len(TARGETS)
                    })
            else:
                logger.warning(f"Invalid existing_query format: {existing_query}")
        
        # Process new queries (insert into database first to get query_ids)
        for query_text in new_queries:
            try:
                # Insert new query into database and get the auto-generated query_id
                query_id = insert_new_query(product_id, query_text, "custom")
                logger.info(f"Processing new query {query_id} for product {product_id}: '{query_text}'")
            except Exception as db_error:
                logger.error(f"Failed to insert new query into database: {db_error}")
                # Fallback to generating query_id like before
                query_counter += 1
                query_id = query_counter
                logger.warning(f"Using fallback query_id {query_id} for product {product_id}: '{query_text}'")
            
            selected_models = extract_models_from_query(query_text)
            orchestration_logger.log_event(job_id, "ModelSelectionCompleted", {
                "query_id": query_id,
                "product_id": product_id,
                "query": query_text,
                "query_type": "custom",    
                "selected_models": selected_models,
                "selection_method": "explicit" if selected_models else "fanout"
            })

            if selected_models:
                # Route to specific models
                logger.info(f"Routing new query {query_id} (product {product_id}) to models: {selected_models}")
                for model in selected_models:
                    lambda_arn = TARGETS[model]
                    trigger_lambda(
                        lambda_arn, 
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        }, 
                        job_id,
                        query_id
                    )
            else:
                # Fan-out to all models
                logger.info(f"Fan-out initiated for new query {query_id} (product {product_id}): '{query_text}'")
                orchestration_logger.log_event(job_id, "FanoutStarted", {
                    "query_id": query_id,
                    "product_id": product_id,
                    "query": query_text,
                    "query_type": "custom",
                    "target_count": len(TARGETS)
                })
                
                for key, arn in TARGETS.items():
                    trigger_lambda(
                        arn,
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        },
                        job_id,
                        query_id
                    )
                
                orchestration_logger.log_event(job_id, "FanoutCompleted", {
                    "query_id": query_id,
                    "product_id": product_id,
                    "query": query_text,
                    "query_type": "custom",
                    "target_count": len(TARGETS)
                })
        
        # Process old format queries (for backward compatibility - insert into database)
        for query_text in old_format_queries:
            try:
                # Insert old format query into database and get the auto-generated query_id
                query_id = insert_new_query(product_id, query_text, "old_format")
                logger.info(f"Processing old format query {query_id} for product {product_id}: '{query_text}'")
            except Exception as db_error:
                logger.error(f"Failed to insert old format query into database: {db_error}")
                # Fallback to generating query_id like before
                query_counter += 1
                query_id = query_counter
                logger.warning(f"Using fallback query_id {query_id} for old format query on product {product_id}: '{query_text}'")
            
            selected_models = extract_models_from_query(query_text)
            orchestration_logger.log_event(job_id, "ModelSelectionCompleted", {
                "query_id": query_id,
                "product_id": product_id,
                "query": query_text,
                "query_type": "old_format",
                "selected_models": selected_models,
                "selection_method": "explicit" if selected_models else "fanout"
            })

            if selected_models:
                # Route to specific models
                logger.info(f"Routing old format query {query_id} (product {product_id}) to models: {selected_models}")
                for model in selected_models:
                    lambda_arn = TARGETS[model]
                    trigger_lambda(
                        lambda_arn, 
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        }, 
                        job_id,
                        query_id
                    )
            else:
                # Fan-out to all models
                logger.info(f"Fan-out initiated for old format query {query_id} (product {product_id}): '{query_text}'")
                orchestration_logger.log_event(job_id, "FanoutStarted", {
                    "query_id": query_id,
                    "product_id": product_id,
                    "query": query_text,
                    "query_type": "old_format",
                    "target_count": len(TARGETS)
                })
                
                for key, arn in TARGETS.items():
                    trigger_lambda(
                        arn,
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        },
                        job_id,
                        query_id
                    )
                
                orchestration_logger.log_event(job_id, "FanoutCompleted", {
                    "query_id": query_id,
                    "product_id": product_id,
                    "query": query_text,
                    "query_type": "old_format",
                    "target_count": len(TARGETS)
                })
        
        orchestration_logger.log_event(job_id, "ProductProcessingCompleted", {
            "product_id": product_id,
            "queries_processed": total_product_queries,
            "existing_queries_processed": len(existing_queries),
            "new_queries_processed": len(new_queries),
            "old_format_queries_processed": len(old_format_queries)
        })

    # Log completion
    orchestration_logger.log_event(job_id, "OrchestrationCompleted", {
        "product_groups_processed": len(selected_queries),
        "total_queries": total_queries
    })
    
    logger.info(f"Completed processing for job {job_id}. All lambdas triggered.")
    
    # Return 202 Accepted response
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
            "product_groups_count": len(selected_queries),
            "total_queries_count": total_queries,
            "polling_info": {
                "table_name": os.environ.get('DYNAMODB_TABLE_NAME', 'OrchestrationLogs'),
                "query_example": f"SELECT * FROM {os.environ.get('DYNAMODB_TABLE_NAME', 'OrchestrationLogs')} WHERE job_id = '{job_id}'"
            }
        })
    }
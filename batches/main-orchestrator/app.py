#!/usr/bin/env python3
"""
AWS Batch version of the LLM Orchestrator
Converts Lambda function to standalone batch processing script
"""

import os
import sys
import boto3
import botocore
import logging
import re
import json
import uuid
import psycopg2
import argparse
from datetime import datetime, timezone
from decimal import Decimal
import time

# Set up structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

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
lambda_boto_config = botocore.config.Config(read_timeout=900, connect_timeout=60)
lambda_client = boto3.client("lambda", config=lambda_boto_config)

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

def trigger_lambda(lambda_arn, payload_dict, job_id: str, query_id: int = None, session_id: str = None, task_id: str = None):
    """Trigger lambda without waiting for response"""
    try:
        # Add session_id and task_id to payload if provided
        if session_id and "session_id" not in payload_dict:
            payload_dict["session_id"] = session_id
        if task_id and "task_id" not in payload_dict:
            payload_dict["task_id"] = task_id
            
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
            "query_id": query_id,
            "session_id": session_id,
            "task_id": task_id
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
            "query_id": query_id,
            "session_id": session_id,
            "task_id": task_id
        })
        return False

def process_retry_tasks(retry_tasks, session_id, job_id):
    """Process a list of retry_tasks, each specifying a model to retry."""
    orchestration_logger.log_event(job_id, "SelectiveRetryStarted", {
        "retry_tasks_count": len(retry_tasks),
        "session_id": session_id
    })

    for task in retry_tasks:
        task_id = task.get("task_id")
        if not task_id:
            logger.warning(f"Skipping retry task with no task_id: {task}")
            continue

        query_id = task.get("query_id")
        if not query_id:
            logger.warning(f"Skipping retry task {task_id} with no query_id: {task}")
            continue

        product_id = task.get("product_id")
        if not product_id:
            logger.warning(f"Skipping retry task {task_id} with no product_id: {task}")
            continue

        model = task.get("model")
        if not model:
            logger.warning(f"Skipping retry task {task_id} with no model: {task}")
            continue

        if model not in TARGETS:
            logger.warning(f"Skipping retry task {task_id} for unknown model: {model}")
            continue

        lambda_arn = TARGETS[model]
        logger.info(f"Retrying task {task_id} (product {product_id}) for model {model} (query {query_id})")

        # Fetch query_text from database using query_id
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT query_text FROM queries WHERE query_id = %s",
                (query_id,)
            )
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result:
                query_text = result[0]
                logger.info(f"Retrieved query_text for query_id {query_id}: '{query_text}'")
            else:
                logger.warning(f"No query_text found for query_id {query_id}, skipping retry")
                continue
                
        except Exception as db_error:
            logger.error(f"Failed to fetch query_text for query_id {query_id}: {db_error}")
            continue

        trigger_lambda(
            lambda_arn,
            {
                "query": query_text,
                "job_id": job_id,
                "query_id": query_id,
                "product_id": product_id,
                "task_id": task_id # Ensure task_id is passed for retry
            },
            job_id,
            query_id,
            session_id,
            task_id
        )
        orchestration_logger.log_event(job_id, "SelectiveRetryCompleted", {
            "task_id": task_id,
            "product_id": product_id,
            "query_id": query_id,
            "model_retried": model
        })

    orchestration_logger.log_event(job_id, "SelectiveRetryCompleted", {
        "retry_tasks_count": len(retry_tasks),
        "session_id": session_id
    })
    return {
        "status": "accepted",
        "message": "Job submitted successfully. All selective retries have been triggered.",
        "job_id": job_id,
        "session_id": session_id,
        "retry_tasks_count": len(retry_tasks),
        "polling_info": {
            "table_name": os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs'),
            "query_example": f"SELECT * FROM {os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')} WHERE job_id = '{job_id}'"
        }
    }

def process_queries_data(selected_queries, options, job_id):
    """Process queries data and trigger lambdas"""
    
    # Check if this is a retry request
    is_retry = options.get('retry', False)
    session_id = options.get('session_id')
    retry_tasks = options.get('retry_tasks', [])
    
    # For retry requests, we don't need selected_queries since we're retrying existing tasks
    if not selected_queries and not is_retry:
        logger.warning("No selected_queries provided.")
        orchestration_logger.log_event(job_id, "OrchestrationFailed", {
            "error": "No selected_queries provided"
        })
        return {"error": "No selected_queries provided"}
    
    # Generate session_id if not provided (for new requests)
    if not session_id:
        session_id = str(uuid.uuid4())
        logger.info(f"Generated new session_id: {session_id}")
    else:
        logger.info(f"Using provided session_id for retry: {session_id}")

    # Handle retry_tasks format for selective retries
    if is_retry and retry_tasks:
        logger.info(f"Processing selective retry for {len(retry_tasks)} tasks with session_id: {session_id}")
        return process_retry_tasks(retry_tasks, session_id, job_id)

    # Calculate total queries (both existing and new)
    total_queries = 0
    for sq in selected_queries:
        total_queries += len(sq.get("queries", []))  # Old format compatibility
        total_queries += len(sq.get("existing_queries", []))  # New format: existing queries
        total_queries += len(sq.get("new_queries", []))  # New format: new queries
    
    logger.info(f"Processing {len(selected_queries)} product groups with {total_queries} total queries for job {job_id} with session_id {session_id}")
    
    # Find highest existing query_id to avoid conflicts when generating new ones
    max_existing_query_id = 0
    for product_group in selected_queries:
        existing_queries = product_group.get("existing_queries", [])
        for eq in existing_queries:
            if isinstance(eq, dict) and "query_id" in eq:
                max_existing_query_id = max(max_existing_query_id, eq["query_id"])
    
    query_counter = max_existing_query_id  # Start new query IDs after existing ones
    
    # Helper function to generate task_id for each worker
    def generate_task_id():
        return str(uuid.uuid4())
    
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
                        task_id = generate_task_id()
                        trigger_lambda(
                            lambda_arn, 
                            {
                                "query": query_text, 
                                "job_id": job_id, 
                                "query_id": query_id,
                                "product_id": product_id
                            }, 
                            job_id,
                            query_id,
                            session_id,
                            task_id
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
                        task_id = generate_task_id()
                        trigger_lambda(
                            arn,
                            {
                                "query": query_text, 
                                "job_id": job_id, 
                                "query_id": query_id,
                                "product_id": product_id
                            },
                            job_id,
                            query_id,
                            session_id,
                            task_id
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
                    task_id = generate_task_id()
                    trigger_lambda(
                        lambda_arn, 
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        }, 
                        job_id,
                        query_id,
                        session_id,
                        task_id
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
                    task_id = generate_task_id()
                    trigger_lambda(
                        lambda_arn, 
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        }, 
                        job_id,
                        query_id,
                        session_id,
                        task_id
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
                    task_id = generate_task_id()
                    trigger_lambda(
                        arn,
                        {
                            "query": query_text, 
                            "job_id": job_id, 
                            "query_id": query_id,
                            "product_id": product_id
                        },
                        job_id,
                        query_id,
                        session_id,
                        task_id
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
        
        # Add 5-second delay after each product group (except for the last one)
        if selected_queries.index(product_group) < len(selected_queries) - 1:
            logger.info(f"[RATE_LIMIT] Applying 5-second delay after product group {selected_queries.index(product_group) + 1}/{len(selected_queries)}")
            orchestration_logger.log_event(job_id, "RateLimitDelayStarted", {
                "product_id": product_id,
                "product_index": selected_queries.index(product_group) + 1,
                "total_products": len(selected_queries),
                "delay_seconds": 5,
                "reason": "API rate limit prevention",
                "status": "delaying"
            })
            
            time.sleep(5)  # 5-second delay
            
            orchestration_logger.log_event(job_id, "RateLimitDelayCompleted", {
                "product_id": product_id,
                "product_index": selected_queries.index(product_group) + 1,
                "total_products": len(selected_queries),
                "delay_seconds": 5,
                "reason": "API rate limit prevention",
                "status": "delay_completed"
            })
            logger.info(f"[RATE_LIMIT] Completed 5-second delay after product group {selected_queries.index(product_group) + 1}/{len(selected_queries)}")

    # Log completion
    orchestration_logger.log_event(job_id, "OrchestrationCompleted", {
        "product_groups_processed": len(selected_queries),
        "total_queries": total_queries
    })
    
    logger.info(f"Completed processing for job {job_id}. All lambdas triggered.")
    
    return {
        "status": "accepted",
        "message": "Job submitted successfully. All lambdas have been triggered.",
        "job_id": job_id,
        "session_id": session_id,
        "product_groups_count": len(selected_queries),
        "total_queries_count": total_queries,
        "polling_info": {
            "table_name": os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs'),
            "query_example": f"SELECT * FROM {os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')} WHERE job_id = '{job_id}'"
        }
    }

def main():
    """Main entry point for AWS Batch job"""
    parser = argparse.ArgumentParser(description='LLM Orchestrator - AWS Batch Version')
    parser.add_argument('--input-file', help='JSON file containing selected_queries and options')
    parser.add_argument('--selected-queries', help='JSON string of selected_queries')
    parser.add_argument('--options', help='JSON string of options')
    
    # Parse known arguments only - ignore unknown arguments from AWS Batch
    args, unknown = parser.parse_known_args()
    
    # Log unknown arguments for debugging
    if unknown:
        logger.info(f"Ignoring unknown arguments: {unknown}")
    
    try:
        # Get job_id from environment (passed by trigger Lambda from UI)
        job_id = os.environ.get('JOB_ID')
        if not job_id:
            logger.error("JOB_ID environment variable not provided by trigger Lambda")
            print(json.dumps({"error": "JOB_ID environment variable not provided"}))
            sys.exit(1)
        
        logger.info(f"Using job_id from UI via trigger Lambda: {job_id}")
        
        # Parse input data
        selected_queries = None
        options = {}
        
        if args.input_file:
            # Read from JSON file
            with open(args.input_file, 'r') as f:
                data = json.load(f)
                selected_queries = data.get('selected_queries')
                options = data.get('options', {})
        elif args.selected_queries:
            # Parse from command line argument
            selected_queries = json.loads(args.selected_queries)
            if args.options:
                options = json.loads(args.options)
        else:
            # Try to read from AWS Batch job parameters via environment variables
            try:
                # Check if job parameters are passed via environment variables
                selected_queries_str = os.environ.get('selected_queries')
                options_str = os.environ.get('options', '{}')
                
                if selected_queries_str:
                    selected_queries = json.loads(selected_queries_str)
                    options = json.loads(options_str)
                    logger.info(f"Read input from environment variables")
                else:
                    # Fallback: try to read from stdin (for AWS Batch job parameters)
                    try:
                        input_data = json.loads(sys.stdin.read())
                        selected_queries = input_data.get('selected_queries')
                        options = input_data.get('options', {})
                        logger.info(f"Read input from stdin")
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"Failed to parse input from stdin: {e}")
                        print(json.dumps({"error": f"Invalid input format: {e}"}))
                        sys.exit(1)
            except (json.JSONDecodeError, ValueError) as e:
                logger.error(f"Failed to parse input from environment variables: {e}")
                print(json.dumps({"error": f"Invalid input format: {e}"}))
                sys.exit(1)
        
        # For retry requests, we don't need selected_queries since we're retrying existing tasks
        is_retry = options.get('retry', False)
        if not selected_queries and not is_retry:
            logger.error("No selected_queries provided")
            print(json.dumps({"error": "No selected_queries provided"}))
            sys.exit(1)
        
        # Log environment variables for debugging
        logger.info(f"Environment variables - JOB_ID: {os.environ.get('JOB_ID')}")
        logger.info(f"Environment variables - selected_queries: {os.environ.get('selected_queries')}")
        logger.info(f"Environment variables - options: {os.environ.get('options')}")
        logger.info(f"Lambda ARNs - ChatGPT: {TARGETS.get('chatgpt')}")
        logger.info(f"Lambda ARNs - AIO: {TARGETS.get('aio')}")
        logger.info(f"Lambda ARNs - AIM: {TARGETS.get('aim')}")
        logger.info(f"Lambda ARNs - Perplexity: {TARGETS.get('perplexity')}")
        
        
        # Process the queries
        result = process_queries_data(selected_queries, options, job_id)
        
        # Output result
        print(json.dumps(result, indent=2))
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        error_result = {"error": str(e), "status": "failed"}
        print(json.dumps(error_result, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()

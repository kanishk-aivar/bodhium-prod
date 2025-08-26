#!/usr/bin/env python3
"""
Lambda Trigger for AWS Batch LLM Orchestrator
Receives API Gateway events and submits AWS Batch jobs
"""

import os
import json
import boto3
import logging
import uuid
import urllib.parse
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Services
batch_client = boto3.client('batch')
dynamodb = boto3.resource('dynamodb')

# Environment variables - these are the trigger Lambda's environment variables
BATCH_JOB_QUEUE = os.environ.get('BATCH_JOB_QUEUE')
BATCH_JOB_DEFINITION = os.environ.get('BATCH_JOB_DEFINITION')
ORCHESTRATION_LOGS_TABLE = os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')

# Batch job environment variables - these will be passed to the batch job
DB_SECRET_NAME = os.environ.get('DB_SECRET_NAME', 'rds-db-credentials/cluster-bodhium/bodhium')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Lambda ARNs - these will be passed to the batch job
LAMBDA_CHATGPT = os.environ.get('LAMBDA_CHATGPT')
LAMBDA_AIOVERVIEW = os.environ.get('LAMBDA_AIOVERVIEW')
LAMBDA_AIMODE = os.environ.get('LAMBDA_AIMODE')
LAMBDA_PERPLEXITYAPI = os.environ.get('LAMBDA_PERPLEXITYAPI')

# Initialize DynamoDB table for logging
orchestration_table = dynamodb.Table(ORCHESTRATION_LOGS_TABLE)


def log_event(job_id, event_name, details=None):
    """Log event to DynamoDB orchestration table"""
    try:
        timestamp = datetime.now(timezone.utc).isoformat()
        unique_id = str(uuid.uuid4())[:8]
        event_timestamp_id = f"{timestamp}#{unique_id}"
        
        item = {
            'pk': f"JOB#{job_id}",
            'sk': event_timestamp_id,
            'job_id': job_id,
            'event_timestamp_id': event_timestamp_id,
            'eventName': event_name,
            'details': details or {},
            'status': details.get('status', 'created') if details else 'created'
        }
        
        orchestration_table.put_item(Item=item)
        logger.info(f"Logged event: {event_name} for job: {job_id}")
    except Exception as e:
        logger.error(f"Failed to log event: {e}")


def lambda_handler(event, context):
    """Lambda handler for API Gateway events"""
    
    logger.info(f"Received API Gateway event: {event}")
    
    try:
        # Validate required environment variables
        if not BATCH_JOB_QUEUE:
            raise ValueError("BATCH_JOB_QUEUE environment variable not set")
        if not BATCH_JOB_DEFINITION:
            raise ValueError("BATCH_JOB_DEFINITION environment variable not set")
        
        # Parse the JSON body from API Gateway event
        try:
            if 'body' in event and event['body']:
                body = event['body']
                if isinstance(body, str):
                    body = json.loads(body)
                
                # Extract job_id from body (UI provides this)
                job_id = body.get("job_id")
                if not job_id:
                    raise ValueError("job_id is required in the request body")
                
                logger.info(f"Using job_id from UI: {job_id}")
                
                # Log the trigger event after job_id is extracted
                log_event(job_id, "BatchTriggerStarted", {
                    "event": event,
                    "context": {
                        "function_name": context.function_name,
                        "function_version": context.function_version,
                        "invoked_function_arn": context.invoked_function_arn,
                        "memory_limit_in_mb": context.memory_limit_in_mb,
                        "remaining_time_in_millis": context.get_remaining_time_in_millis()
                    },
                    "mode": "Batch_trigger",
                    "status": "created"
                })
                
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
                job_id = event.get("job_id")
                if not job_id:
                    raise ValueError("job_id is required in the event")
                
                logger.info(f"Using job_id from direct invocation: {job_id}")
                
                # Log the trigger event after job_id is extracted
                log_event(job_id, "BatchTriggerStarted", {
                    "event": event,
                    "context": {
                        "function_name": context.function_name,
                        "function_version": context.function_version,
                        "invoked_function_arn": context.invoked_function_arn,
                        "memory_limit_in_mb": context.memory_limit_in_mb,
                        "remaining_time_in_millis": context.get_remaining_time_in_millis()
                    },
                    "mode": "Batch_trigger",
                    "status": "created"
                })
                
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
            log_event(job_id, "BatchTriggerFailed", {
                "error": f"Failed to parse request body: {e}",
                "status": "failed"
            })
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS'
                },
                'body': json.dumps({"error": "Invalid JSON in request body"})
            }

        if not selected_queries:
            logger.warning("No selected_queries provided in the event.")
            log_event(job_id, "BatchTriggerFailed", {
                "error": "No selected_queries provided in the event",
                "status": "failed"
            })
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'POST, OPTIONS'
                },
                'body': json.dumps({"error": "No selected_queries provided"})
            }
        
        # Create job name with timestamp to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_name = f"llm-orchestrator-{timestamp}-{job_id[:8]}"
        
        # Prepare batch job parameters
        job_parameters = {
            'selected_queries': json.dumps(selected_queries),
            'options': json.dumps(options),
            'job_id': job_id,
            'triggered_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Submit AWS Batch job
        logger.info(f"Submitting batch job: {job_name} with job_id: {job_id}")
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=BATCH_JOB_QUEUE,
            jobDefinition=BATCH_JOB_DEFINITION,
            parameters=job_parameters,
            containerOverrides={
                'environment': [
                    {
                        'name': 'JOB_ID',
                        'value': job_id
                    },
                    {
                        'name': 'TRIGGERED_AT',
                        'value': job_parameters['triggered_at']
                    },
                    {
                        'name': 'AWS_REGION',
                        'value': AWS_REGION
                    },
                    {
                        'name': 'ORCHESTRATION_LOGS_TABLE',
                        'value': ORCHESTRATION_LOGS_TABLE
                    },
                    {
                        'name': 'DB_SECRET_NAME',
                        'value': DB_SECRET_NAME
                    },
                    {
                        'name': 'LAMBDA_CHATGPT',
                        'value': LAMBDA_CHATGPT
                    },
                    {
                        'name': 'LAMBDA_AIOVERVIEW',
                        'value': LAMBDA_AIOVERVIEW
                    },
                    {
                        'name': 'LAMBDA_AIMODE',
                        'value': LAMBDA_AIMODE
                    },
                    {
                        'name': 'LAMBDA_PERPLEXITYAPI',
                        'value': LAMBDA_PERPLEXITYAPI
                    },
                    {
                        'name': 'selected_queries',
                        'value': job_parameters['selected_queries']
                    },
                    {
                        'name': 'options',
                        'value': job_parameters['options']
                    }
                ]
            }
        )
        
        batch_job_id = response['jobId']
        
        # Log successful batch job submission
        log_event(job_id, "BatchJobSubmitted", {
            "batch_job_id": batch_job_id,
            "batch_job_name": job_name,
            "job_queue": BATCH_JOB_QUEUE,
            "job_definition": BATCH_JOB_DEFINITION,
            "selected_queries_count": len(selected_queries),
            "parameters": job_parameters,
            "status": "submitted"
        })
        
        logger.info(f"Successfully submitted batch job: {batch_job_id}")
        
        # Return success response
        return {
            'statusCode': 202,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS'
            },
            'body': json.dumps({
                'status': 'accepted',
                'message': 'Batch job submitted successfully',
                'job_id': job_id,
                'batch_job_id': batch_job_id,
                'batch_job_name': job_name,
                'selected_queries_count': len(selected_queries),
                'mode': 'Batch_trigger',
                'polling_info': {
                    'batch_job_id': batch_job_id,
                    'job_queue': BATCH_JOB_QUEUE,
                    'query_example': f"aws batch describe-jobs --jobs {batch_job_id}"
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Failed to process API Gateway event: {e}")
        
        # Log error event
        log_event(job_id, "BatchTriggerFailed", {
            "error": str(e),
            "event": event,
            "status": "failed"
        })
        
        # Return error response
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS'
            },
            'body': json.dumps({
                'status': 'error',
                'message': f'Failed to submit batch job: {str(e)}',
                'job_id': job_id,
                'mode': 'Batch_trigger'
            })
        }

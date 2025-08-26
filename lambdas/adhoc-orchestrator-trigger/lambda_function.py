#!/usr/bin/env python3
"""
Lambda Trigger for AWS Batch Adhoc Orchestrator
Receives S3 events and submits AWS Batch jobs
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
FILE_TRACKING_TABLE = os.environ.get('FILE_TRACKING_TABLE', 'Adhoc-Query-Tracker')
TEST_PRODUCT_ID = os.environ.get('TEST_PRODUCT_ID', '99999999')
RDS_DB = os.environ.get('RDS_DB', 'dev/rds')
JOB_ID_BUCKET = os.environ.get('JOB_ID_BUCKET', 'bodhium-adhoc-upload-jobs-output')
JOB_ID_PATH = os.environ.get('JOB_ID_PATH', 'logs')
FORCE_LLM_FANOUT = os.environ.get('FORCE_LLM_FANOUT', 'true')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Lambda ARNs - these will be passed to the batch job
LAMBDA_CHATGPT_V1 = os.environ.get('LAMBDA_CHATGPT_V1')
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
    """Lambda handler for S3 event triggers"""
    
    # Generate a unique job ID for tracking
    job_id = str(uuid.uuid4())
    
    logger.info(f"Received S3 event: {event}")
    
    # Log the trigger event
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
    
    try:
        # Extract S3 information from the event
        if 'Records' not in event or not event['Records']:
            raise ValueError("No S3 records found in event")
        
        s3_record = event['Records'][0]
        if not s3_record.get('eventName', '').startswith('ObjectCreated'):
            raise ValueError(f"Unsupported event type: {s3_record.get('eventName')}")
        
        bucket = s3_record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])
        
        logger.info(f"Processing S3 upload: {bucket}/{key}")
        logger.info(f"Original encoded key: {s3_record['s3']['object']['key']}")
        logger.info(f"Decoded key: {key}")
        
        # Validate required environment variables
        if not BATCH_JOB_QUEUE:
            raise ValueError("BATCH_JOB_QUEUE environment variable not set")
        if not BATCH_JOB_DEFINITION:
            raise ValueError("BATCH_JOB_DEFINITION environment variable not set")
        
        # Create job name with timestamp to avoid conflicts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        job_name = f"adhoc-orchestrator-{timestamp}-{job_id[:8]}"
        
        # Prepare batch job parameters
        job_parameters = {
            'bucket': bucket,
            'key': key,
            'job_id': job_id,
            'triggered_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Submit AWS Batch job
        logger.info(f"Submitting batch job: {job_name}")
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=BATCH_JOB_QUEUE,
            jobDefinition=BATCH_JOB_DEFINITION,
            parameters=job_parameters,
            containerOverrides={
                'environment': [
                    {
                        'name': 'S3_BUCKET',
                        'value': bucket
                    },
                    {
                        'name': 'S3_KEY', 
                        'value': key
                    },
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
                        'name': 'FILE_TRACKING_TABLE', 
                        'value': FILE_TRACKING_TABLE
                    },
                    {
                        'name': 'ORCHESTRATION_LOGS_TABLE',
                        'value': ORCHESTRATION_LOGS_TABLE
                    },
                    {
                        'name': 'TEST_PRODUCT_ID',
                        'value': TEST_PRODUCT_ID
                    },
                    {
                        'name': 'RDS_DB',
                        'value': RDS_DB
                    },
                    {
                        'name': 'JOB_ID_BUCKET',
                        'value': JOB_ID_BUCKET
                    },
                    {
                        'name': 'JOB_ID_PATH', 
                        'value': JOB_ID_PATH
                    },
                    {
                        'name': 'FORCE_LLM_FANOUT',
                        'value': FORCE_LLM_FANOUT
                    },
                    {
                        'name': 'LAMBDA_CHATGPT_V1',
                        'value': LAMBDA_CHATGPT_V1
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
            "s3_bucket": bucket,
            "s3_key": key,
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
                's3_bucket': bucket,
                's3_key': key,
                'mode': 'Batch_trigger',
                'polling_info': {
                    'batch_job_id': batch_job_id,
                    'job_queue': BATCH_JOB_QUEUE,
                    'query_example': f"aws batch describe-jobs --jobs {batch_job_id}"
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Failed to process S3 event: {e}")
        
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
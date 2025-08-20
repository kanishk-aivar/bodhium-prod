import json
import os
import boto3
import uuid
import logging
import psycopg2
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name=None):
    """Fetch secrets from AWS Secrets Manager."""
    if secret_name is None:
        secret_name = os.environ.get("RDS_SECRET_NAME", "dev/rds")
        
    region_name = os.environ.get("AWS_REGION", "us-east-1")

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except ClientError as e:
        logger.error(f"Secrets Manager fetch failed: {str(e)}")
        raise e

def get_db_connection():
    """Get PostgreSQL connection using credentials from Secrets Manager"""
    try:
        # Get database credentials from Secrets Manager
        secret = get_secret()
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=secret['DB_PORT']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def create_or_update_scrape_job(job_id: str, source_url: str, status: str, brand_name: str = None, error_message: str = None):
    """Create or update a scrape job record in the database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Try to update existing job first
        cursor.execute("""
            UPDATE scrapejobs 
            SET status = %s, updated_at = NOW(), error_message = %s
            WHERE job_id = %s
        """, (status, error_message, job_id))
        
        # If no rows were updated, insert new record
        if cursor.rowcount == 0:
            cursor.execute("""
                INSERT INTO scrapejobs (job_id, source_url, status, brand_name, created_at, updated_at, error_message)
                VALUES (%s, %s, %s, %s, NOW(), NOW(), %s)
            """, (job_id, source_url, status, brand_name, error_message))
            logger.info(f"Created new scrape job: {job_id}")
        else:
            logger.info(f"Updated scrape job: {job_id} to status: {status}")
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating/updating scrape job: {e}")
        return False

def log_orchestration_event(event_name: str, details: dict = None, job_id: str = None):
    """Log orchestration events to DynamoDB"""
    try:
        dynamodb_table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'OrchestrationLogs')
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(dynamodb_table_name)
        
        if not job_id:
            job_id = str(uuid.uuid4())
        
        timestamp = datetime.now().isoformat()
        unique_id = str(uuid.uuid4())[:8]
        event_timestamp_id = f"{timestamp}#{unique_id}"
        
        log_data = {
            "pk": job_id,
            "sk": event_timestamp_id,
            "job_id": job_id,
            "event_timestamp_id": event_timestamp_id,
            "eventName": event_name,
            "details": details or {}
        }
        
        import decimal
        def convert_floats(obj):
            if isinstance(obj, float):
                return decimal.Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(v) for v in obj]
            return obj
        
        log_data = convert_floats(log_data)
        response = table.put_item(Item=log_data)
        logger.info(f"Logged orchestration event: {event_name} for job {job_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to log orchestration event: {e}")
        return False

def lambda_handler(event, context):
    try:
        # Extract URL and job_id from event
        url = None
        job_id = None
        brand_name = None
        
        # Parse input to get job_id if provided
        if 'body' in event:
            try:
                body = event['body']
                if isinstance(body, str):
                    body = json.loads(body)
                url = body.get('url')
                job_id = body.get('job_id')
                brand_name = body.get('brand_name')
            except (json.JSONDecodeError, AttributeError):
                pass
        
        if not url and 'queryStringParameters' in event and event['queryStringParameters']:
            url = event['queryStringParameters'].get('url')
            job_id = event['queryStringParameters'].get('job_id')
            brand_name = event['queryStringParameters'].get('brand_name')
        
        if not url and 'url' in event:
            url = event['url']
            job_id = event.get('job_id')
            brand_name = event.get('brand_name')
        
        # Generate job_id if not provided
        if not job_id:
            job_id = str(uuid.uuid4())
            logger.info(f"Generated new job_id: {job_id}")
        else:
            logger.info(f"Using provided job_id: {job_id}")
        
        # Create initial job record
        create_or_update_scrape_job(job_id, url, "PENDING", brand_name)
        
        log_orchestration_event("JobInitiated", {
            "lambda_function": context.function_name if context else "unknown",
            "request_id": context.aws_request_id if context else "unknown",
            "timestamp": datetime.now().isoformat(),
            "job_id_source": "provided" if 'job_id' in locals() and job_id else "generated"
        }, job_id)
        
        if not url:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'status': 'error',
                    'message': 'URL parameter is required'
                })
            }
        
        if not url.startswith('http'):
            url = 'https://' + url
        
        # Get configuration from environment variables
        batch_job_queue = os.environ.get('BATCH_JOB_QUEUE')
        batch_job_definition = os.environ.get('BATCH_JOB_DEFINITION')
        batch_job_name_prefix = os.environ.get('BATCH_JOB_NAME_PREFIX', 'bodhium-scrapper')
        
        if not batch_job_queue or not batch_job_definition:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Batch configuration is missing'
                })
            }
        
        # Generate a unique job name (keeping hash for job name as it needs to be unique)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        job_name = f"{batch_job_name_prefix}-{timestamp}-{unique_id}"
        
        # Collect environment variables to pass to the batch job
        batch_env_vars = [
            {'name': 'CRAWL_URL', 'value': url},
            {'name': 'AWS_BATCH_JOB_ID', 'value': job_id},  # Pass job_id with correct env var name
            {'name': 'BRAND_NAME', 'value': brand_name},  # Pass brand name to batch scraper
            {'name': 'AWS_REGION', 'value': os.environ.get('AWS_REGION', 'us-east-1')},
            {'name': 'S3_BUCKET_NAME', 'value': os.environ.get('S3_BUCKET_NAME')},
            {'name': 'S3_PATH', 'value': os.environ.get('S3_PATH')},
            {'name': 'DYNAMODB_TABLE_NAME', 'value': os.environ.get('DYNAMODB_TABLE_NAME', 'OrchestrationLogs')},
            {'name': 'GEMINI_SECRET_NAME', 'value': os.environ.get('GEMINI_SECRET_NAME', 'Gemini-API-ChatGPT')},
            {'name': 'GEMINI_SECRET_REGION', 'value': os.environ.get('GEMINI_SECRET_REGION', 'us-east-1')}
        ]
        
        # Filter out None values
        batch_env_vars = [env for env in batch_env_vars if env['value'] is not None]
        
        # Submit the batch job
        batch_client = boto3.client('batch')
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=batch_job_queue,
            jobDefinition=batch_job_definition,
            containerOverrides={
                'environment': batch_env_vars,
                'command': ['python', 'app.py', '--url', url, '--job-id', job_id, '--brand-name', brand_name] if brand_name else ['python', 'app.py', '--url', url, '--job-id', job_id]
            }
        )
        
        batch_job_id = response['jobId']
        logger.info(f"Submitted batch job {job_name} with ID {batch_job_id} for URL: {url}")
        
        # Update job status to SUBMITTED
        create_or_update_scrape_job(job_id, url, "SUBMITTED", brand_name)
        
        log_orchestration_event("JobSuccessful", {
            "batch_job_id": batch_job_id,
            "batch_job_name": job_name,
            "url": url,
            "batch_job_queue": batch_job_queue,
            "batch_job_definition": batch_job_definition,
            "timestamp": datetime.now().isoformat()
        }, job_id)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'success',
                'message': 'Batch job submitted successfully',
                'data': {
                    'job_id': job_id,  # Return the Lambda job_id (timestamp only)
                    'batch_job_id': batch_job_id,  # Also return batch job ID
                    'job_name': job_name,
                    'url': url,
                    'batch_job_queue': batch_job_queue,
                    'batch_job_definition': batch_job_definition,
                    'timestamp': datetime.now().isoformat()
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Error submitting batch job: {str(e)}")
        
        # Update job status to FAILED if job_id exists
        if 'job_id' in locals():
            create_or_update_scrape_job(job_id, url if 'url' in locals() else None, "JOB_FAILED", 
                                       brand_name if 'brand_name' in locals() else None, str(e))
        
        log_orchestration_event("JobFailed", {
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, job_id if 'job_id' in locals() else None)
        
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'error',
                'message': f'Failed to submit batch job: {str(e)}'
            })
        }
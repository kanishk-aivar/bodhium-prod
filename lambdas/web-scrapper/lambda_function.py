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
        max_urls = None
        
        # Parse input to get job_id if provided
        if 'body' in event:
            try:
                body = event['body']
                if isinstance(body, str):
                    body = json.loads(body)
                url = body.get('url')
                job_id = body.get('job_id')
                brand_name = body.get('brand_name')
                max_urls = body.get('max_urls', '100')  # Default to 100 if not specified
            except (json.JSONDecodeError, AttributeError):
                pass
        
        if not url and 'queryStringParameters' in event and event['queryStringParameters']:
            url = event['queryStringParameters'].get('url')
            job_id = event['queryStringParameters'].get('job_id')
            brand_name = event['queryStringParameters'].get('brand_name')
            max_urls = event['queryStringParameters'].get('max_urls', '100')
        
        if not url and 'url' in event:
            url = event['url']
            job_id = event.get('job_id')
            brand_name = event.get('brand_name')
            max_urls = event.get('max_urls', '100')
        
        # Generate job_id if not provided
        if not job_id:
            job_id = str(uuid.uuid4())
            logger.info(f"Generated new job_id: {job_id}")
        else:
            logger.info(f"Using provided job_id: {job_id}")
        
        # Validate max_urls
        try:
            max_urls = int(max_urls) if max_urls else 100
            if max_urls <= 0 or max_urls > 10000:
                max_urls = 100
                logger.warning(f"Invalid max_urls value, using default: 100")
        except (ValueError, TypeError):
            max_urls = 100
            logger.warning(f"Invalid max_urls value, using default: 100")
        
        # Create initial job record
        create_or_update_scrape_job(job_id, url, "PENDING", brand_name)
        
        log_orchestration_event("JobInitiated", {
            "lambda_function": context.function_name if context else "unknown",
            "request_id": context.aws_request_id if context else "unknown",
            "timestamp": datetime.now().isoformat(),
            "job_id_source": "provided" if 'job_id' in locals() and job_id else "generated",
            "max_urls": max_urls
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
        batch_job_name_prefix = os.environ.get('BATCH_JOB_NAME_PREFIX', 'bodhium-scraper')
        
        if not batch_job_queue or not batch_job_definition:
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Batch configuration is missing'
                })
            }
        
        # Generate a unique job name
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        job_name = f"{batch_job_name_prefix}-{timestamp}-{unique_id}"
        
        # Get RDS credentials from Secrets Manager for the batch job
        try:
            rds_secret = get_secret()
            rds_host = rds_secret['DB_HOST']
            rds_database = rds_secret['DB_NAME']
            rds_username = rds_secret['DB_USER']
            rds_password = rds_secret['DB_PASSWORD']
            rds_port = rds_secret['DB_PORT']
        except Exception as e:
            logger.error(f"Failed to get RDS credentials: {e}")
            return {
                'statusCode': 500,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'status': 'error',
                    'message': 'Failed to get database credentials'
                })
            }
        
        # Get Gemini API key from Secrets Manager
        gemini_api_key = None
        try:
            gemini_secret_name = os.environ.get('GEMINI_SECRET_NAME', 'Gemini-API-ChatGPT')
            gemini_secret = get_secret(gemini_secret_name)
            gemini_api_key = gemini_secret.get('GEMINI_API_KEY') or gemini_secret.get('api_key')
        except Exception as e:
            logger.warning(f"Failed to get Gemini API key: {e}")
        
        # Collect environment variables to pass to the batch job
        batch_env_vars = [
            {'name': 'JOB_ID', 'value': job_id},
            {'name': 'BRAND_NAME', 'value': brand_name or 'Unknown'},
            {'name': 'SOURCE_URL', 'value': url},
            {'name': 'MAX_URLS', 'value': str(max_urls)},
            {'name': 'RDS_HOST', 'value': rds_host},
            {'name': 'RDS_PORT', 'value': str(rds_port)},
            {'name': 'RDS_DATABASE', 'value': rds_database},
            {'name': 'RDS_USERNAME', 'value': rds_username},
            {'name': 'RDS_PASSWORD', 'value': rds_password},
            {'name': 'AWS_REGION', 'value': os.environ.get('AWS_REGION', 'us-east-1')},
            {'name': 'MONITOR_INTERVAL', 'value': '5'},
            {'name': 'SQLITE_DB_PATH', 'value': './scraper.db'}
        ]
        
        # Add Gemini API key if available
        if gemini_api_key:
            batch_env_vars.append({'name': 'GEMINI_API_KEY', 'value': gemini_api_key})
        
        # Add optional thread configuration
        batch_env_vars.extend([
            {'name': 'MAX_CRAWLER_THREADS', 'value': os.environ.get('MAX_CRAWLER_THREADS', '10')},
            {'name': 'MAX_SCRAPER_THREADS', 'value': os.environ.get('MAX_SCRAPER_THREADS', '10')},
            {'name': 'MAX_GEMINI_THREADS', 'value': os.environ.get('MAX_GEMINI_THREADS', '10')}
        ])
        
        # Filter out None values
        batch_env_vars = [env for env in batch_env_vars if env['value'] is not None]
        
        logger.info(f"Submitting batch job with {len(batch_env_vars)} environment variables")
        
        # Submit the batch job
        batch_client = boto3.client('batch')
        response = batch_client.submit_job(
            jobName=job_name,
            jobQueue=batch_job_queue,
            jobDefinition=batch_job_definition,
            parameters={
                'job_id': job_id,
                'brand_name': brand_name or 'Unknown',
                'source_url': url,
                'max_urls': str(max_urls)
            },
            containerOverrides={
                'environment': batch_env_vars
            }
        )
        
        batch_job_id = response['jobId']
        logger.info(f"Submitted batch job {job_name} with ID {batch_job_id} for URL: {url}")
        
        # Update job status to SUBMITTED
        create_or_update_scrape_job(job_id, url, "JOB_RUNNING", brand_name)
        
        log_orchestration_event("JobSubmitted", {
            "batch_job_id": batch_job_id,
            "batch_job_name": job_name,
            "url": url,
            "brand_name": brand_name,
            "max_urls": max_urls,
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
                    'job_id': job_id,
                    'batch_job_id': batch_job_id,
                    'job_name': job_name,
                    'url': url,
                    'brand_name': brand_name,
                    'max_urls': max_urls,
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
import os
import boto3
import json
import logging
import psycopg2
import csv
from io import StringIO
from datetime import datetime, timezone

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SECRET_NAME = os.environ.get('RDS_DB_SECRET', 'dev/rds')
REGION_NAME = os.environ.get('AWS_REGION', 'us-east-1')
CSV_OUTPUT_BUCKET = os.environ.get('CSV_OUTPUT_BUCKET')
CSV_OUTPUT_PATH = os.environ.get('CSV_OUTPUT_PATH', 'csv-job-results/')

# AWS clients
secrets_client = boto3.client('secretsmanager', region_name=REGION_NAME)
s3_client = boto3.client('s3', region_name=REGION_NAME)

def get_rds_connection():
    """Get RDS connection from secrets manager"""
    try:
        secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        secret = json.loads(secret_response['SecretString'])
        
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=int(secret.get('DB_PORT', 5432))
        )
        logger.info("Successfully connected to RDS PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed connecting to RDS: {str(e)}")
        raise Exception(f"Database connection failed: {str(e)}")

def check_job_completion_status(job_id):
    """Check if all tasks for a job are completed"""
    conn = get_rds_connection()
    try:
        with conn.cursor() as cur:
            # Check completion status
            query = """
            SELECT 
                COUNT(*) as total_tasks,
                COUNT(CASE WHEN status IN ('completed', 'failed') THEN 1 END) as finished_tasks,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_tasks,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_tasks,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_tasks,
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_tasks
            FROM public.llmtasks 
            WHERE job_id = %s
            """
            
            cur.execute(query, (job_id,))
            result = cur.fetchone()
            
            if result and result[0] > 0:  # total_tasks > 0
                total, finished, completed, failed, processing, pending = result
                is_complete = total == finished
                
                logger.info(f"Job {job_id} Status: {finished}/{total} finished "
                          f"({completed} completed, {failed} failed, {processing} processing, {pending} pending)")
                
                return {
                    'job_exists': True,
                    'is_complete': is_complete,
                    'total_tasks': total,
                    'finished_tasks': finished,
                    'completed_tasks': completed,
                    'failed_tasks': failed,
                    'processing_tasks': processing,
                    'pending_tasks': pending,
                    'completion_percentage': round((finished / total) * 100, 2) if total > 0 else 0
                }
            else:
                # Job doesn't exist or has no tasks
                logger.warning(f"Job {job_id} not found or has no tasks")
                return {
                    'job_exists': False,
                    'error': f'Job {job_id} not found or has no associated tasks'
                }
            
    except Exception as e:
        logger.error(f"Failed to check job status for {job_id}: {str(e)}")
        return {
            'job_exists': False,
            'error': f'Database query failed: {str(e)}'
        }
    finally:
        conn.close()

def fetch_job_results(job_id):
    """Fetch completed job results with all relevant details"""
    conn = get_rds_connection()
    try:
        with conn.cursor() as cur:
            query = """
            SELECT 
                llm.task_id,
                llm.job_id,
                llm.query_id,
                llm.llm_model_name,
                q.query_text,
                q.query_type,
                llm.status,
                llm.s3_output_path,
                llm.error_message,
                llm.created_at,
                llm.completed_at,
                q.is_active,
                EXTRACT(EPOCH FROM (llm.completed_at - llm.created_at)) as duration_seconds
            FROM public.llmtasks as llm
            INNER JOIN public.queries as q ON llm.query_id = q.query_id
            WHERE llm.job_id = %s 
            AND llm.status IN ('completed', 'failed')
            ORDER BY llm.created_at, llm.query_id, llm.llm_model_name
            """
            
            cur.execute(query, (job_id,))
            results = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            
            logger.info(f"Fetched {len(results)} completed/failed records for job_id: {job_id}")
            return results, column_names
            
    except Exception as e:
        logger.error(f"Failed to fetch job results for {job_id}: {str(e)}")
        raise Exception(f"Results fetch failed: {str(e)}")
    finally:
        conn.close()

def convert_to_csv(data, column_names):
    """Convert database results to CSV format"""
    try:
        output = StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
        
        # Write header
        writer.writerow(column_names)
        
        # Write data rows
        for row in data:
            csv_row = []
            for item in row:
                if item is None:
                    csv_row.append('')
                elif isinstance(item, datetime):
                    csv_row.append(item.isoformat())
                elif isinstance(item, (int, float)):
                    csv_row.append(str(item))
                else:
                    # Handle strings with potential commas/quotes
                    csv_row.append(str(item).replace('\n', ' ').replace('\r', ''))
            writer.writerow(csv_row)
        
        csv_content = output.getvalue()
        logger.info(f"Generated CSV with {len(data)} rows and {len(column_names)} columns")
        return csv_content
        
    except Exception as e:
        logger.error(f"CSV conversion failed: {str(e)}")
        raise Exception(f"CSV generation failed: {str(e)}")

def save_csv_to_s3(csv_content, job_id):
    """Save CSV content to S3 bucket"""
    try:
        if not CSV_OUTPUT_BUCKET:
            logger.error("CSV_OUTPUT_BUCKET environment variable not configured")
            raise Exception("S3 bucket not configured")
        
        # Generate filename with timestamp
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{CSV_OUTPUT_PATH.rstrip('/')}/job_results_{job_id}_{timestamp}.csv"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=CSV_OUTPUT_BUCKET,
            Key=filename,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv',
            ContentDisposition=f'attachment; filename="job_results_{job_id}.csv"',
            Metadata={
                'job_id': str(job_id),
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'lambda_function': 'job-results-processor'
            }
        )
        
        s3_url = f"s3://{CSV_OUTPUT_BUCKET}/{filename}"
        logger.info(f"CSV successfully saved to S3: {s3_url}")
        return s3_url, filename
        
    except Exception as e:
        logger.error(f"Failed to save CSV to S3: {str(e)}")
        return None, None

def lambda_handler(event, context):
    """Main Lambda Handler for Job Results Processing"""
    try:
        logger.info(f"Lambda invoked with event: {json.dumps(event, default=str)}")
        
        # Extract job_id from multiple possible sources
        job_id = None
        
        # Method 1: Direct from event
        if 'job_id' in event:
            job_id = event['job_id']
        
        # Method 2: From API Gateway path parameters
        elif 'pathParameters' in event and event['pathParameters']:
            job_id = event['pathParameters'].get('job_id')
        
        # Method 3: From query string parameters
        elif 'queryStringParameters' in event and event['queryStringParameters']:
            job_id = event['queryStringParameters'].get('job_id')
        
        # Method 4: From request body
        elif 'body' in event and event['body']:
            try:
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
                job_id = body.get('job_id')
            except json.JSONDecodeError:
                logger.warning("Failed to parse request body as JSON")
        
        # Validate job_id
        if not job_id:
            error_msg = "job_id is required. Provide it in event, pathParameters, queryStringParameters, or body"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Missing required parameter',
                    'message': error_msg,
                    'expected_format': {
                        'job_id': 'string'
                    }
                })
            }
        
        job_id = str(job_id).strip()
        logger.info(f"Processing results for job_id: {job_id}")
        
        # Step 1: Check job completion status
        logger.info(f"Step 1: Checking job completion status for {job_id}")
        job_status = check_job_completion_status(job_id)
        
        if not job_status.get('job_exists'):
            logger.warning(f"Job {job_id} not found")
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Job not found',
                    'job_id': job_id,
                    'message': job_status.get('error', 'Job does not exist or has no tasks')
                })
            }
        
        # Step 2: Handle incomplete jobs
        # if not job_status['is_complete']:
        #     logger.info(f"Job {job_id} is still processing")
        #     return {
        #         'statusCode': 202,  # Accepted - still processing
        #         'headers': {
        #             'Content-Type': 'application/json',
        #             'Access-Control-Allow-Origin': '*'
        #         },
        #         'body': json.dumps({
        #             'status': 'processing',
        #             'job_id': job_id,
        #             'progress': {
        #                 'total_tasks': job_status['total_tasks'],
        #                 'finished_tasks': job_status['finished_tasks'],
        #                 'completed_tasks': job_status['completed_tasks'],
        #                 'failed_tasks': job_status['failed_tasks'],
        #                 'processing_tasks': job_status['processing_tasks'],
        #                 'pending_tasks': job_status['pending_tasks'],
        #                 'completion_percentage': job_status['completion_percentage']
        #             },
        #             'message': 'Job still in progress. Poll this endpoint to check status.',
        #             'estimated_completion': 'Please check again in a few minutes'
        #         })
        #     }
        
        # Step 3: Fetch completed job results
        logger.info(f"Step 3: Fetching results for completed job {job_id}")
        try:
            results, column_names = fetch_job_results(job_id)
        except Exception as e:
            logger.error(f"Failed to fetch results: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Database query failed',
                    'job_id': job_id,
                    'message': str(e)
                })
            }
        
        if not results:
            logger.warning(f"No results found for job {job_id}")
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'No results available',
                    'job_id': job_id,
                    'message': 'Job completed but no results found',
                    'job_status': job_status
                })
            }
        
        # Step 4: Convert to CSV
        logger.info(f"Step 4: Converting {len(results)} records to CSV")
        try:
            csv_content = convert_to_csv(results, column_names)
        except Exception as e:
            logger.error(f"CSV conversion failed: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'CSV generation failed',
                    'job_id': job_id,
                    'message': str(e)
                })
            }
        
        # Step 5: Save CSV to S3
        logger.info(f"Step 5: Saving CSV to S3 for job {job_id}")
        s3_url, filename = save_csv_to_s3(csv_content, job_id)
        
        if not s3_url:
            logger.error("Failed to save CSV to S3")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'S3 upload failed',
                    'job_id': job_id,
                    'message': 'CSV generated but could not be saved to S3'
                })
            }
        
        # Step 6: Return success response
        success_response = {
            'status': 'completed',
            'job_id': job_id,
            'summary': {
                'total_records': len(results),
                'completed_tasks': job_status['completed_tasks'],
                'failed_tasks': job_status['failed_tasks'],
                'total_tasks': job_status['total_tasks'],
                'success_rate': round((job_status['completed_tasks'] / job_status['total_tasks']) * 100, 2)
            },
            'csv_details': {
                'generated': True,
                's3_location': s3_url,
                'filename': filename,
                'size_info': f"{len(results)} rows, {len(column_names)} columns"
            },
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'processing_complete': True
        }
        
        logger.info(f"Job {job_id} processing completed successfully: {len(results)} records saved to CSV")
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(success_response, indent=2)
        }
    
    except Exception as e:
        logger.error(f"Unexpected error processing job: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'error': 'Internal server error',
                'job_id': event.get('job_id', 'unknown'),
                'message': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            })
        }

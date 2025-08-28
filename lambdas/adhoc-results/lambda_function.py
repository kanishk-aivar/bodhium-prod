import os
import sys
import boto3
import json
import logging
import psycopg2
import csv
import re

import numpy
import pytz
import pandas as pd
from io import StringIO
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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

# Thread lock for DataFrame updates
df_lock = threading.Lock()

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
            
            if result and result[0] > 0:
                total, finished, completed, failed, processing, pending = result
                is_complete = total == finished
                
                logger.info(f"Job {job_id} Status: {finished}/{total} finished")
                
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

def check_soft_failure(content):
    """Check if the response contains a soft failure"""
    if isinstance(content, str):
        return "Crawl4AI Error" in content or "Error Message" in content
    elif isinstance(content, dict):
        content_str = content.get('content', '')
        return "Crawl4AI Error" in content_str or "Error Message" in content_str
    return False

def check_ai_overview_presence(content):
    """Check if AI Overview is present in the response"""
    if isinstance(content, str):
        if "# AI Overview" in content or "**AI Overview**" in content:
            if "An AI Overview is not available" in content and not re.search(r'\*\*AI Overview\*\*\s+\w+', content):
                return False
            return bool(re.search(r'(\*\*AI Overview\*\*\s+\w+)|(\# AI Overview\s+\w+)', content))
        return False
    elif isinstance(content, dict):
        content_str = content.get('content', '')
        if "# AI Overview" in content_str or "**AI Overview**" in content_str:
            if "An AI Overview is not available" in content_str and not re.search(r'\*\*AI Overview\*\*\s+\w+', content_str):
                return False
            return bool(re.search(r'(\*\*AI Overview\*\*\s+\w+)|(\# AI Overview\s+\w+)', content_str))
        return False
    return False

def extract_citations(content):
    """Extract citations from the content"""
    citations = []
    
    if isinstance(content, str):
        try:
            content_json = json.loads(content)
            if 'citations' in content_json:
                return content_json['citations']
        except:
            pass
        
        numbered_citations = re.findall(r'$$\d+$$(?:$$[^$$]+$$)?', content)
        if numbered_citations:
            citations.extend(numbered_citations)
        
        url_citations = re.findall(r'$$([^$$]+)$$$([^)]+)$', content)
        if url_citations:
            citations.extend([url for _, url in url_citations])
            
    elif isinstance(content, dict):
        if 'citations' in content:
            return content['citations']
        
        content_str = content.get('content', '')
        numbered_citations = re.findall(r'$$\d+$$(?:$$[^$$]+$$)?', content_str)
        if numbered_citations:
            citations.extend(numbered_citations)
        
        url_citations = re.findall(r'$$([^$$]+)$$$([^)]+)$', content_str)
        if url_citations:
            citations.extend([url for _, url in url_citations])
    
    return citations

def extract_model_specific_content(content, model_name, file_extension):
    """Extract model-specific content based on the model name and file type"""
    if file_extension == '.json':
        if isinstance(content, dict):
            if model_name in ['GOOGLE_AI_MODE', 'GOOGLE_AI_OVERVIEW', 'perplexity']:
                return content.get('content', '')
        elif isinstance(content, str):
            try:
                json_content = json.loads(content)
                if model_name in ['GOOGLE_AI_MODE', 'GOOGLE_AI_OVERVIEW', 'perplexity']:
                    return json_content.get('content', '')
            except:
                pass
    
    elif file_extension == '.md' and model_name == 'ChatGPT':
        # For ChatGPT markdown files, extract only the Response Content section
        if isinstance(content, str):
            # Find the "## Response Content" section
            # response_content_match = re.search(r'## Response Content\s*\n\n(.*?)(?=\n---|\n###|\Z)', content, re.DOTALL)
            # if response_content_match:
            #     return response_content_match.group(1).strip()
            pass
    # Fallback: return original content if no specific extraction rule applies
    return content

def process_row(index, row, df):
    """Process a single row from the CSV file"""
    result_data = {
        'result': None,
        'soft_failure': False,
        'presence': False,
        'citations': None
    }
    
    if row['status'] == 'completed' and pd.notna(row['s3_output_path']):
        try:
            # Parse the S3 path
            s3_path = row['s3_output_path']
            bucket_name = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            # Get the file content from S3
            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            content = response['Body'].read().decode('utf-8')
            
            # Handle different file types
            file_extension = '.' + s3_path.split('.')[-1]
            
            if s3_path.endswith('.json'):
                # Parse JSON content
                json_content = json.loads(content)
                
                # Extract model-specific content
                extracted_content = extract_model_specific_content(json_content, row['llm_model_name'], file_extension)
                result_data['result'] = extracted_content
                
                # Check for soft failure
                result_data['soft_failure'] = check_soft_failure(json_content)
                
                # Check for AI Overview presence if it's GOOGLE_AI_OVERVIEW
                if row['llm_model_name'] == 'GOOGLE_AI_OVERVIEW':
                    result_data['presence'] = check_ai_overview_presence(json_content)
                
                # Extract citations
                citations = extract_citations(json_content)
                if citations:
                    result_data['citations'] = citations
                
            elif s3_path.endswith('.md'):
                # Extract model-specific content for markdown
                extracted_content = extract_model_specific_content(content, row['llm_model_name'], file_extension)
                result_data['result'] = extracted_content
                
                # Check for soft failure
                result_data['soft_failure'] = check_soft_failure(content)
                
                # Check for AI Overview presence if it's GOOGLE_AI_OVERVIEW
                if row['llm_model_name'] == 'GOOGLE_AI_OVERVIEW':
                    result_data['presence'] = check_ai_overview_presence(content)
                
                # Extract citations
                citations = extract_citations(content)
                if citations:
                    result_data['citations'] = citations
                
            else:
                # For other file types, extract model-specific content
                extracted_content = extract_model_specific_content(content, row['llm_model_name'], file_extension)
                result_data['result'] = extracted_content
                
                # Check for soft failure
                result_data['soft_failure'] = check_soft_failure(content)
                
                # Check for AI Overview presence if it's GOOGLE_AI_OVERVIEW
                if row['llm_model_name'] == 'GOOGLE_AI_OVERVIEW':
                    result_data['presence'] = check_ai_overview_presence(content)
                
                # Extract citations
                citations = extract_citations(content)
                if citations:
                    result_data['citations'] = citations
            
            logger.info(f"Processed row {index}: {s3_path}")
            
        except Exception as e:
            logger.error(f"Error processing row {index}: {str(e)}")
            result_data['result'] = f"Error: {str(e)}"
    else:
        result_data['result'] = "No output path or task not completed"
    
    return index, result_data

def analyze_content_from_csv(csv_content, max_workers=5):
    """Analyze content from CSV string and return enhanced CSV with analysis"""
    try:
        # Convert CSV string to DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Add new columns for analysis
        df['result'] = None
        df['soft_failure'] = False
        df['presence'] = False
        df['citations'] = None
        
        logger.info(f"Processing {len(df)} rows with {max_workers} workers...")
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all rows for processing
            future_to_index = {
                executor.submit(process_row, index, row, df): index 
                for index, row in df.iterrows()
            }
            
            # Process completed futures
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    row_index, result_data = future.result()
                    
                    # Thread-safe DataFrame update
                    with df_lock:
                        df.at[row_index, 'result'] = result_data['result']
                        df.at[row_index, 'soft_failure'] = result_data['soft_failure']
                        df.at[row_index, 'presence'] = result_data['presence']
                        df.at[row_index, 'citations'] = result_data['citations']
                        
                except Exception as e:
                    logger.error(f"Error in future for row {index}: {str(e)}")
                    with df_lock:
                        df.at[index, 'result'] = f"Future error: {str(e)}"
        
        # Convert result and citations columns to string for JSON objects
        for index, row in df.iterrows():
            # Result should now mostly be strings due to content extraction,
            # but handle any remaining dict objects
            if isinstance(row['result'], dict):
                df.at[index, 'result'] = json.dumps(row['result'])
            
            if isinstance(row['citations'], list):
                df.at[index, 'citations'] = json.dumps(row['citations'])
        
        # Convert back to CSV string
        output = StringIO()
        df.to_csv(output, index=False)
        enhanced_csv = output.getvalue()
        
        logger.info(f"Content analysis completed. Enhanced CSV with {len(df)} rows")
        return enhanced_csv, df
        
    except Exception as e:
        logger.error(f"Content analysis failed: {str(e)}")
        raise Exception(f"Content analysis failed: {str(e)}")

def lambda_handler(event, context):
    """Main Lambda Handler for Job Results Processing with Content Analysis"""
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
        
        # Step 2: Fetch completed job results
        logger.info(f"Step 2: Fetching results for completed job {job_id}")
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
        
        # Step 3: Convert to CSV
        logger.info(f"Step 3: Converting {len(results)} records to CSV")
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
        
        # Step 4: Analyze content from CSV
        logger.info(f"Step 4: Analyzing content for {len(results)} records")
        try:
            enhanced_csv, df = analyze_content_from_csv(csv_content, max_workers=5)
        except Exception as e:
            logger.error(f"Content analysis failed: {str(e)}")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'Content analysis failed',
                    'job_id': job_id,
                    'message': str(e)
                })
            }
        
        # Step 5: Save enhanced CSV to S3
        logger.info(f"Step 5: Saving enhanced CSV to S3 for job {job_id}")
        s3_url, filename = save_csv_to_s3(enhanced_csv, job_id)
        
        if not s3_url:
            logger.error("Failed to save enhanced CSV to S3")
            return {
                'statusCode': 500,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                'body': json.dumps({
                    'error': 'S3 upload failed',
                    'job_id': job_id,
                    'message': 'Enhanced CSV generated but could not be saved to S3'
                })
            }
        
        # Step 6: Calculate analysis summary
        analysis_summary = {
            'total_records': len(df),
            'records_with_content': len(df[df['result'].notna() & (df['result'] != "No output path or task not completed")]),
            'soft_failures': len(df[df['soft_failure'] == True]),
            'ai_overview_present': len(df[df['presence'] == True]),
            'records_with_citations': len(df[df['citations'].notna()])
        }
        
        # Step 7: Return success response
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
            'content_analysis': analysis_summary,
            'csv_details': {
                'generated': True,
                's3_location': s3_url,
                'filename': filename,
                'size_info': f"{len(results)} rows, {len(column_names) + 4} columns (with analysis)",
                'analysis_columns_added': ['result', 'soft_failure', 'presence', 'citations']
            },
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'processing_complete': True
        }
        
        logger.info(f"Job {job_id} processing completed successfully: {len(results)} records analyzed and saved to CSV")
        
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



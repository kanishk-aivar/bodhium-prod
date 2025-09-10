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

# Gemini AI imports
try:
    from google import genai
    from google.genai import types
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    logging.warning("Google GenAI not available. Brand analysis will be limited.")

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SECRET_NAME = os.environ.get('RDS_DB_SECRET', 'dev/rds')
REGION_NAME = os.environ.get('AWS_REGION', 'us-east-1')
CSV_OUTPUT_BUCKET = os.environ.get('CSV_OUTPUT_BUCKET')
CSV_OUTPUT_PATH = os.environ.get('CSV_OUTPUT_PATH', 'csv-job-results/')
GEMINI_AVAILABLE = os.environ.get('GEMINI_API_KEY') is not None

# AWS clients with timeout configuration
secrets_client = boto3.client(
    'secretsmanager', 
    region_name=REGION_NAME,
    config=boto3.session.Config(
        connect_timeout=10,
        read_timeout=30,
        retries={'max_attempts': 2}
    )
)
s3_client = boto3.client(
    's3', 
    region_name=REGION_NAME,
    config=boto3.session.Config(
        connect_timeout=10,
        read_timeout=30,
        retries={'max_attempts': 2}
    )
)

# Thread lock for DataFrame updates (no longer needed with new approach)

def get_rds_connection():
    """Get RDS connection from secrets manager"""
    try:
        logger.info(f"Retrieving secret: {SECRET_NAME} from region: {REGION_NAME}")
        
        # Add timeout and error handling for secrets retrieval
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError("Secrets Manager call timed out after 30 seconds")
        
        # Set a 30-second timeout for the secrets call
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(30)
        
        try:
            secret_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
            signal.alarm(0)  # Cancel the alarm
            logger.info("Secret retrieved successfully")
        except Exception as e:
            signal.alarm(0)  # Cancel the alarm
            logger.error(f"Failed to retrieve secret: {str(e)}")
            raise
        
        secret = json.loads(secret_response['SecretString'])
        logger.info("Secret parsed successfully")
        
        logger.info(f"Connecting to database: {secret.get('DB_HOST', 'unknown')}:{secret.get('DB_PORT', 5432)}")
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=int(secret.get('DB_PORT', 5432)),
            connect_timeout=10,  # 10 second connection timeout
            options='-c statement_timeout=30s'  # 30 second query timeout
        )
        logger.info("Successfully connected to RDS PostgreSQL")
        return conn
    except Exception as e:
        logger.error(f"Failed connecting to RDS via Secrets Manager: {str(e)}")
        
        # Fallback: Try using environment variables
        logger.info("Attempting fallback connection using environment variables...")
        try:
            db_host = os.environ.get('DB_HOST')
            db_name = os.environ.get('DB_NAME') 
            db_user = os.environ.get('DB_USER')
            db_password = os.environ.get('DB_PASSWORD')
            db_port = os.environ.get('DB_PORT', '5432')
            
            if all([db_host, db_name, db_user, db_password]):
                logger.info(f"Using environment variables for connection: {db_host}:{db_port}")
                conn = psycopg2.connect(
                    host=db_host,
                    database=db_name,
                    user=db_user,
                    password=db_password,
                    port=int(db_port),
                    connect_timeout=10,
                    options='-c statement_timeout=30s'
                )
                logger.info("Successfully connected to RDS PostgreSQL using environment variables")
                return conn
            else:
                logger.error("Environment variables not available for database connection")
                
        except Exception as env_error:
            logger.error(f"Fallback connection also failed: {str(env_error)}")
        
        raise Exception(f"Database connection failed: {str(e)}")

def check_job_completion_status(job_id):
    """Check if all tasks for a job are completed"""
    logger.info(f"Starting job completion check for job_id: {job_id}")
    
    try:
        logger.info("Attempting to get RDS connection...")
        conn = get_rds_connection()
        logger.info("RDS connection established successfully")
    except Exception as e:
        logger.error(f"Failed to establish RDS connection: {str(e)}")
        return {
            'job_exists': False,
            'error': f'Database connection failed: {str(e)}'
        }
    
    try:
        with conn.cursor() as cur:
            # First, try a simple existence check
            logger.info(f"Testing simple query for job_id: {job_id}")
            simple_query = "SELECT COUNT(*) FROM public.llmtasks WHERE job_id = %s"
            
            # Set a query timeout
            cur.execute("SET statement_timeout = '30s'")
            logger.info("Query timeout set to 30 seconds")
            
            cur.execute(simple_query, (job_id,))
            simple_result = cur.fetchone()
            logger.info(f"Simple count query result: {simple_result}")
            
            if not simple_result or simple_result[0] == 0:
                logger.warning(f"No tasks found for job_id: {job_id}")
                return {
                    'job_exists': False,
                    'error': f'Job {job_id} not found or has no associated tasks'
                }
            
            logger.info(f"Found {simple_result[0]} tasks, executing detailed status query...")
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
            logger.info("Detailed query executed, fetching results...")
            result = cur.fetchone()
            logger.info(f"Detailed query result fetched: {result}")
            
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
        try:
            if conn:
                conn.close()
                logger.info("Database connection closed successfully")
        except Exception as close_error:
            logger.warning(f"Error closing database connection: {str(close_error)}")

def debug_job_status(job_id):
    """Debug function to check job status with minimal queries"""
    logger.info(f"=== DEBUG: Starting debug for job_id: {job_id} ===")
    
    try:
        conn = get_rds_connection()
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'llmtasks')")
            table_exists = cur.fetchone()[0]
            logger.info(f"DEBUG: llmtasks table exists: {table_exists}")
            
            if table_exists:
                # Check total records in table
                cur.execute("SELECT COUNT(*) FROM public.llmtasks")
                total_records = cur.fetchone()[0]
                logger.info(f"DEBUG: Total records in llmtasks: {total_records}")
                
                # Check records for this job
                cur.execute("SELECT COUNT(*) FROM public.llmtasks WHERE job_id = %s", (job_id,))
                job_records = cur.fetchone()[0]
                logger.info(f"DEBUG: Records for job {job_id}: {job_records}")
                
                if job_records > 0:
                    # Check status distribution
                    cur.execute("""
                        SELECT status, COUNT(*) 
                        FROM public.llmtasks 
                        WHERE job_id = %s 
                        GROUP BY status
                    """, (job_id,))
                    status_dist = cur.fetchall()
                    logger.info(f"DEBUG: Status distribution for job {job_id}: {status_dist}")
        
        conn.close()
        logger.info("=== DEBUG: Debug completed ===")
        
    except Exception as e:
        logger.error(f"DEBUG: Error during debug: {str(e)}")

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
        
        # Generate filename with timestamp in Adhoc/{job_id}/ format
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"Adhoc/{job_id}/{timestamp}.csv"
        
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

def generate_presigned_url(bucket_name, key, expiration=3600):
    """Generate a presigned GET URL for S3 object"""
    try:
        # Create a presigned URL that expires in 1 hour (3600 seconds) by default
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket_name,
                'Key': key
            },
            ExpiresIn=expiration
        )
        
        logger.info(f"Generated presigned URL for s3://{bucket_name}/{key}")
        return presigned_url
        
    except Exception as e:
        logger.error(f"Failed to generate presigned URL for s3://{bucket_name}/{key}: {str(e)}")
        return None

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

def extract_citations_from_s3(s3_path):
    """Extract citations from citations.json file in S3 folder"""
    try:
        # Parse the S3 path to get the folder
        bucket_name = s3_path.split('/')[2]
        folder_key = '/'.join(s3_path.split('/')[3:-1])  # Remove the filename, keep folder path
        
        if not folder_key:
            return []
        
        # Try to read citations.json
        citations_json_key = f"{folder_key}/citations.json"
        
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=citations_json_key)
            citations_data = json.loads(response['Body'].read().decode('utf-8'))
            citations_urls = citations_data.get('citations', [])
            
            logger.info(f"Extracted {len(citations_urls)} citations from citations.json")
            return citations_urls
            
        except Exception as e:
            logger.info(f"citations.json not found for {s3_path}: {str(e)}")
            return []
            
    except Exception as e:
        logger.error(f"Error extracting citations from S3 for {s3_path}: {str(e)}")
        return []

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
            elif model_name == 'ChatGPT':
                # For ChatGPT JSON files, extract the content field
                return content.get('content', '')
        elif isinstance(content, str):
            try:
                json_content = json.loads(content)
                if model_name in ['GOOGLE_AI_MODE', 'GOOGLE_AI_OVERVIEW', 'perplexity']:
                    return json_content.get('content', '')
                elif model_name == 'ChatGPT':
                    # For ChatGPT JSON files, extract the content field
                    return json_content.get('content', '')
            except:
                pass
    
    elif file_extension == '.md' and model_name == 'ChatGPT':
        # For ChatGPT markdown files, extract only the Response Content section
        if isinstance(content, str):
            # Find the "## Response Content" section
            response_content_match = re.search(r'## Response Content\s*\n\n(.*?)(?=\n---|\n###|\Z)', content, re.DOTALL)
            if response_content_match:
                return response_content_match.group(1).strip()
            # If no Response Content section found, return the full content
            return content
    
    # Fallback: return original content if no specific extraction rule applies
    return content

def extract_brand_names_batch(query_texts):
    """Extract brand names and categories from multiple query texts using Gemini AI in a single request"""
    if not GEMINI_AVAILABLE:
        logger.warning("Gemini AI not available, cannot extract brand names")
        return ["None"] * len(query_texts), ["Non-Branded"] * len(query_texts)
    
    if not query_texts:
        return [], []
    
    try:
        # Get Gemini API key from environment
        gemini_api_key = os.environ.get("GEMINI_API_KEY")
        if not gemini_api_key:
            logger.warning("GEMINI_API_KEY not found in environment variables")
            return ["None"] * len(query_texts), ["Non-Branded"] * len(query_texts)
        
        client = genai.Client(api_key=gemini_api_key)
        model = "gemini-2.5-flash"
        
        # Build the batch query text
        batch_query = "Extract brand names and categorize queries from the following:\n\n"
        for i, query_text in enumerate(query_texts, 1):
            batch_query += f"Query {i}: {query_text}\n"
        
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text="""Extract brand names and categorize queries from the following:

Query 1: What are the best Apple smartphones compared to Samsung in USA?
Query 2: What are the best Samsung tablets compared to Apple in USA?
Query 3: How to choose the best laptop for gaming?"""),
                ],
            ),
            types.Content(
                role="model",
                parts=[
                    types.Part.from_text(text="""[
    {"query_number": 1, "brand_name": "Apple", "brand_category": "Branded"},
    {"query_number": 2, "brand_name": "Samsung", "brand_category": "Branded"},
    {"query_number": 3, "brand_name": "None", "brand_category": "Non-Branded"}
]"""),
                ],
            ),
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=batch_query),
                ],
            ),
        ]
        
        generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_budget=0,
            ),
            response_mime_type="application/json",
            system_instruction=[
                types.Part.from_text(text=f"""You are tasked to extract the brand name and categorize each given google search query.
You should extract only one brand per query and determine if the query is branded or non-branded.

Return a JSON array with {len(query_texts)} objects, one for each query in order.
Each object should have:
- "query_number": the sequential number (1, 2, 3, etc.)
- "brand_name": the extracted brand name or "None" if no brand is present
- "brand_category": "Branded" if a brand name is found, "Non-Branded" if no brand is present

A query is considered "Branded" if it explicitly mentions a specific brand name (e.g., Apple, Samsung, Nike, Coca-Cola, etc.).
A query is considered "Non-Branded" if it asks about general categories, features, or comparisons without mentioning specific brand names.

Example format:
[
    {{"query_number": 1, "brand_name": "Apple", "brand_category": "Branded"}},
    {{"query_number": 2, "brand_name": "None", "brand_category": "Non-Branded"}},
    {{"query_number": 3, "brand_name": "Samsung", "brand_category": "Branded"}}
]"""),
            ],
        )
        
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=generate_content_config,
        )
        
        # Parse the response to extract brand names and categories
        try:
            response_text = response.text
            response_json = json.loads(response_text)
            
            # Extract brand names and categories in order
            brand_names = []
            brand_categories = []
            if isinstance(response_json, list):
                # Sort by query_number to ensure correct order
                sorted_results = sorted(response_json, key=lambda x: x.get("query_number", 0))
                for result in sorted_results:
                    brand_name = result.get("brand_name", "None")
                    brand_category = result.get("brand_category", "Non-Branded")
                    brand_names.append(brand_name)
                    brand_categories.append(brand_category)
                
                # Ensure we have the right number of results
                while len(brand_names) < len(query_texts):
                    brand_names.append("None")
                    brand_categories.append("Non-Branded")
                
                # Trim if we got too many results
                brand_names = brand_names[:len(query_texts)]
                brand_categories = brand_categories[:len(query_texts)]
            else:
                # Fallback if response format is unexpected
                brand_names = ["None"] * len(query_texts)
                brand_categories = ["Non-Branded"] * len(query_texts)
            
            logger.info(f"Extracted {len(brand_names)} brand names and categories from batch of {len(query_texts)} queries")
            return brand_names, brand_categories
            
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse Gemini batch response as JSON: {response.text}")
            return ["None"] * len(query_texts), ["Non-Branded"] * len(query_texts)
            
    except Exception as e:
        logger.error(f"Error extracting brand names and categories from batch of {len(query_texts)} queries: {str(e)}")
        return ["None"] * len(query_texts), ["Non-Branded"] * len(query_texts)

def extract_brand_name_from_query(query_text):
    """Extract brand name from single query text (wrapper for batch function)"""
    if not query_text:
        return "None", "Non-Branded"
    
    brand_names, brand_categories = extract_brand_names_batch([query_text])
    return (brand_names[0] if brand_names else "None", 
            brand_categories[0] if brand_categories else "Non-Branded")

def analyze_citations_and_brand(s3_path, brand_name):
    """Analyze citations and brand presence from S3 folder"""
    result = {
        'citation_presence': False,
        'citation_count': 0,
        'brand_count': 0
    }
    
    try:
        # Parse the S3 path to get the folder
        # s3_path format: s3://bucket/something/response.json
        # We need to read from: s3://bucket/something/
        bucket_name = s3_path.split('/')[2]
        folder_key = '/'.join(s3_path.split('/')[3:-1])  # Remove the filename, keep folder path
        
        if not folder_key:
            logger.warning(f"No folder path found in S3 path: {s3_path}")
            return result
        
        # First, try to read citations.json (new format)
        citations_json_key = f"{folder_key}/citations.json"
        citations_urls = []
        
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=citations_json_key)
            citations_data = json.loads(response['Body'].read().decode('utf-8'))
            citations_urls = citations_data.get('citations', [])
            
            # Set citation presence and count from citations.json
            result['citation_presence'] = len(citations_urls) > 0
            result['citation_count'] = len(citations_urls)
            
            logger.info(f"Found citations.json with {len(citations_urls)} citations")
            
            # For brand analysis, we still need to read the citation markdown files
            if brand_name != "None" and citations_urls:
                # List citation markdown files for brand analysis
                try:
                    list_response = s3_client.list_objects_v2(
                        Bucket=bucket_name,
                        Prefix=folder_key + "/citation_",
                        MaxKeys=1000
                    )
                    
                    if 'Contents' in list_response:
                        citation_files = [obj['Key'] for obj in list_response['Contents'] if obj['Key'].endswith('.md')]
                        
                        # Analyze each citation file for brand mentions
                        for citation_file in citation_files:
                            try:
                                file_response = s3_client.get_object(Bucket=bucket_name, Key=citation_file)
                                content = file_response['Body'].read().decode('utf-8')
                                
                                # Check if brand name is present in this citation
                                if brand_name.lower() in content.lower():
                                    result['brand_count'] += 1
                                    
                            except Exception as e:
                                logger.warning(f"Failed to read citation file {citation_file}: {str(e)}")
                                continue
                                
                except Exception as e:
                    logger.warning(f"Failed to list citation files for brand analysis: {str(e)}")
            
            logger.info(f"Citation analysis for '{brand_name}': {result['citation_count']} citations, {result['brand_count']} with brand mentions")
            return result
            
        except Exception as e:
            logger.info(f"citations.json not found, falling back to legacy method: {str(e)}")
        
        # Fallback: Legacy method - count citation markdown files
        citation_files = []
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder_key + "/citation_",
                MaxKeys=1000
            )
            
            if 'Contents' in response:
                citation_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.md')]
        except Exception as e:
            logger.warning(f"Failed to list citation files in folder {folder_key}: {str(e)}")
            return result
        
        # Set citation presence and count
        result['citation_presence'] = len(citation_files) > 0
        result['citation_count'] = len(citation_files)
        
        if not citation_files:
            logger.info(f"No citation files found in folder: {folder_key}")
            return result
        
        # Analyze each citation file for brand mentions
        if brand_name != "None":
            for citation_file in citation_files:
                try:
                    response = s3_client.get_object(Bucket=bucket_name, Key=citation_file)
                    content = response['Body'].read().decode('utf-8')
                    
                    # Check if brand name is present in this citation
                    if brand_name.lower() in content.lower():
                        result['brand_count'] += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to read citation file {citation_file}: {str(e)}")
                    continue
        
        logger.info(f"Citation analysis for '{brand_name}': {result['citation_count']} citations, {result['brand_count']} with brand mentions")
        
        return result
        
    except Exception as e:
        logger.error(f"Error analyzing citations for {s3_path}: {str(e)}")
        return result

def process_row(index, row, df):
    """Process a single row from the CSV file"""
    result_data = {
        'result': None,
        'soft_failure': False,
        'presence': False,
        'citations': None,
        'brand_name': 'None',
        'brand_category': 'Non-Branded',
        'citation_presence': False,
        'citation_count': 0,
        'brand_count': 0,
        'brand_present': False
    }
    
    # Extract brand name and category from query text
    if pd.notna(row['query_text']):
        brand_name, brand_category = extract_brand_name_from_query(row['query_text'])
        result_data['brand_name'] = brand_name
        result_data['brand_category'] = brand_category
    
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
                    citations = extract_citations(content)
                if citations:
                    result_data['citations'] = citations
            
            # Analyze citations and brand presence
            citation_analysis = analyze_citations_and_brand(s3_path, result_data['brand_name'])
            result_data['citation_presence'] = citation_analysis['citation_presence']
            result_data['citation_count'] = citation_analysis['citation_count']
            result_data['brand_count'] = citation_analysis['brand_count']
            result_data['brand_present'] = citation_analysis['brand_count'] > 0
            
            logger.info(f"Processed row {index}: {s3_path}")
            
        except Exception as e:
            logger.error(f"Error processing row {index}: {str(e)}")
            result_data['result'] = f"Error: {str(e)}"
    else:
        result_data['result'] = "No output path or task not completed"
    
    return index, result_data

def process_row_content_only(index, row, df):
    """Process a single row for content analysis only (brand name already extracted)"""
    result_data = {
        'result': None,
        'soft_failure': False,
        'presence': False,
        'citations': None,
        'citation_presence': False,
        'citation_count': 0,
        'brand_count': 0,
        'brand_present': False
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
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
                
                # Extract citations - try S3 first, then fallback to content parsing
                citations = extract_citations_from_s3(s3_path)
                if not citations:
                    citations = extract_citations(content)
                if citations:
                    result_data['citations'] = citations
            
            # Analyze citations and brand presence using already extracted brand name
            brand_name = row.get('brand_name', 'None')
            citation_analysis = analyze_citations_and_brand(s3_path, brand_name)
            result_data['citation_presence'] = citation_analysis['citation_presence']
            result_data['citation_count'] = citation_analysis['citation_count']
            result_data['brand_count'] = citation_analysis['brand_count']
            result_data['brand_present'] = citation_analysis['brand_count'] > 0
            
            logger.info(f"Processed content for row {index}: {s3_path}")
            
        except Exception as e:
            logger.error(f"Error processing content for row {index}: {str(e)}")
            result_data['result'] = f"Error: {str(e)}"
    else:
        result_data['result'] = "No output path or task not completed"
    
    return index, result_data

def analyze_content_from_csv(csv_content, max_workers=5):
    """Analyze content from CSV string and return enhanced CSV with analysis"""
    try:
        # Convert CSV string to DataFrame
        df = pd.read_csv(StringIO(csv_content))
        
        # Add new columns for analysis in the specified order
        df['result'] = None
        df['soft_failure'] = False
        df['presence'] = False
        df['citation_presence'] = False
        df['citation_count'] = 0
        df['citations'] = None
        df['brand_name'] = 'None'
        df['brand_category'] = 'Non-Branded'
        df['brand_present'] = False
        df['brand_count'] = 0
        
        logger.info(f"Processing {len(df)} rows with {max_workers} workers...")
        
        # Step 1: Batch extract brand names for all queries
        logger.info("Step 1: Batch extracting brand names from all queries...")
        query_texts = []
        query_indices = []
        
        for index, row in df.iterrows():
            if pd.notna(row['query_text']):
                query_texts.append(row['query_text'])
                query_indices.append(index)
        
        # Process queries in batches of 100
        batch_size = 100
        for i in range(0, len(query_texts), batch_size):
            batch_queries = query_texts[i:i+batch_size]
            batch_indices = query_indices[i:i+batch_size]
            
            logger.info(f"Processing batch {i//batch_size + 1}: {len(batch_queries)} queries")
            brand_names, brand_categories = extract_brand_names_batch(batch_queries)
            
            # Update DataFrame with extracted brand names and categories
            for j, (brand_name, brand_category) in enumerate(zip(brand_names, brand_categories)):
                if j < len(batch_indices):
                    df.at[batch_indices[j], 'brand_name'] = brand_name
                    df.at[batch_indices[j], 'brand_category'] = brand_category
        
        # Step 2: Process content analysis and brand visibility in parallel
        logger.info("Step 2: Processing content analysis and brand visibility...")
        
        # Create a thread-safe results dictionary
        results_dict = {}
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all rows for processing
            future_to_index = {
                executor.submit(process_row_content_only, index, row, df): index 
                for index, row in df.iterrows()
            }
            
            # Process completed futures and collect results
            for future in as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    row_index, result_data = future.result()
                    results_dict[row_index] = result_data
                except Exception as e:
                    logger.error(f"Error in future for row {index}: {str(e)}")
                    results_dict[index] = {
                        'result': f"Future error: {str(e)}",
                        'soft_failure': False,
                        'presence': False,
                        'citations': None,
                        'citation_presence': False,
                        'citation_count': 0,
                        'brand_count': 0,
                        'brand_present': False
                    }
        
        # Step 3: Update DataFrame with collected results (single-threaded, safe)
        logger.info("Step 3: Updating DataFrame with collected results...")
        for row_index, result_data in results_dict.items():
            df.at[row_index, 'result'] = result_data['result']
            df.at[row_index, 'soft_failure'] = result_data['soft_failure']
            df.at[row_index, 'presence'] = result_data['presence']
            df.at[row_index, 'citations'] = result_data['citations']
            df.at[row_index, 'citation_presence'] = result_data['citation_presence']
            df.at[row_index, 'citation_count'] = result_data['citation_count']
            df.at[row_index, 'brand_count'] = result_data['brand_count']
            df.at[row_index, 'brand_present'] = result_data['brand_present']
        
        # Convert result and citations columns to string for JSON objects
        for index, row in df.iterrows():
            # Result should now mostly be strings due to content extraction,
            # but handle any remaining dict objects
            if isinstance(row['result'], dict):
                df.at[index, 'result'] = json.dumps(row['result'])
            
            if isinstance(row['citations'], list):
                df.at[index, 'citations'] = json.dumps(row['citations'])
        
        # Reorder columns to match the specified order
        desired_column_order = [
            'task_id', 'job_id', 'query_id', 'llm_model_name', 'query_text', 'query_type', 
            'status', 's3_output_path', 'error_message', 'created_at', 'completed_at', 
            'is_active', 'duration_seconds', 'result', 'soft_failure', 'presence', 
            'citation_presence', 'citation_count', 'citations', 'brand_name', 'brand_category',
            'brand_present', 'brand_count'
        ]
        
        # Ensure all columns exist and reorder
        existing_columns = df.columns.tolist()
        final_columns = [col for col in desired_column_order if col in existing_columns]
        
        # Add any remaining columns that weren't in the desired order (shouldn't happen, but safety check)
        remaining_columns = [col for col in existing_columns if col not in desired_column_order]
        if remaining_columns:
            logger.warning(f"Found unexpected columns: {remaining_columns}")
            final_columns.extend(remaining_columns)
        
        df = df[final_columns]
        logger.info(f"Reordered columns to: {final_columns}")
        
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
        
        # Log environment diagnostics
        logger.info(f"Environment diagnostics:")
        logger.info(f"  AWS_REGION: {REGION_NAME}")
        logger.info(f"  SECRET_NAME: {SECRET_NAME}")
        logger.info(f"  CSV_OUTPUT_BUCKET: {CSV_OUTPUT_BUCKET}")
        logger.info(f"  Lambda timeout: {context.get_remaining_time_in_millis() / 1000:.1f}s remaining")
        
        # Test AWS connectivity
        try:
            logger.info("Testing AWS Secrets Manager connectivity...")
            # Quick test to see if we can at least list secrets (doesn't require specific secret access)
            secrets_client.list_secrets(MaxResults=1)
            logger.info("AWS Secrets Manager connectivity: OK")
        except Exception as conn_test_error:
            logger.error(f"AWS Secrets Manager connectivity test failed: {str(conn_test_error)}")
        
        # Test specific secret access
        try:
            logger.info(f"Testing access to specific secret: {SECRET_NAME}")
            secrets_client.describe_secret(SecretId=SECRET_NAME)
            logger.info("Secret access test: OK")
        except Exception as secret_test_error:
            logger.error(f"Secret access test failed: {str(secret_test_error)}")
            logger.error("This may indicate IAM permission issues or incorrect secret name")
        
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
        
        # Add debug information for troubleshooting
        debug_job_status(job_id)
        
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
            enhanced_csv, df = analyze_content_from_csv(csv_content, max_workers=50)
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
        
        # Step 5.5: Generate presigned URL for direct download
        logger.info(f"Step 5.5: Generating presigned URL for {filename}")
        presigned_url = generate_presigned_url(CSV_OUTPUT_BUCKET, filename, expiration=3600)  # 1 hour expiration
        
        if not presigned_url:
            logger.warning("Failed to generate presigned URL, but CSV was saved successfully")
            presigned_url = None
        else:
            logger.info(f"Presigned URL generated successfully, expires in 1 hour")
        
        # Step 7: Return success response
        success_response = {
            'status': 'completed',
            'job_id': job_id,
            'csv_details': {
                'generated': True,
                's3_location': s3_url,
                'filename': filename
            },
            'download': {
                'presigned_url': presigned_url,
                'expires_in': '1 hour',
                'direct_download': presigned_url is not None
            }
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
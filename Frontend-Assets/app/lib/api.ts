import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda"
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3"
import { Pool } from "pg"

// AWS Configuration - Uses IAM Task Role when running in ECS
const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || "us-east-1",
  // No explicit credentials - uses default credential provider chain
  // This will automatically use:
  // 1. ECS Task Role (when running in ECS)
  // 2. EC2 Instance Profile (when running on EC2)  
  // 3. Environment variables (when running locally)
})

const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  // Uses same credential provider chain as Lambda client
})

// Lambda ARNs from environment variables
export const LAMBDA_ARNS = {
  WEBSCRAPPER: process.env.LAMBDA_WEBSCRAPPER_ARN!,
  QUERY_GENERATOR: process.env.LAMBDA_QUERY_GENERATOR_ARN!,
  LLM_ORCHESTRATOR: process.env.LAMBDA_LLM_ORCHESTRATOR_ARN!,
}

// PostgreSQL Connection Pool
let pool: Pool | null = null

function getPool() {
  if (!pool) {
    pool = new Pool({
      host: process.env.RDS_HOST,
      port: Number.parseInt(process.env.RDS_PORT || "5432"),
      database: process.env.RDS_DATABASE,
      user: process.env.RDS_USERNAME,
      password: process.env.RDS_PASSWORD,
      ssl: {
        rejectUnauthorized: false, // For AWS RDS
      },
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    })
  }
  return pool
}

// Lambda Invocation Helper
export async function invokeLambda(functionName: string, payload: any) {
  try {
    const command = new InvokeCommand({
      FunctionName: functionName,
      Payload: JSON.stringify(payload),
      InvocationType: "RequestResponse",
    })

    const response = await lambdaClient.send(command)

    if (response.Payload) {
      const result = JSON.parse(new TextDecoder().decode(response.Payload))
      return result
    }

    throw new Error("No response from Lambda function")
  } catch (error) {
    console.error("Lambda invocation error:", error)
    throw error
  }
}

// Database Query Helper
export async function executeQuery(sql: string, parameters: any[] = []) {
  const client = getPool()
  try {
    const result = await client.query(sql, parameters)
    return result.rows
  } catch (error) {
    console.error("Database query error:", error)
    throw error
  }
}

// API Response Helper
export function createApiResponse(data: any, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization",
    },
  })
}

export function createErrorResponse(message: string, status = 500) {
  return createApiResponse({ error: message }, status)
}

// S3 Helper Functions for new bucket structure
const S3_JOBS_OUTPUT = process.env.S3_JOBS_OUTPUT

export async function listS3Objects(prefix: string) {
  try {
    const command = new ListObjectsV2Command({
      Bucket: S3_JOBS_OUTPUT,
      Prefix: prefix,
    })
    
    const response = await s3Client.send(command)
    return response.Contents || []
  } catch (error) {
    console.error("S3 list objects error:", error)
    throw error
  }
}

export async function getS3Object(s3Path: string) {
  try {
    // Strip s3://{bucket_name}/ prefix if present
    let key = s3Path
    if (s3Path.startsWith(`s3://${S3_JOBS_OUTPUT}/`)) {
      key = s3Path.replace(`s3://${S3_JOBS_OUTPUT}/`, '')
    } else if (s3Path.startsWith('s3://')) {
      // Handle other S3 URI formats
      const parts = s3Path.replace('s3://', '').split('/')
      if (parts.length > 1) {
        key = parts.slice(1).join('/') // Remove bucket name, keep the rest as key
      }
    }

    console.log(`Fetching S3 object with key: ${key} from bucket: ${S3_JOBS_OUTPUT}`)
    
    const command = new GetObjectCommand({
      Bucket: S3_JOBS_OUTPUT,
      Key: key,
    })
    
    const response = await s3Client.send(command)
    
    if (response.Body) {
      const content = await response.Body.transformToString()
      
      // Parse JSON files, return markdown as-is
      if (key.endsWith('.json')) {
        return JSON.parse(content)
      } else {
        return content
      }
    }
    
    throw new Error("No content found in S3 object")
  } catch (error) {
    console.error("S3 get object error:", error)
    throw error
  }
}

export async function getAllJobResults() {
  try {
    // List all objects in the bucket to get job structure
    const objects = await listS3Objects("")
    
    // Group objects by job_id and product_id
    const jobResults: Record<string, Record<string, any[]>> = {}
    
    for (const object of objects) {
      if (!object.Key) continue
      
      // Parse path: {job_id}/{product_id}/{worker_file}
      const pathParts = object.Key.split('/')
      if (pathParts.length !== 3) continue
      
      const [jobId, productId, fileName] = pathParts
      
      if (!jobResults[jobId]) {
        jobResults[jobId] = {}
      }
      if (!jobResults[jobId][productId]) {
        jobResults[jobId][productId] = []
      }
      
      // Get file content
      try {
        const content = await getS3Object(object.Key)
        
        jobResults[jobId][productId].push({
          fileName,
          content,
          key: object.Key,
          lastModified: object.LastModified,
        })
      } catch (error) {
        console.error(`Failed to get content for ${object.Key}:`, error)
      }
    }
    
    return jobResults
  } catch (error) {
    console.error("Error getting all job results:", error)
    throw error
  }
}

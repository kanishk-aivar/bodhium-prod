import { NextRequest, NextResponse } from "next/server"
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda"
import { S3Client, GetObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"

const lambdaClient = new LambdaClient({
  region: process.env.AWS_REGION || 'us-east-1',
})

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
})

const LAMBDA_ARN = process.env.LAMBDA_LLM_ADHOC_ORCHESTRATOR_ARN
const S3_ADHOC_DOWNLOAD_BUCKET = process.env.S3_ADHOC_DOWNLOAD_BUCKET

interface LambdaResponse {
  status: string
  job_id: string
  csv_details: {
    generated: boolean
    s3_location: string
    filename: string
  }
  download?: {
    presigned_url: string
    expires_in: string
    direct_download: boolean
  }
}

interface S3CsvFile {
  job_id: string
  filename: string
  s3_key: string
  last_modified: string
  size: number
}

export async function GET(request: NextRequest) {
  try {
    if (!S3_ADHOC_DOWNLOAD_BUCKET) {
      return NextResponse.json(
        { error: 'S3 bucket not configured' },
        { status: 500 }
      )
    }

    // List all CSV files in the Adhoc/ prefix
    const listCommand = new ListObjectsV2Command({
      Bucket: S3_ADHOC_DOWNLOAD_BUCKET,
      Prefix: 'Adhoc/',
      MaxKeys: 1000 // Adjust as needed
    })

    const listResponse = await s3Client.send(listCommand)
    
    if (!listResponse.Contents) {
      return NextResponse.json({
        success: true,
        csv_files: []
      })
    }

    // Parse S3 objects and group by job_id, keeping only the latest file per job
    const jobFiles = new Map<string, S3CsvFile>()
    
    for (const object of listResponse.Contents) {
      if (!object.Key || !object.Key.endsWith('.csv')) {
        continue
      }

      // Parse the S3 key: "Adhoc/{job_id}/timestamp.csv"
      const keyParts = object.Key.split('/')
      if (keyParts.length !== 3 || keyParts[0] !== 'Adhoc') {
        continue
      }

      const job_id = keyParts[1]
      const filename = keyParts[2]
      
      if (!object.LastModified) {
        continue
      }

      const csvFile: S3CsvFile = {
        job_id,
        filename,
        s3_key: object.Key,
        last_modified: object.LastModified.toISOString(),
        size: object.Size || 0
      }

      // Keep only the latest file per job_id
      const existing = jobFiles.get(job_id)
      if (!existing || new Date(csvFile.last_modified) > new Date(existing.last_modified)) {
        jobFiles.set(job_id, csvFile)
      }
    }

    return NextResponse.json({
      success: true,
      csv_files: Array.from(jobFiles.values())
    })

  } catch (error) {
    console.error('S3 CSV listing error:', error)
    return NextResponse.json(
      { error: 'Failed to list CSV files' },
      { status: 500 }
    )
  }
}

export async function POST(request: NextRequest) {
  try {
    const { job_id, action } = await request.json()

    if (!job_id) {
      return NextResponse.json(
        { error: 'job_id is required' },
        { status: 400 }
      )
    }

    // For download action, skip Lambda call entirely and go straight to S3
    if (action === 'download') {
      if (!S3_ADHOC_DOWNLOAD_BUCKET) {
        return NextResponse.json(
          { error: 'S3 bucket not configured' },
          { status: 500 }
        )
      }

      // Find the latest CSV file for this job_id directly from S3
      const listCommand = new ListObjectsV2Command({
        Bucket: S3_ADHOC_DOWNLOAD_BUCKET,
        Prefix: `Adhoc/${job_id}/`,
        MaxKeys: 10
      })

      const listResponse = await s3Client.send(listCommand)
      
      if (!listResponse.Contents || listResponse.Contents.length === 0) {
        throw new Error('No CSV file found for this job')
      }

      // Find the latest CSV file
      const csvFiles = listResponse.Contents
        .filter(obj => obj.Key && obj.Key.endsWith('.csv'))
        .sort((a, b) => (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0))
      
      if (csvFiles.length === 0) {
        throw new Error('No CSV file found for this job')
      }

      const s3Key = csvFiles[0].Key!

      // Create presigned URL for direct S3 download
      const getObjectCommand = new GetObjectCommand({
        Bucket: S3_ADHOC_DOWNLOAD_BUCKET,
        Key: s3Key,
      })

      const presignedUrl = await getSignedUrl(s3Client, getObjectCommand, {
        expiresIn: 3600 // 1 hour
      })

      return NextResponse.json({
        success: true,
        download_type: 'presigned_url',
        presigned_url: presignedUrl,
        expires_in: '1 hour',
        filename: s3Key.split('/').pop() || s3Key,
        metadata: {
          size_info: 'Unknown',
          generated_at: new Date().toISOString()
        }
      })
    }

    // For non-download actions, call Lambda
    if (!LAMBDA_ARN) {
      return NextResponse.json(
        { error: 'Lambda ARN not configured' },
        { status: 500 }
      )
    }

    // Call Lambda function to generate/get CSV
    const lambdaCommand = new InvokeCommand({
      FunctionName: LAMBDA_ARN,
      Payload: JSON.stringify({ job_id }),
    })

    const lambdaResponse = await lambdaClient.send(lambdaCommand)
    
    if (!lambdaResponse.Payload) {
      throw new Error('No payload returned from Lambda')
    }

    const responseText = new TextDecoder().decode(lambdaResponse.Payload)
    const lambdaResult = JSON.parse(responseText)

    // Parse the body if it's a string (typical Lambda response format)
    let parsedBody: LambdaResponse
    if (typeof lambdaResult.body === 'string') {
      parsedBody = JSON.parse(lambdaResult.body)
    } else {
      parsedBody = lambdaResult.body || lambdaResult
    }

    // Return job status and CSV info
    return NextResponse.json({
      success: true,
      data: parsedBody
    })

  } catch (error) {
    console.error('Ad-hoc CSV API error:', error)
    return NextResponse.json(
      { error: 'Failed to process CSV request' },
      { status: 500 }
    )
  }
}

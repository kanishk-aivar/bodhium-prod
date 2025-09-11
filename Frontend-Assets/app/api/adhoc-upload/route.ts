import { NextRequest, NextResponse } from "next/server"
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3"

const s3Client = new S3Client({
  region: process.env.AWS_REGION || 'us-east-1',
})

const S3_UPLOAD_BUCKET = process.env.S3_ADHOC_UPLOAD_BUCKET

// JSON Schema validation function
function validateJsonSchema(jsonData: any): { valid: boolean; error?: string } {
  try {
    // Check if it's an object
    if (typeof jsonData !== 'object' || jsonData === null) {
      return { valid: false, error: "JSON must be an object" }
    }

    // Check required fields
    if (!jsonData.queries) {
      return { valid: false, error: "Missing required field: 'queries'" }
    }


    // Validate queries array
    if (!Array.isArray(jsonData.queries)) {
      return { valid: false, error: "'queries' must be an array" }
    }

    if (jsonData.queries.length === 0) {
      return { valid: false, error: "'queries' array cannot be empty" }
    }

    // Check each query is a string
    for (let i = 0; i < jsonData.queries.length; i++) {
      if (typeof jsonData.queries[i] !== 'string') {
        return { valid: false, error: `Query at index ${i} must be a string` }
      }
      if (jsonData.queries[i].trim() === '') {
        return { valid: false, error: `Query at index ${i} cannot be empty` }
      }
    }

    return { valid: true }
  } catch (error) {
    return { valid: false, error: "Invalid JSON structure" }
  }
}

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData()
    const file = formData.get('file') as File

    if (!file) {
      return NextResponse.json(
        { error: 'No file provided' },
        { status: 400 }
      )
    }

    // Validate file type
    if (!file.name.endsWith('.json')) {
      return NextResponse.json(
        { error: 'Only JSON files are allowed' },
        { status: 400 }
      )
    }

    // Read file content
    const arrayBuffer = await file.arrayBuffer()
    const buffer = Buffer.from(arrayBuffer)

    // Validate JSON content and schema
    let jsonData: any
    try {
      jsonData = JSON.parse(buffer.toString())
    } catch (error) {
      return NextResponse.json(
        { error: 'Invalid JSON file' },
        { status: 400 }
      )
    }

    // Validate JSON schema
    const schemaValidation = validateJsonSchema(jsonData)
    if (!schemaValidation.valid) {
      return NextResponse.json(
        { error: `Invalid JSON schema: ${schemaValidation.error}` },
        { status: 400 }
      )
    }

    // Generate unique filename with timestamp
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const filename = `adhoc-${timestamp}-${file.name}`

    // Upload to S3
    const uploadCommand = new PutObjectCommand({
      Bucket: S3_UPLOAD_BUCKET,
      Key: filename,
      Body: buffer,
      ContentType: 'application/json',
      Metadata: {
        originalName: file.name,
        uploadedAt: new Date().toISOString(),
        mode: 'adhoc'
      }
    })

    await s3Client.send(uploadCommand)

    return NextResponse.json({
      success: true,
      filename,
      message: 'File uploaded successfully and processing started'
    })

  } catch (error) {
    console.error('Error uploading file:', error)
    return NextResponse.json(
      { error: 'Failed to upload file' },
      { status: 500 }
    )
  }
}

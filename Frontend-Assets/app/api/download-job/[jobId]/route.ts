import type { NextRequest } from "next/server"
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3"
import { listS3Objects, createApiResponse, createErrorResponse } from "../../../lib/api"
import JSZip from "jszip"

// S3 Configuration - Uses IAM Task Role when running in ECS
const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
})

const S3_JOBS_OUTPUT = process.env.S3_JOBS_OUTPUT

export async function GET(request: NextRequest, { params }: { params: { jobId: string } }) {
  try {
    const { jobId } = params

    // List all objects for this job_id
    const objects = await listS3Objects(jobId + "/")

    if (!objects || objects.length === 0) {
      return createErrorResponse("No files found for this job", 404)
    }

    // Create a zip file with all objects
    const zip = new JSZip()

    for (const object of objects) {
      if (object.Key && !object.Key.endsWith("/")) {
        try {
          const getCommand = new GetObjectCommand({
            Bucket: S3_JOBS_OUTPUT,
            Key: object.Key,
          })

          const response = await s3Client.send(getCommand)
          if (response.Body) {
            // Create a proper folder structure in the zip
            // Remove the job_id prefix to avoid duplication
            const fileName = object.Key.replace(jobId + "/", "")
            const content = await response.Body.transformToByteArray()
            zip.file(fileName, content)
          }
        } catch (fileError) {
          console.error(`Failed to get file ${object.Key}:`, fileError)
          // Continue with other files even if one fails
        }
      }
    }

    // Check if we have any files in the zip
    if (Object.keys(zip.files).length === 0) {
      return createErrorResponse("No downloadable files found for this job", 404)
    }

    // Generate zip file as buffer for direct download
    const zipBuffer = await zip.generateAsync({ type: "nodebuffer" })

    // Return the zip file directly
    return new Response(zipBuffer, {
      headers: {
        "Content-Type": "application/zip",
        "Content-Disposition": `attachment; filename="${jobId}_data.zip"`,
        "Content-Length": zipBuffer.length.toString(),
      },
    })
  } catch (error) {
    console.error("Job download API error:", error)
    return createErrorResponse("Failed to generate download")
  }
}

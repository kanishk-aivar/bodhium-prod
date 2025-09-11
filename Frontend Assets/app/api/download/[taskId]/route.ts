import type { NextRequest } from "next/server"
import { S3Client, GetObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3"
import { getSignedUrl } from "@aws-sdk/s3-request-presigner"
import { executeQuery, createApiResponse, createErrorResponse } from "../../../lib/api"
import JSZip from "jszip"

// S3 Configuration - Uses IAM Task Role when running in ECS
const s3Client = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  // No explicit credentials - uses default credential provider chain
  // This will automatically use:
  // 1. ECS Task Role (when running in ECS)
  // 2. EC2 Instance Profile (when running on EC2)  
  // 3. Environment variables (when running locally)
})

export async function GET(request: NextRequest, { params }: { params: { taskId: string } }) {
  try {
    const { taskId } = params

    // Get task details
    const records = await executeQuery(
      `
      SELECT s3_output_path, status
      FROM llmtasks
      WHERE task_id = $1
    `,
      [taskId],
    )

    if (records.length === 0) {
      return createErrorResponse("Task not found", 404)
    }

    const s3Path = records[0].s3_output_path
    const status = records[0].status

    if (status !== "completed" || !s3Path) {
      return createErrorResponse("Task not completed or no output available", 400)
    }

    // Extract bucket and key from S3 path
    const s3Url = new URL(s3Path)
    const bucket = s3Url.hostname.split(".")[0]
    const key = s3Url.pathname.substring(1)

    // Check if it's a folder (ends with /) or a single file
    if (key.endsWith("/")) {
      // It's a folder, list all objects and create a zip
      const listCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: key,
      })

      const listResponse = await s3Client.send(listCommand)

      if (!listResponse.Contents || listResponse.Contents.length === 0) {
        return createErrorResponse("No files found in the specified path", 404)
      }

      // Create a zip file with all objects
      const zip = new JSZip()

      for (const object of listResponse.Contents) {
        if (object.Key && !object.Key.endsWith("/")) {
          const getCommand = new GetObjectCommand({
            Bucket: bucket,
            Key: object.Key,
          })

          const response = await s3Client.send(getCommand)
          if (response.Body) {
            const fileName = object.Key.replace(key, "")
            const content = await response.Body.transformToByteArray()
            zip.file(fileName, content)
          }
        }
      }

      // Generate zip file
      const zipContent = await zip.generateAsync({ type: "blob" })

      // Create a temporary download URL for the zip
      const zipUrl = URL.createObjectURL(zipContent)

      return createApiResponse({
        download_url: zipUrl,
        is_zip: true,
        filename: `${taskId}_results.zip`,
      })
    } else {
      // Single file, generate presigned URL with forced download
      const inferredFilename = key.split("/").pop() || `${taskId}_results.json`

      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
        ResponseContentDisposition: `attachment; filename="${inferredFilename}"`,
      })

      const downloadUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 })

      return createApiResponse({
        download_url: downloadUrl,
        is_zip: false,
        filename: inferredFilename,
      })
    }
  } catch (error) {
    console.error("Download API error:", error)
    return createErrorResponse("Failed to generate download URL")
  }
}

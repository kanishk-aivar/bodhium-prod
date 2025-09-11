import type { NextRequest } from "next/server"
import { listS3Objects, createApiResponse, createErrorResponse } from "../../../lib/api"

export async function GET(request: NextRequest, { params }: { params: { jobId: string } }) {
  try {
    const { jobId } = params

    // Check if any objects exist for this job_id
    const objects = await listS3Objects(jobId + "/")
    
    const hasS3Data = objects && objects.length > 0

    return createApiResponse({
      jobId,
      hasS3Data,
      fileCount: objects?.length || 0
    })
  } catch (error) {
    console.error("Check job S3 API error:", error)
    return createErrorResponse("Failed to check S3 data")
  }
}

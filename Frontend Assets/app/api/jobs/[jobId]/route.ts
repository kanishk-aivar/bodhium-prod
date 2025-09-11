import type { NextRequest } from "next/server"
import { executeQuery, createApiResponse, createErrorResponse } from "../../../lib/api"

export async function GET(request: NextRequest, { params }: { params: { jobId: string } }) {
  try {
    const { jobId } = params

    const records = await executeQuery(
      `
      SELECT job_id, source_url, status, created_at, updated_at, brand_name
      FROM scrapejobs
      WHERE job_id = $1
    `,
      [jobId],
    )

    if (records.length === 0) {
      return createErrorResponse("Job not found", 404)
    }

    const job = {
      job_id: records[0].job_id,
      source_url: records[0].source_url,
      status: records[0].status,
      created_at: records[0].created_at,
      updated_at: records[0].updated_at,
      brand_name: records[0].brand_name,
    }

    return createApiResponse(job)
  } catch (error) {
    console.error("Job API error:", error)
    return createErrorResponse("Failed to fetch job")
  }
}

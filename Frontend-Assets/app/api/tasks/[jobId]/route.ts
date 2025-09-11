import type { NextRequest } from "next/server"
import { executeQuery, createApiResponse, createErrorResponse } from "../../../lib/api"

export async function GET(request: NextRequest, { params }: { params: { jobId: string } }) {
  try {
    const { jobId } = params

    const records = await executeQuery(
      `
      SELECT task_id, job_id, query_id, llm_model_name, status, s3_output_path, error_message, created_at, completed_at
      FROM llmtasks
      WHERE job_id = $1
      ORDER BY created_at DESC
    `,
      [jobId],
    )

    const tasks = records.map((record) => ({
      task_id: record.task_id,
      job_id: record.job_id,
      query_id: record.query_id,
      llm_model_name: record.llm_model_name,
      status: record.status,
      s3_output_path: record.s3_output_path,
      error_message: record.error_message,
      created_at: record.created_at,
      completed_at: record.completed_at,
    }))

    return createApiResponse(tasks)
  } catch (error) {
    console.error("Tasks API error:", error)
    return createErrorResponse("Failed to fetch tasks")
  }
}

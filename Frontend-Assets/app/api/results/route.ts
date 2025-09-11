import { executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"

export const dynamic = 'force-dynamic'

type TaskWithJobRow = {
  task_id: string
  job_id: string
  query_id: number | null
  llm_model_name: string | null
  status: string | null
  s3_output_path: string | null
  error_message: string | null
  created_at: string
  completed_at: string | null
  source_url: string | null
  brand_name: string | null
  job_created_at: string
  job_updated_at: string | null
  job_status: string | null
}

export async function GET() {
  try {
    const records = await executeQuery(`
      SELECT 
        t.task_id,
        t.job_id,
        t.query_id,
        t.llm_model_name,
        t.status,
        t.s3_output_path,
        t.error_message,
        t.created_at,
        t.completed_at,
        j.source_url,
        j.brand_name,
        j.created_at as job_created_at,
        j.updated_at as job_updated_at,
        j.status as job_status
      FROM llmtasks t
      LEFT JOIN scrapejobs j ON t.job_id = j.job_id
      WHERE j.job_id IS NOT NULL
      ORDER BY t.created_at DESC
    `)

    const tasksWithJobs = (records as TaskWithJobRow[]).map((record) => ({
      task_id: record.task_id,
      job_id: record.job_id,
      query_id: record.query_id,
      llm_model_name: record.llm_model_name,
      status: record.status,
      s3_output_path: record.s3_output_path,
      error_message: record.error_message,
      created_at: record.created_at,
      completed_at: record.completed_at,
      job: {
        job_id: record.job_id,
        source_url: record.source_url,
        brand_name: record.brand_name,
        created_at: record.job_created_at,
        updated_at: record.job_updated_at,
        status: record.job_status,
      },
    }))

    // Group tasks by job_id
    const groupedResults = tasksWithJobs.reduce((acc, task) => {
      const jobId = task.job_id
      if (!acc[jobId]) {
        acc[jobId] = {
          job: task.job,
          tasks: [],
        }
      }
      acc[jobId].tasks.push({
        task_id: task.task_id,
        job_id: task.job_id,
        query_id: task.query_id,
        llm_model_name: task.llm_model_name,
        status: task.status,
        s3_output_path: task.s3_output_path,
        error_message: task.error_message,
        created_at: task.created_at,
        completed_at: task.completed_at,
      })
      return acc
    }, {} as Record<string, { job: any; tasks: any[] }>)

    return createApiResponse(groupedResults)
  } catch (error) {
    console.error("Results API error:", error)
    return createErrorResponse("Failed to fetch results")
  }
}

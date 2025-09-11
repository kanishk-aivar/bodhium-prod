import { executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"

export const dynamic = 'force-dynamic'

interface AdHocJob {
  job_id: string
  created_at: string
  status: string
  total_tasks?: number
  completed_tasks?: number
  brand_name: string
}

export async function GET() {
  try {
    // Query for jobs that are Ad-hoc (where brand_name = 'Ad-hoc' or similar pattern)
    const records = await executeQuery(`
      SELECT 
        j.job_id,
        j.created_at,
        j.status,
        j.brand_name,
        COUNT(t.task_id) as total_tasks,
        SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as completed_tasks
      FROM scrapejobs j
      LEFT JOIN llmtasks t ON j.job_id = t.job_id
      WHERE LOWER(j.brand_name) = 'ad-hoc' OR j.brand_name ILIKE '%adhoc%'
      GROUP BY j.job_id, j.created_at, j.status, j.brand_name
      ORDER BY j.created_at DESC
    `)

    const jobs = (records as any[]).map((record) => ({
      job_id: record.job_id,
      created_at: record.created_at,
      status: record.status,
      brand_name: record.brand_name || 'Ad-hoc',
      total_tasks: parseInt(record.total_tasks) || 0,
      completed_tasks: parseInt(record.completed_tasks) || 0,
    })) as AdHocJob[]

    return createApiResponse({
      jobs,
      total_count: jobs.length
    })
  } catch (error) {
    console.error("Ad-hoc jobs API error:", error)
    return createErrorResponse("Failed to fetch Ad-hoc jobs")
  }
}

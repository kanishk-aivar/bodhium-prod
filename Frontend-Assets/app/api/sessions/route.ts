import { executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"
import type { NextRequest } from "next/server"

export const dynamic = 'force-dynamic'

interface SessionInfo {
  session_id: string
  session_start: string
  session_end: string
  total_tasks: number
  completed_tasks: number
  failed_tasks: number
  session_status: string
}

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const brand = searchParams.get('brand')
    
    let whereClause = "WHERE session_id IS NOT NULL"
    let queryParams: any[] = []
    
    if (brand && brand !== "all") {
      whereClause += " AND (p.brand_name = $1 OR j.brand_name = $1)"
      queryParams.push(brand)
    }
    
    const records = await executeQuery(`
      -- Get session IDs with human-readable timestamps and status summary
      SELECT
          t.session_id,
          MIN(t.created_at) as session_start,
          MAX(t.created_at) as session_end,
          COUNT(*) as total_tasks,
          COUNT(CASE WHEN t.status = 'completed' THEN 1 END) as completed_tasks,
          COUNT(CASE WHEN t.status = 'failed' THEN 1 END) as failed_tasks,
          CASE
              WHEN COUNT(CASE WHEN t.status = 'failed' THEN 1 END) > 0
              THEN 'Has Failures'
              WHEN COUNT(CASE WHEN t.status = 'running' THEN 1 END) > 0
              THEN 'In Progress'
              WHEN COUNT(CASE WHEN t.status = 'completed' THEN 1 END) = COUNT(*)
              THEN 'All Completed'
              ELSE 'Mixed Status'
          END as session_status
      FROM llmtasks t
      LEFT JOIN products p ON t.product_id = p.product_id
      LEFT JOIN scrapejobs j ON t.job_id = j.job_id
      ${whereClause}
      GROUP BY t.session_id
      ORDER BY session_start DESC
    `, queryParams)

    const sessions = (records as any[]).map((record) => ({
      session_id: record.session_id,
      session_start: record.session_start,
      session_end: record.session_end,
      total_tasks: parseInt(record.total_tasks) || 0,
      completed_tasks: parseInt(record.completed_tasks) || 0,
      failed_tasks: parseInt(record.failed_tasks) || 0,
      session_status: record.session_status
    })) as SessionInfo[]

    return createApiResponse(sessions)
  } catch (error) {
    console.error("Sessions API error:", error)
    return createErrorResponse("Failed to fetch sessions")
  }
}

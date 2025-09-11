import { executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"

export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    const records = await executeQuery(`
      SELECT 
        s.job_id, 
        s.source_url, 
        s.status, 
        s.created_at, 
        s.updated_at, 
        s.brand_name,
        COALESCE(p.product_count, 0) as product_count
      FROM scrapejobs s
      LEFT JOIN (
        SELECT job_id, COUNT(*) as product_count
        FROM products
        GROUP BY job_id
      ) p ON s.job_id = p.job_id
      ORDER BY s.created_at DESC
      LIMIT 50
    `)

    const jobs = records.map((record) => ({
      job_id: record.job_id,
      source_url: record.source_url,
      status: record.status,
      created_at: record.created_at,
      updated_at: record.updated_at,
      brand_name: record.brand_name,
      product_count: record.product_count,
    }))

    return createApiResponse(jobs)
  } catch (error) {
    console.error("Jobs API error:", error)
    return createErrorResponse("Failed to fetch jobs")
  }
}
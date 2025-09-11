import { executeQuery, createApiResponse, createErrorResponse } from "../../../lib/api"

export const dynamic = 'force-dynamic'

export async function GET() {
  try {
    // Get unique brands
    const brandsResult = await executeQuery(`
      SELECT DISTINCT p.brand_name
      FROM llmtasks t
      LEFT JOIN products p ON t.product_id = p.product_id
      WHERE p.brand_name IS NOT NULL AND p.brand_name != ''
      ORDER BY p.brand_name
    `)
    
    // Get unique statuses
    const statusesResult = await executeQuery(`
      SELECT DISTINCT status
      FROM llmtasks
      WHERE status IS NOT NULL AND status != ''
      ORDER BY status
    `)
    
    // Get unique models
    const modelsResult = await executeQuery(`
      SELECT DISTINCT llm_model_name
      FROM llmtasks
      WHERE llm_model_name IS NOT NULL AND llm_model_name != ''
      ORDER BY llm_model_name
    `)

    const brands = (brandsResult as any[]).map(row => row.brand_name)
    const statuses = (statusesResult as any[]).map(row => row.status)
    const models = (modelsResult as any[]).map(row => row.llm_model_name)

    return createApiResponse({
      brands,
      statuses,
      models
    })
  } catch (error) {
    console.error("Filter options API error:", error)
    return createErrorResponse("Failed to fetch filter options")
  }
}

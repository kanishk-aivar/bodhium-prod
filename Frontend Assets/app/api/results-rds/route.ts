import { executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"

export const dynamic = 'force-dynamic'

interface LLMTaskResult {
  task_id: string
  job_id: string | null
  session_id: string | null
  query_id: number | null
  llm_model_name: string | null
  status: string | null
  s3_output_path: string | null
  error_message: string | null
  created_at: string
  completed_at: string | null
  product_id: number | null
  product_name: string | null
  
  // Joined data
  query_text: string | null
  query_type: string | null
  brand_name: string | null
  product_data: any
}

export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url)
    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '30')
    const search = searchParams.get('search') || ''
    const brand = searchParams.get('brand') || ''
    const status = searchParams.get('status') || ''
    const model = searchParams.get('model') || ''
    const sessionId = searchParams.get('sessionId') || ''
    
    // Calculate offset for pagination
    const offset = (page - 1) * limit
    
    // Build WHERE clause based on filters
    let whereConditions = ['t.task_id IS NOT NULL']
    let queryParams: any[] = []
    let paramIndex = 1
    
    if (search) {
      whereConditions.push(`(
        LOWER(t.product_name) LIKE LOWER($${paramIndex}) OR 
        LOWER(q.query_text) LIKE LOWER($${paramIndex}) OR 
        LOWER(t.llm_model_name) LIKE LOWER($${paramIndex}) OR
        LOWER(p.brand_name) LIKE LOWER($${paramIndex})
      )`)
      queryParams.push(`%${search}%`)
      paramIndex++
    }
    
    if (brand && brand !== 'all') {
      whereConditions.push(`p.brand_name = $${paramIndex}`)
      queryParams.push(brand)
      paramIndex++
    }
    
    if (status && status !== 'all') {
      whereConditions.push(`t.status = $${paramIndex}`)
      queryParams.push(status)
      paramIndex++
    }
    
    if (model && model !== 'all') {
      whereConditions.push(`t.llm_model_name = $${paramIndex}`)
      queryParams.push(model)
      paramIndex++
    }
    
    if (sessionId && sessionId !== 'all') {
      whereConditions.push(`t.session_id = $${paramIndex}`)
      queryParams.push(sessionId)
      paramIndex++
    }
    
    const whereClause = whereConditions.join(' AND ')
    
    // Get total count for pagination
    const countQuery = `
      SELECT COUNT(*) as total_count
      FROM llmtasks t
      LEFT JOIN queries q ON t.query_id = q.query_id
      LEFT JOIN products p ON t.product_id = p.product_id
      LEFT JOIN scrapejobs j ON t.job_id = j.job_id
      WHERE ${whereClause}
    `
    
    const countResult = await executeQuery(countQuery, queryParams)
    const totalCount = (countResult as any[])[0]?.total_count || 0
    
    // Get paginated results
    const dataQuery = `
      SELECT 
        t.task_id,
        t.job_id,
        t.session_id,
        t.query_id,
        t.llm_model_name,
        t.status,
        t.s3_output_path,
        t.error_message,
        t.created_at,
        t.completed_at,
        t.product_id,
        t.product_name,
        q.query_text,
        q.query_type,
        p.brand_name,
        p.product_data,
        j.brand_name as job_brand_name
      FROM llmtasks t
      LEFT JOIN queries q ON t.query_id = q.query_id
      LEFT JOIN products p ON t.product_id = p.product_id
      LEFT JOIN scrapejobs j ON t.job_id = j.job_id
      WHERE ${whereClause}
      ORDER BY t.created_at DESC
      LIMIT $${paramIndex} OFFSET $${paramIndex + 1}
    `
    
    // Add pagination parameters
    queryParams.push(limit, offset)
    
    const records = await executeQuery(dataQuery, queryParams)

    const results = (records as any[]).map((record) => {
      // Extract product name from product_data if not available in product_name column
      let finalProductName = record.product_name
      
      if (!finalProductName && record.product_data) {
        try {
          const data = typeof record.product_data === 'string' 
            ? JSON.parse(record.product_data)
            : record.product_data
          
          finalProductName = data.productname || data.name || data.title || 
                           data.product_name || data.productName || 
                           data.product_title || data.display_name || 
                           data.model || data.variant || null
        } catch (error) {
          console.error("Error parsing product_data:", error)
        }
      }

      // Use job brand_name as fallback if product brand_name is null
      const finalBrandName = record.brand_name || record.job_brand_name

      return {
        task_id: record.task_id,
        job_id: record.job_id,
        session_id: record.session_id,
        query_id: record.query_id,
        llm_model_name: record.llm_model_name,
        status: record.status,
        s3_output_path: record.s3_output_path,
        error_message: record.error_message,
        created_at: record.created_at,
        completed_at: record.completed_at,
        product_id: record.product_id,
        product_name: finalProductName || record.product_id?.toString() || "Unknown Product",
        query_text: record.query_text,
        query_type: record.query_type,
        brand_name: finalBrandName || "Unknown Brand",
        product_data: record.product_data
      } as LLMTaskResult
    })

    return createApiResponse({
      tasks: results,
      total_count: totalCount,
      current_page: page,
      total_pages: Math.ceil(totalCount / limit),
      has_next: page * limit < totalCount,
      has_previous: page > 1
    })
  } catch (error) {
    console.error("Results RDS API error:", error)
    return createErrorResponse("Failed to fetch RDS-based results")
  }
}

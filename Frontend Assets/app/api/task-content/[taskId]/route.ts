import type { NextRequest } from "next/server"
import { executeQuery, createApiResponse, createErrorResponse, getS3Object } from "../../../lib/api"

export const dynamic = 'force-dynamic'

export async function GET(request: NextRequest, { params }: { params: { taskId: string } }) {
  try {
    const { taskId } = params

    // Get task details from RDS
    const taskRecords = await executeQuery(`
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
      WHERE t.task_id = $1
    `, [taskId])

    if (taskRecords.length === 0) {
      return createErrorResponse("Task not found", 404)
    }

    const task = taskRecords[0]

    // Extract product name from product_data if not available
    let finalProductName = task.product_name
    if (!finalProductName && task.product_data) {
      try {
        const data = typeof task.product_data === 'string' 
          ? JSON.parse(task.product_data)
          : task.product_data
        
        finalProductName = data.productname || data.name || data.title || 
                         data.product_name || data.productName || 
                         data.product_title || data.display_name || 
                         data.model || data.variant || null
      } catch (error) {
        console.error("Error parsing product_data:", error)
      }
    }

    const result = {
      task_id: task.task_id,
      job_id: task.job_id,
      query_id: task.query_id,
      llm_model_name: task.llm_model_name,
      status: task.status,
      s3_output_path: task.s3_output_path,
      error_message: task.error_message,
      created_at: task.created_at,
      completed_at: task.completed_at,
      product_id: task.product_id,
      product_name: finalProductName || `Product ${task.product_id}`,
      query_text: task.query_text,
      query_type: task.query_type,
      brand_name: task.brand_name || task.job_brand_name || "Unknown Brand",
      s3_content: null as any
    }

    // Fetch S3 content if task is completed and has S3 path
    if (task.status === 'completed' && task.s3_output_path) {
      try {
        const s3Content = await getS3Object(task.s3_output_path)
        result.s3_content = s3Content
      } catch (error) {
        console.error(`Failed to fetch S3 content for task ${taskId}:`, error)
        // Don't fail the entire request if S3 fetch fails
        result.s3_content = { error: "Failed to load content from S3" }
      }
    }

    return createApiResponse(result)
  } catch (error) {
    console.error("Task content API error:", error)
    return createErrorResponse("Failed to fetch task content")
  }
}

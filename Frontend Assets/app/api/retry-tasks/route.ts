import type { NextRequest } from "next/server"
import { invokeLambda, LAMBDA_ARNS, createApiResponse, createErrorResponse } from "../../lib/api"

export const dynamic = 'force-dynamic'

interface RetryTaskRequest {
  job_id: string
  options: {
    retry: boolean
    session_id: string
    retry_tasks: Array<{
      task_id: string
      model: string
      query_id: number
      product_id: number
    }>
  }
}

export async function POST(request: NextRequest) {
  try {
    const body: RetryTaskRequest = await request.json()
    console.log("Retry tasks request body:", JSON.stringify(body, null, 2))
    
    const { job_id, options } = body

    if (!job_id) {
      return createErrorResponse("Job ID is required", 400)
    }

    if (!options.session_id) {
      return createErrorResponse("Session ID is required", 400)
    }

    if (!options.retry_tasks || options.retry_tasks.length === 0) {
      return createErrorResponse("At least one task to retry is required", 400)
    }

    // Validate retry tasks structure
    for (const task of options.retry_tasks) {
      if (!task.task_id || !task.model || !task.query_id || !task.product_id) {
        return createErrorResponse("Invalid task structure in retry_tasks", 400)
      }
    }

    // Build the lambda payload
    const lambdaPayload = {
      job_id,
      options: {
        retry: true,
        session_id: options.session_id,
        retry_tasks: options.retry_tasks
      }
    }
    
    console.log("Lambda payload:", JSON.stringify(lambdaPayload, null, 2))

    // Invoke LLM orchestrator lambda
    const result = await invokeLambda(LAMBDA_ARNS.LLM_ORCHESTRATOR, {
      body: lambdaPayload,
    })

    console.log("Lambda result:", result)

    if (result.statusCode === 202 || result.statusCode === 200) {
      return createApiResponse({
        message: "Task retry initiated successfully",
        total_tasks: options.retry_tasks.length,
        job_id,
        session_id: options.session_id
      })
    } else {
      throw new Error(result.body?.error || "Failed to retry tasks")
    }
  } catch (error) {
    console.error("Retry tasks API error:", error)
    return createErrorResponse(error instanceof Error ? error.message : "Failed to retry tasks")
  }
}

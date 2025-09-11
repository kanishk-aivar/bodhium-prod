import type { NextRequest } from "next/server"
import { createErrorResponse } from "../../lib/api"
import { BatchClient, SubmitJobCommand } from "@aws-sdk/client-batch"

// Initialize AWS Batch client
const batchClient = new BatchClient({
  region: process.env.AWS_REGION || "us-east-1",
})

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { job_id, product_ids } = body

    if (!job_id || !product_ids || product_ids.length === 0) {
      return createErrorResponse("Job ID and product IDs are required", 400)
    }

    // Generate a unique JOB_ID for AWS Batch
    const batchJobId = `query-gen-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`



    // Submit job to AWS Batch
    const submitJobCommand = new SubmitJobCommand({
      jobName: batchJobId,
      jobQueue: process.env.BATCH_QUEUE_QUERY_GENERATOR,
      jobDefinition: process.env.BATCH_DEFINITION_QUERY_GENERATOR,
      containerOverrides: {
        environment: [
          {
            name: "JOB_ID",
            value: job_id,
          },
          {
            name: "NUM_QUESTIONS",
            value: "25",
          },
          {
            name: "PRODUCTS_JSON",
            value: JSON.stringify(product_ids),
          },
          {
            name: "MAX_WORKERS",
            value: "8",
          },
          {
            name: "THREAD_TIMEOUT",
            value: "600",
          },
        ],
      },
    })

    console.log("Submitting AWS Batch job:", {
      jobName: batchJobId,
      jobQueue: process.env.BATCH_QUEUE_QUERY_GENERATOR,
      jobDefinition: process.env.BATCH_DEFINITION_QUERY_GENERATOR,
      environment: {
        JOB_ID: job_id,
        NUM_QUESTIONS: "25",
        PRODUCTS_JSON: JSON.stringify(product_ids),
        MAX_WORKERS: "8",
        THREAD_TIMEOUT: "600",
      }
    })

    try {
      const result = await batchClient.send(submitJobCommand)
      console.log("AWS Batch job submitted successfully:", result)
      
      // Return 202 Accepted with the batch job ID for tracking
      return new Response(
        JSON.stringify({
          success: true,
          message: "Query generation job submitted to AWS Batch",
          batch_job_id: batchJobId,
          job_id: job_id,
          aws_response: result,
        }),
        {
          status: 202,
          headers: {
            "Content-Type": "application/json",
          },
        }
      )
    } catch (batchError: any) {
      console.error("AWS Batch submission error:", batchError)
      console.error("Error details:", {
        name: batchError?.name,
        message: batchError?.message,
        code: batchError?.code,
        statusCode: batchError?.statusCode,
      })
      return createErrorResponse("Failed to submit job to AWS Batch", 500)
    }
  } catch (error) {
    console.error("Generate queries API error:", error)
    return createErrorResponse("Internal server error")
  }
}

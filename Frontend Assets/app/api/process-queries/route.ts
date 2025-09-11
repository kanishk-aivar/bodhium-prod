import type { NextRequest } from "next/server"
import { invokeLambda, LAMBDA_ARNS, executeQuery, createApiResponse, createErrorResponse } from "../../lib/api"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    console.log("Process queries request body:", JSON.stringify(body, null, 2))
    
    const { job_id, existing_query_ids, new_queries, selected_products } = body

    if (!job_id) {
      return createErrorResponse("Job ID is required", 400)
    }

    if ((!existing_query_ids || existing_query_ids.length === 0) && (!new_queries || new_queries.length === 0)) {
      return createErrorResponse("At least one existing query or new query is required", 400)
    }

    if (!selected_products || selected_products.length === 0) {
      return createErrorResponse("Selected products are required", 400)
    }

    // Convert string IDs to numbers for safety
    const existingQueryIdsNumbers = existing_query_ids ? existing_query_ids.map((id: any) => Number(id)) : []
    const selectedProductsNumbers = selected_products.map((id: any) => Number(id))
    
    console.log("Converted IDs - existing_query_ids:", existingQueryIdsNumbers, "selected_products:", selectedProductsNumbers)

    // Get existing query records if any
    let existingQueryRecords: any[] = []
    if (existingQueryIdsNumbers.length > 0) {
      existingQueryRecords = await executeQuery(
        `
        SELECT query_id, query_text, product_id
        FROM queries
        WHERE query_id = ANY($1)
      `,
        [existingQueryIdsNumbers],
      )
    }

    // Group existing queries by product_id
    const existingQueriesByProduct = new Map<string, Array<{query_id: number, query_text: string}>>()
    
    for (const record of existingQueryRecords) {
      if (!record.query_text) continue
      
      const productId = record.product_id?.toString() || "unknown"
      
      if (!existingQueriesByProduct.has(productId)) {
        existingQueriesByProduct.set(productId, [])
      }
      
      existingQueriesByProduct.get(productId)!.push({
        query_id: Number(record.query_id), // Ensure it's a number for Lambda
        query_text: record.query_text
      })
    }

    // Build the new format - include all selected products
    const selectedQueriesArray = selectedProductsNumbers.map((productId: number) => {
      const productIdStr = productId.toString()
      return {
        product_id: productIdStr,
        existing_queries: existingQueriesByProduct.get(productIdStr) || [],
        new_queries: new_queries || []
      }
    })

    // Build the lambda payload
    const lambdaPayload = {
      job_id,
      selected_queries: selectedQueriesArray,
      options: {
        models: ["chatgpt", "perplexity", "aimode", "aioverview"],
        async: true,
        priority: "high",
        timeout: 300
      },
    }
    
    console.log("Lambda payload:", JSON.stringify(lambdaPayload, null, 2))

    // Invoke LLM orchestrator lambda with new format
    const result = await invokeLambda(LAMBDA_ARNS.LLM_ORCHESTRATOR, {
      body: lambdaPayload,
    })

    console.log("Lambda result:", result)

    const totalExistingQueries = existingQueryRecords.length
    const totalNewQueries = new_queries ? new_queries.length : 0
    const totalQueries = totalExistingQueries + totalNewQueries

    if (result.statusCode === 202 || result.statusCode === 200) {
      return createApiResponse({
        message: "Query processing started",
        job_id,
        products_count: selectedQueriesArray.length,
        total_existing_queries: totalExistingQueries,
        total_new_queries: totalNewQueries,
        total_queries: totalQueries,
      })
    } else {
      console.error("Lambda invocation failed:", result)
      return createErrorResponse(result.body?.error || "Query processing failed", result.statusCode)
    }
  } catch (error) {
    console.error("Process queries API error:", error)
    return createErrorResponse("Internal server error")
  }
}

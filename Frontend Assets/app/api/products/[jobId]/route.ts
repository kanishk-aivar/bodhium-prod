import type { NextRequest } from "next/server"
import { executeQuery, createApiResponse, createErrorResponse } from "../../../lib/api"

export async function GET(request: NextRequest, { params }: { params: { jobId: string } }) {
  try {
    const { jobId } = params

    const records = await executeQuery(
      `
      SELECT p.product_id,
          p.product_hash,
          p.product_data,
          p.source_url,
          p.first_scraped_at,
          p.brand_name
      FROM products p
      WHERE p.job_id = $1
      ORDER BY p.first_scraped_at DESC;

    `,
      [jobId],
    )

    const products = records.map((record) => ({
      product_id: record.product_id,
      product_hash: record.product_hash,
      product_data: record.product_data,
      source_url: record.source_url,
      first_scraped_at: record.first_scraped_at,
      brand_name: record.brand_name,
    }))

    return createApiResponse(products)
  } catch (error) {
    console.error("Products API error:", error)
    return createErrorResponse("Failed to fetch products")
  }
}

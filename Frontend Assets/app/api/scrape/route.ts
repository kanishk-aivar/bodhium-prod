import type { NextRequest } from "next/server"
import { invokeLambda, LAMBDA_ARNS, createApiResponse, createErrorResponse } from "../../lib/api"

export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { url, brand_name } = body

    if (!url) {
      return createErrorResponse("URL is required", 400)
    }

    // Use provided brand_name or extract from URL hostname as fallback
    const finalBrandName = brand_name || new URL(url).hostname

    // Invoke the web scraper lambda
    const result = await invokeLambda(LAMBDA_ARNS.WEBSCRAPPER, {
      body: {
        url,
        brand_name: finalBrandName,
        max_urls: 1000,
      },
    })

    if (result.statusCode === 200) {
      return createApiResponse(JSON.parse(result.body))
    } else {
      return createErrorResponse(result.body?.error || "Scraping failed", result.statusCode)
    }
  } catch (error) {
    console.error("Scrape API error:", error)
    return createErrorResponse("Internal server error")
  }
}

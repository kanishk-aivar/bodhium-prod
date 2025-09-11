import { executeQuery, createApiResponse, createErrorResponse, getAllJobResults } from "../../lib/api"
import type { ProductResult, NewResultsResponse, S3WorkerResult } from "../../lib/types"

export const dynamic = 'force-dynamic'

interface ProductData {
  product_id: number
  product_data: any
  brand_name: string | null
}

interface JobData {
  job_id: string
  brand_name: string | null
}

export async function GET(request: Request) {
  try {
    // Extract job_id from URL parameters
    const { searchParams } = new URL(request.url)
    const filterJobId = searchParams.get('job_id')
    
    // Step 1: Query S3 for all results in the new structure
    const s3Results = await getAllJobResults()
    
    // Step 2: Filter by job_id if provided, then extract unique job_ids and product_ids from S3 data
    let filteredS3Results = s3Results
    if (filterJobId) {
      filteredS3Results = { [filterJobId]: s3Results[filterJobId] || {} }
    }
    
    const jobIds = Object.keys(filteredS3Results)
    const allProductIds = new Set<string>()
    
    for (const jobId of jobIds) {
      for (const productId of Object.keys(filteredS3Results[jobId])) {
        allProductIds.add(productId)
      }
    }
    
    // Filter product IDs to only include valid integers (bigint compatible)
    const allProductIdsArray = Array.from(allProductIds)
    const validProductIds = allProductIdsArray.filter(id => {
      // Check if the ID is a valid integer (not a UUID)
      return /^\d+$/.test(id)
    })
    
    // Log for debugging
    console.log(`Found ${allProductIdsArray.length} product IDs, ${validProductIds.length} are valid integers`)
    if (allProductIdsArray.length !== validProductIds.length) {
      const invalidIds = allProductIdsArray.filter(id => !/^\d+$/.test(id))
      console.log('Invalid product IDs (UUIDs):', invalidIds.slice(0, 5)) // Log first 5 for debugging
    }
    
    // Step 3: Get additional info from RDS
    const [jobsData, productsData] = await Promise.all([
      // Get job information for brand names
      executeQuery(`
        SELECT job_id, brand_name
        FROM scrapejobs
        WHERE job_id = ANY($1)
      `, [jobIds]),
      
      // Get product information (only for valid integer product IDs)
      validProductIds.length > 0 ? executeQuery(`
        SELECT product_id, product_data, brand_name
        FROM products
        WHERE product_id = ANY($1)
      `, [validProductIds.map(id => parseInt(id))]) : Promise.resolve([])
    ])
    
    // Create lookup maps
    const jobsMap = new Map<string, JobData>()
    jobsData.forEach((job: any) => {
      jobsMap.set(job.job_id, job)
    })
    
    const productsMap = new Map<string, ProductData>()
    productsData.forEach((product: any) => {
      productsMap.set(product.product_id.toString(), product)
    })
    
    // Step 4: Process S3 results and combine with RDS data
    const productResults: ProductResult[] = []
    
    for (const [jobId, jobProducts] of Object.entries(filteredS3Results)) {
      const jobInfo = jobsMap.get(jobId)
      
      for (const [productId, files] of Object.entries(jobProducts)) {
        const productInfo = productsMap.get(productId)
        
        // Group files by worker type
        const workerResults = new Map<string, S3WorkerResult[]>()
        
        for (const file of files) {
          let workerType = ""
          
          // Determine worker type from filename
          if (file.fileName.startsWith("aio_query_")) {
            workerType = "aio"
          } else if (file.fileName.startsWith("aim_query_")) {
            workerType = "aim"
          } else if (file.fileName.startsWith("perplexity_query_")) {
            workerType = "perplexity"
          } else if (file.fileName.startsWith("chatgpt_query_")) {
            workerType = "chatgpt"
          } else {
            continue // Skip unknown file types
          }
          
          if (!workerResults.has(workerType)) {
            workerResults.set(workerType, [])
          }
          
          // For ChatGPT markdown files, we need to parse differently
          let result: S3WorkerResult
          if (workerType === "chatgpt" && typeof file.content === "string") {
            // Parse markdown content to extract metadata
            const lines = file.content.split('\n')
            let query = ""
            let timestamp = ""
            let content = ""
            
            // Extract metadata from markdown
            for (let i = 0; i < lines.length; i++) {
              const line = lines[i]
              if (line.startsWith('**Query:**')) {
                query = line.replace('**Query:**', '').trim()
              } else if (line.startsWith('**Timestamp:**')) {
                timestamp = line.replace('**Timestamp:**', '').trim()
              } else if (line === '## Response Content') {
                // Extract content after this line until next section
                const contentStartIndex = i + 2
                const nextSectionIndex = lines.findIndex((l: string, idx: number) => idx > contentStartIndex && l.startsWith('---'))
                content = lines.slice(contentStartIndex, nextSectionIndex === -1 ? undefined : nextSectionIndex).join('\n').trim()
                break
              }
            }
            
            // Extract query_id from filename
            const queryIdMatch = file.fileName.match(/chatgpt_query_(\d+)\.md/)
            const queryId = queryIdMatch ? parseInt(queryIdMatch[1]) : 1
            
            result = {
              job_id: jobId,
              product_id: productId,
              query_id: queryId,
              query: query,
              timestamp: timestamp,
              model: "ChatGPT",
              content: content,
              formatted_markdown: file.content,
              status: "success"
            }
          } else {
            // For JSON files (AIO, AIM, Perplexity)
            result = file.content as S3WorkerResult
          }
          
          workerResults.get(workerType)!.push(result)
        }
        
        // Extract product name from product_data if available
        let productName = ""
        
        if (productInfo?.product_data) {
          try {
            const data = typeof productInfo.product_data === 'string' 
              ? JSON.parse(productInfo.product_data)
              : productInfo.product_data
            
            // Try multiple possible field names for product name - prioritize 'productname'
            productName = data.productname || data.name || data.title || data.product_name || 
                         data.productName || data.product_title || 
                         data.display_name || data.model || 
                         data.variant || ""
            
            // Log for debugging (only if name not found)
            if (!productName) {
              console.log(`Product ${productId} - no name found. Available keys:`, Object.keys(data))
            }
          } catch (error) {
            console.error("Error parsing product_data:", error, productInfo.product_data)
          }
        } else {
          console.log(`Product ${productId} has no product_data`)
        }
        
        productResults.push({
          product_id: productId,
          job_id: jobId,
          brand_name: jobInfo?.brand_name || productInfo?.brand_name || "",
          product_name: productName || `Product ${productId}`,
          workers: Array.from(workerResults.entries()).map(([workerType, results]) => ({
            worker_type: workerType,
            results: results.sort((a, b) => a.query_id - b.query_id) // Sort by query_id
          }))
        })
      }
    }
    
    const response: NewResultsResponse = {
      products: productResults,
      total_results: productResults.length
    }
    
    return createApiResponse(response)
  } catch (error) {
    console.error("Results V2 API error:", error)
    return createErrorResponse("Failed to fetch S3-based results")
  }
}

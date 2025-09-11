export interface ScrapeJob {
  job_id: string
  source_url: string | null
  status: string | null
  created_at: string
  updated_at: string
  brand_name: string | null
  progress?: {
    urls_collected: number
    urls_visited: number
    products_scraped: number
  }
}

export interface Product {
  product_id: number
  product_hash: string | null
  product_data: any
  source_url: string | null
  first_scraped_at: string
  brand_name: string | null
}

export interface Query {
  query_id: number
  product_id: number | null
  query_text: string | null
  query_type: string | null
  is_active: boolean | null
}

export interface LLMTask {
  task_id: string
  job_id: string | null
  query_id: number | null
  llm_model_name: string | null
  status: string | null
  s3_output_path: string | null
  error_message: string | null
  created_at: string
  completed_at: string | null
}

export interface JobSelectedProduct {
  job_id: string
  product_id: number
}

export interface JobWithTasks {
  job: ScrapeJob
  tasks: LLMTask[]
}

export type GroupedResults = Record<string, JobWithTasks>

// New S3-based result types
export interface S3FileContent {
  fileName: string
  content: any
  key: string
  lastModified?: Date
}

export interface S3WorkerResult {
  job_id: string
  product_id: string
  query_id: number
  query: string
  timestamp: string
  model: string
  content: string
  formatted_markdown?: string
  links?: Array<{ text: string; url: string }>
  related_questions?: string[]
  metadata?: any
  status: string
}

export interface ProductResult {
  product_id: string
  job_id: string
  brand_name?: string
  product_name?: string
  workers: Array<{
    worker_type: string // aio, aim, perplexity, chatgpt
    results: S3WorkerResult[]
  }>
}

export interface NewResultsResponse {
  products: ProductResult[]
  total_results: number
}

// RDS-based result types
export interface RDSTaskResult {
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
  query_text: string | null
  query_type: string | null
  brand_name: string | null
  product_data: any
}

export interface RDSResultsResponse {
  tasks: RDSTaskResult[]
  total_count: number
  current_page: number
  total_pages: number
  has_next: boolean
  has_previous: boolean
}

export interface TaskContentResponse extends RDSTaskResult {
  s3_content: any
}

// New Perplexity S3 response format
export interface PerplexityS3Response {
  job_id: string
  product_id: string
  query_id: number
  query: string
  timestamp: string
  model: string
  content: string
  citations: string[]
  related_questions: string[]
  status: string
}

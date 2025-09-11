"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Checkbox } from "@/components/ui/checkbox"
import { useToast } from "../hooks/use-toast"
import { TaskResponseContent } from "@/components/ui/task-response-content"
import type { RDSResultsResponse, RDSTaskResult, ScrapeJob } from "../lib/types"
import {
  Filter,
  Eye,
  Download,
  RefreshCw,
  Loader2,
  ChevronLeft,
  ChevronRight,
  RotateCcw,
  AlertTriangle,
  CheckCircle,
  XCircle,
  Clock,
  Search,
} from "lucide-react"

interface TaskRow {
  id: string
  jobId: string
  sessionId: string | null
  brand: string
  productName: string
  query: string
  status: string
  llmModel: string
  createdAt: string
  completedAt: string | null
  task: RDSTaskResult
}

export default function ResultsPage() {
  const [results, setResults] = useState<RDSResultsResponse>({ 
    tasks: [], 
    total_count: 0, 
    current_page: 1, 
    total_pages: 1, 
    has_next: false, 
    has_previous: false 
  })
  const [tableData, setTableData] = useState<TaskRow[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [brandFilter, setBrandFilter] = useState<string>("all")
  const [statusFilter, setStatusFilter] = useState<string>("all")
  const [modelFilter, setModelFilter] = useState<string>("all")
  const [sessionIdFilter, setSessionIdFilter] = useState<string>("all")
  const [currentPage, setCurrentPage] = useState(1)
  const [isLoading, setIsLoading] = useState(true)
  const [tableLoading, setTableLoading] = useState(false)
  const [selectedTask, setSelectedTask] = useState<RDSTaskResult | null>(null)

  // Filter options state - independent of current results
  const [filterOptions, setFilterOptions] = useState({
    brands: [] as string[],
    statuses: [] as string[],
    models: [] as string[]
  })
  const [filterOptionsLoading, setFilterOptionsLoading] = useState(false)

  // Job download state
  const [jobs, setJobs] = useState<ScrapeJob[]>([])
  const [jobsWithS3Data, setJobsWithS3Data] = useState<Set<string>>(new Set())
  const [downloadingJobs, setDownloadingJobs] = useState<Set<string>>(new Set())
  const [jobsLoading, setJobsLoading] = useState(false)
  const [selectedBrand, setSelectedBrand] = useState<string>("")
  const [selectedJobId, setSelectedJobId] = useState<string>("")

  // Session state
  const [sessions, setSessions] = useState<any[]>([])
  const [sessionsLoading, setSessionsLoading] = useState(false)

  // Retry state
  const [selectedFailedTasks, setSelectedFailedTasks] = useState<Set<string>>(new Set())
  const [isRetrying, setIsRetrying] = useState(false)

  const [showRetryConfirmation, setShowRetryConfirmation] = useState(false)
  const [retryConfirmationData, setRetryConfirmationData] = useState<{
    type: "bulk" | "single" | "filter"
    tasks: any[]
    description: string
  } | null>(null)
  const [singleTaskRetrying, setSingleTaskRetrying] = useState<Set<string>>(new Set())

  const ROWS_PER_PAGE = 30

  const { toast } = useToast()

  useEffect(() => {
    const initializePage = async () => {
      try {
        setIsLoading(true)
        console.log("Starting page initialization...")
        try {
          await Promise.all([
            fetchResults(1),
            fetchJobs(),
            fetchSessions(),
            fetchFilterOptions()
          ])
          console.log("Page initialization complete, setting isLoading to false")
          setIsLoading(false)
        } catch (error) {
          console.error("Error in Promise.all:", error)
          setIsLoading(false)
        }
      } catch (error) {
        console.error("Error initializing page:", error)
        setIsLoading(false)
      }
    }
    
    initializePage()
  }, [])



  // Fetch results when filters change (excluding search term)
  useEffect(() => {
    fetchResults(1)
  }, [brandFilter, statusFilter, modelFilter, sessionIdFilter])

  // Refresh filter options when brand filter changes (for sessions)
  useEffect(() => {
    if (brandFilter !== 'all') {
      fetchSessions()
    }
  }, [brandFilter])

  useEffect(() => {
    console.log("Results changed:", results)
    // Transform tasks data into table rows
    const rows: TaskRow[] = results.tasks.map((task) => ({
      id: task.task_id,
      jobId: task.job_id || "Unknown Job",
      sessionId: task.session_id || null,
      brand: task.brand_name || "Unknown Brand",
      productName: task.product_name || task.product_id?.toString() || "Unknown Product",
      query: task.query_text || "No query text",
      status: task.status || "unknown",
      llmModel: task.llm_model_name || "Unknown Model",
      createdAt: task.created_at,
      completedAt: task.completed_at,
      task: task,
    }))

    console.log("Setting table data:", rows.length, "rows")
    setTableData(rows)
  }, [results])

  const fetchResults = async (page: number = 1) => {
    try {
      setTableLoading(true)
      
      // Build query parameters
      const params = new URLSearchParams({
        page: page.toString(),
        limit: ROWS_PER_PAGE.toString()
      })
      
      if (searchTerm) params.append('search', searchTerm)
      if (brandFilter !== 'all') params.append('brand', brandFilter)
      if (statusFilter !== 'all') params.append('status', statusFilter)
      if (modelFilter !== 'all') params.append('model', modelFilter)
      if (sessionIdFilter !== 'all') params.append('sessionId', sessionIdFilter)
      
      const response = await fetch(`/api/results-rds?${params.toString()}`)
      const data = await response.json()
      console.log("Results fetched:", data)
      setResults(data)
      setCurrentPage(page)
      // Clear task selection when fetching new results
      setSelectedFailedTasks(new Set())
      setTableLoading(false)
      return data
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to fetch results",
        variant: "destructive",
      })
      setTableLoading(false)
      throw error
    }
  }

  const fetchFilterOptions = async () => {
    try {
      setFilterOptionsLoading(true)
      const response = await fetch('/api/results-rds/filter-options')
      const data = await response.json()
      setFilterOptions(data)
      return data
    } catch (error) {
      console.error("Error fetching filter options:", error)
      // Fallback to empty arrays if API fails
      setFilterOptions({ brands: [], statuses: [], models: [] })
      throw error
    } finally {
      setFilterOptionsLoading(false)
    }
  }

  const fetchJobs = async () => {
    try {
      setJobsLoading(true)
      const response = await fetch("/api/jobs")
      const data = await response.json()

      if (Array.isArray(data)) {
        setJobs(data)
        // Check S3 data for all jobs
        await checkJobsS3Data(data)
        return data
      } else {
        console.error("Jobs API returned non-array:", data)
        setJobs([])
        toast({
          title: "Warning",
          description: "Failed to load jobs for download",
          variant: "destructive",
        })
        return []
      }
    } catch (error) {
      console.error("Error fetching jobs:", error)
      setJobs([])
      toast({
        title: "Error",
        description: "Failed to fetch jobs for download",
        variant: "destructive",
      })
      throw error
    } finally {
      setJobsLoading(false)
    }
  }

  const fetchSessions = async () => {
    try {
      setSessionsLoading(true)
      const brandParam = brandFilter !== "all" ? `?brand=${encodeURIComponent(brandFilter)}` : ""
      const response = await fetch(`/api/sessions${brandParam}`)
      const data = await response.json()
      setSessions(data)
      return data
    } catch (error) {
      console.error("Error fetching sessions:", error)
      setSessions([])
      toast({
        title: "Error",
        description: "Failed to fetch sessions",
        variant: "destructive",
      })
      throw error
    } finally {
      setSessionsLoading(false)
    }
  }

  const initiateRetry = (type: "bulk" | "single" | "filter", tasks: any[], description: string) => {
    setRetryConfirmationData({ type, tasks, description })
    setShowRetryConfirmation(true)
  }

  const confirmRetry = async () => {
    if (!retryConfirmationData) return

    const { type, tasks } = retryConfirmationData

    if (type === "single") {
      await retrySingleTask(tasks[0])
    } else {
      await executeRetry(tasks)
    }

    setShowRetryConfirmation(false)
    setRetryConfirmationData(null)
  }

  const retrySingleTask = async (task: any) => {
    setSingleTaskRetrying((prev) => new Set(prev).add(task.id))

    try {
      // Map AI model names to supported model values
      let model = task.llmModel.toLowerCase().replace(/\s+/g, '')
      
      // Handle model name variations to match supported models
      if (model.includes('ChatGPT') || model.includes('gpt')) {
        model = 'chatgpt'
      } else if (model.includes('aio') || model.includes('GOOGLE_AI_OVERVIEW') || model.includes('google_ai_overview')) {
        model = 'aio'
      } else if (model.includes('aim') || model.includes('GOOGLE_AI_MODE') || model.includes('google_ai_mode')) {
        model = 'aim'
      } else if (model.includes('perplexity')) {
        model = 'perplexity'
      } else {
        console.log("Unknown model:", model)
        // exit the function
        throw new Error("Unknown model: " + model)
        // model = 'perplexity'
      }

      const response = await fetch("/api/retry-tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: task.task.job_id,
          options: {
            retry: true,
            session_id: task.task.session_id,
            retry_tasks: [
              {
                task_id: task.task.task_id,
                model: model,
                query_id: task.task.query_id || 0,
                product_id: task.task.product_id || 0
              },
            ],
          },
        }),
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: "Task retry initiated successfully!",
        })
        setTimeout(() => fetchResults(currentPage), 1000)
      } else {
        const errorData = await response.json()
        throw new Error(errorData.error || "Failed to retry task")
      }
    } catch (error) {
      console.error("Single task retry error:", error)
      toast({
        title: "Retry Failed",
        description: error instanceof Error ? error.message : "Failed to retry task",
        variant: "destructive",
      })
    } finally {
      setSingleTaskRetrying((prev) => {
        const newSet = new Set(prev)
        newSet.delete(task.id)
        return newSet
      })
    }
  }

  const executeRetry = async (tasksToRetry: any[]) => {
    setIsRetrying(true)
    try {
      const failedTasksData = tasksToRetry.map((row) => {
        // Map AI model names to supported model values
        let model = row.llmModel.toLowerCase().replace(/\s+/g, '')
        
        // Handle model name variations to match supported models
        if (model.includes('ChatGPT') || model.includes('gpt')) {
          model = 'chatgpt'
        } else if (model.includes('aio') || model.includes('GOOGLE_AI_OVERVIEW') || model.includes('google_ai_overview')) {
          model = 'aio'
        } else if (model.includes('aim') || model.includes('GOOGLE_AI_MODE') || model.includes('google_ai_mode')) {
          model = 'aim'
        } else if (model.includes('perplexity')) {
          model = 'perplexity'
        } else {
          console.log("Unknown model:", model)
          // exit the function
          throw new Error("Unknown model: " + model)
          // model = 'perplexity'
        }
        
        return {
          task_id: row.task.task_id,
          model: model,
          query_id: row.task.query_id || 0,
          product_id: row.task.product_id || 0
        }
      })

      const firstTask = tasksToRetry[0]
      const response = await fetch("/api/retry-tasks", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: firstTask.task.job_id,
          options: {
            retry: true,
            session_id: firstTask.task.session_id,
            retry_tasks: failedTasksData,
          },
        }),
      })

      if (response.ok) {
        const result = await response.json()
        toast({
          title: "Success",
          description: `Retry initiated for ${result.total_tasks} tasks!`,
        })

        setSelectedFailedTasks(new Set())
        setTimeout(() => fetchResults(currentPage), 1000)
      } else {
        const errorData = await response.json()
        throw new Error(errorData.error || "Failed to retry tasks")
      }
    } catch (error) {
      console.error("Retry error:", error)
      toast({
        title: "Retry Failed",
        description: error instanceof Error ? error.message : "Failed to retry tasks",
        variant: "destructive",
      })
    } finally {
      setIsRetrying(false)
    }
  }

  const retryFailedTasks = async () => {
    if (selectedFailedTasks.size === 0) {
      toast({
        title: "No Tasks Selected",
        description: "Please select at least one failed task to retry",
        variant: "destructive",
      })
      return
    }

    const selectedTasks = tableData.filter((row) => selectedFailedTasks.has(row.id))
    initiateRetry("bulk", selectedTasks, `${selectedTasks.length} selected failed tasks`)
  }

  const retryAllFailedByModel = (model: string) => {
    const failedTasks = tableData.filter((row) => row.status === "failed" && row.llmModel === model)
    if (failedTasks.length === 0) {
      toast({
        title: "No Failed Tasks",
        description: `No failed tasks found for model ${model}`,
        variant: "destructive",
      })
      return
    }
    initiateRetry("filter", failedTasks, `all failed tasks from model ${model} (${failedTasks.length} tasks)`)
  }

  const retryAllFailedByBrand = (brand: string) => {
    const failedTasks = tableData.filter((row) => row.status === "failed" && row.brand === brand)
    if (failedTasks.length === 0) {
      return
    }
    initiateRetry("filter", failedTasks, `all failed tasks from brand ${brand} (${failedTasks.length} tasks)`)
  }

  // Check which jobs have S3 data available
  const checkJobsS3Data = async (jobsList: ScrapeJob[]) => {
    const newJobsWithS3Data = new Set<string>()

    // Check each job in parallel
    const promises = jobsList.map(async (job) => {
      try {
        const response = await fetch(`/api/check-job-s3/${job.job_id}`)
        const data = await response.json()

        if (data.hasS3Data) {
          newJobsWithS3Data.add(job.job_id)
        }
      } catch (error) {
        console.error(`Failed to check S3 data for job ${job.job_id}:`, error)
      }
    })

    await Promise.all(promises)
    setJobsWithS3Data(newJobsWithS3Data)
  }

  // Handle job download
  const downloadJobData = async (jobId: string) => {
    try {
      setDownloadingJobs((prev) => new Set([...Array.from(prev), jobId]))

      const response = await fetch(`/api/download-job/${jobId}`)

      if (response.ok) {
        // Create blob from response
        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)

        // Create temporary download link
        const a = document.createElement("a")
        a.href = url
        a.download = `${jobId}_data.zip`
        document.body.appendChild(a)
        a.click()

        // Cleanup
        window.URL.revokeObjectURL(url)
        document.body.removeChild(a)

        toast({
          title: "Success",
          description: "Job data downloaded successfully",
        })
      } else {
        const errorData = await response.json()
        throw new Error(errorData.error || "Failed to download job data")
      }
    } catch (error) {
      toast({
        title: "Download Failed",
        description: error instanceof Error ? error.message : "Failed to download job data",
        variant: "destructive",
      })
    } finally {
      setDownloadingJobs((prev) => {
        const newSet = new Set(prev)
        newSet.delete(jobId)
        return newSet
      })
    }
  }

  // Helper functions for job dropdowns
  const getUniqueBrands = () => {
    return Array.from(new Set(jobs.map((job) => job.brand_name || "Unknown Brand"))).sort()
  }

  const getFilteredJobs = () => {
    if (!selectedBrand) return []
    return jobs.filter((job) => (job.brand_name || "Unknown Brand") === selectedBrand)
  }

  const getJobsWithS3DataForBrand = () => {
    return getFilteredJobs().filter((job) => jobsWithS3Data.has(job.job_id))
  }

  const getFailedTasksCount = () => {
    return tableData.filter((row) => row.status === "failed").length
  }

  const handleDownloadSelected = () => {
    if (selectedJobId) {
      downloadJobData(selectedJobId)
    }
  }

  // Helper functions for task selection
  const toggleTaskSelection = (taskId: string) => {
    setSelectedFailedTasks((prev) => {
      const newSet = new Set(prev)
      if (newSet.has(taskId)) {
        newSet.delete(taskId)
      } else {
        newSet.add(taskId)
      }
      return newSet
    })
  }

  const selectAllFailedTasks = () => {
    const failedTaskIds = tableData.filter((row) => row.status === "failed").map((row) => row.id)
    setSelectedFailedTasks(new Set(failedTaskIds))
  }

  const clearTaskSelection = () => {
    setSelectedFailedTasks(new Set())
  }

  // Pagination logic
  const totalPages = results.total_pages || 1

  // Get unique values for filters from filter options, with fallback to current data
  const uniqueBrands = filterOptions.brands.length > 0 ? filterOptions.brands : Array.from(new Set(tableData.map((row) => row.brand))).sort()
  const uniqueStatuses = filterOptions.statuses.length > 0 ? filterOptions.statuses : Array.from(new Set(tableData.map((row) => row.status))).sort()
  const uniqueModels = filterOptions.models.length > 0 ? filterOptions.models : Array.from(new Set(tableData.map((row) => row.llmModel))).sort()



  // Reset job selection when brand changes
  useEffect(() => {
    setSelectedJobId("")
  }, [selectedBrand])

  // Reset session_id filter when brand filter changes
  useEffect(() => {
    if (brandFilter !== "all") {
      setSessionIdFilter("all")
    }
  }, [brandFilter])



  const getStatusBadgeColor = (status: string) => {
    switch (status) {
      case "completed":
        return "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border border-emerald-500/20"
      case "failed":
        return "bg-red-500/15 text-red-700 dark:text-red-300 border border-red-500/20"
      case "processing":
        return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
      case "retrying":
        return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
      default:
        return "bg-amber-500/15 text-amber-700 dark:text-amber-300 border border-amber-500/20"
    }
  }

  const getModelBadgeColor = (model: string) => {
    const modelLower = model.toLowerCase()
    if (modelLower.includes("chatgpt")) {
      return "bg-orange-500/15 text-orange-700 dark:text-orange-300 border border-orange-500/20"
    } else if (modelLower.includes("perplexity")) {
      return "bg-purple-500/15 text-purple-700 dark:text-purple-300 border border-purple-500/20"
    } else if (modelLower.includes("aimode") || modelLower.includes("GOOGLE_AI_MODE") || modelLower.includes("google_ai_mode")) {
      return "bg-green-500/15 text-green-700 dark:text-green-300 border border-green-500/20"
    } else if (modelLower.includes("aioverview") || modelLower.includes("GOOGLE_AI_OVERVIEW") || modelLower.includes("google_ai_overview")) {
      return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
    }
    return "bg-gray-500/15 text-gray-700 dark:text-gray-300 border border-gray-500/20"
  }

  // Helper functions for job status icons and colors
  const getJobStatusIcon = (status: string) => {
    switch (status) {
      case "JOB_SUCCESS":
      case "completed":
        return <CheckCircle className="h-4 w-4 text-green-500" />
      case "JOB_FAILED":
      case "failed":
        return <XCircle className="h-4 w-4 text-red-500" />
      case "JOB_RUNNING":
      case "processing":
        return <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
      case "retrying":
        return <Loader2 className="h-4 w-4 text-blue-500 animate-spin" />
      default:
        return <Clock className="h-4 w-4 text-yellow-500" />
    }
  }

  const getJobStatusColor = (status: string) => {
    switch (status) {
      case "JOB_SUCCESS":
      case "completed":
        return "bg-green-100 text-green-800"
      case "JOB_FAILED":
      case "failed":
        return "bg-red-100 text-red-800"
      case "JOB_RUNNING":
      case "processing":
        return "bg-blue-100 text-blue-800"
      case "llm_generated":
        return "bg-purple-100 text-purple-800"
      case "retrying":
        return "bg-blue-100 text-blue-800"
      default:
        return "bg-yellow-100 text-yellow-800"
    }
  }

  const downloadTaskResult = async (task: RDSTaskResult) => {
    try {
      if (!task.s3_output_path) {
        toast({
          title: "Error",
          description: "No S3 output path available for this task",
          variant: "destructive",
        })
        return
      }

      // Fetch content directly from S3
      const response = await fetch(`/api/task-content/${task.task_id}`)
      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || "Failed to fetch task content")
      }

      if (!data.s3_content) {
        throw new Error("No S3 content available")
      }

      // Create filename-safe strings
      const safeBrand = (task.brand_name || "Unknown").replace(/[^a-z0-9]/gi, "_")
      const safeProduct = (task.product_name || "Unknown").replace(/[^a-z0-9]/gi, "_")
      const safeModel = (task.llm_model_name || "Unknown").replace(/[^a-z0-9]/gi, "_")
      const timestamp = new Date(task.created_at).toISOString().split("T")[0]

      // Determine file extension based on S3 path
      const fileExtension = task.s3_output_path.endsWith(".json")
        ? "json"
        : task.s3_output_path.endsWith(".md")
          ? "md"
          : "txt"

      // Create file content
      let content: string
      if (typeof data.s3_content === "string") {
        content = data.s3_content
      } else {
        content = JSON.stringify(data.s3_content, null, 2)
      }

      // Create and download the file
      const blob = new Blob([content], { type: "text/plain" })
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = url
      link.download = `${safeBrand}_${safeProduct}_${safeModel}_${timestamp}.${fileExtension}`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)

      toast({
        title: "Success",
        description: "S3 content downloaded successfully!",
      })
    } catch (error) {
      console.error("Download error:", error)
      toast({
        title: "Error",
        description: "Failed to download S3 content",
        variant: "destructive",
      })
    }
  }

  console.log("Current loading states - isLoading:", isLoading, "tableLoading:", tableLoading, "results:", results)
  
  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
          <p>Loading page...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen">
      <div className="container mx-auto px-6 md:px-8 py-10 max-w-7xl">
        <div className="mb-8 flex items-start justify-between gap-4">
          <div>
            <h1 className="text-3xl md:text-4xl font-semibold tracking-tight mb-2">
              AI Task Results
            </h1>
            <p className="text-muted-foreground">View and manage AI processing tasks from the database</p>
            <div className="mt-2 flex gap-4 text-sm text-muted-foreground">
              <span>{results.total_count} total tasks</span>
              <span>{results.tasks.length} current page results</span>              
              {totalPages > 1 && (
                <span>
                  Page {currentPage} of {totalPages}
                </span>
              )}
              {tableLoading && (
                <span className="flex items-center gap-1 text-blue-600 dark:text-blue-400">
                  <Loader2 className="h-3 w-3 animate-spin" />
                  Loading...
                </span>
              )}
            </div>
          </div>
        </div>

        {/* Job Downloads */}
        <Card className="mb-6 bg-white/60 dark:bg-white/5 backdrop-blur border border-white/60 dark:border-white/10">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Job Data Downloads</CardTitle>
              <div className="flex gap-2">
                <Button size="sm" variant="outline" onClick={fetchJobs} disabled={jobsLoading}>
                  {jobsLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            {jobsLoading ? (
              <div className="text-center py-4">
                <Loader2 className="h-6 w-6 animate-spin mx-auto mb-2" />
                <p className="text-sm text-muted-foreground">Loading jobs...</p>
              </div>
            ) : jobs.length === 0 ? (
              <p className="text-muted-foreground text-center py-4">No jobs found</p>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  {/* Brand Selection */}
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Brand</label>
                    <select
                      value={selectedBrand}
                      onChange={(e) => setSelectedBrand(e.target.value)}
                      className="w-full px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    >
                      <option value="">Select a brand</option>
                      {getUniqueBrands().map((brand) => (
                        <option key={brand} value={brand}>
                          {brand}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Job ID Selection */}
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Job ID</label>
                    <select
                      value={selectedJobId}
                      onChange={(e) => setSelectedJobId(e.target.value)}
                      disabled={!selectedBrand}
                      className="w-full px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <option value="">Select a job</option>
                      {getJobsWithS3DataForBrand().map((job) => (
                        <option key={job.job_id} value={job.job_id}>
                          {job.job_id} ({new Date(job.created_at).toLocaleDateString()}{" "}
                          {new Date(job.created_at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })})
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Download Button */}
                  <div className="space-y-2">
                    <label className="text-sm font-medium text-foreground">Action</label>
                    <Button
                      onClick={handleDownloadSelected}
                      disabled={!selectedJobId || downloadingJobs.has(selectedJobId)}
                      className="w-full h-10"
                    >
                      {downloadingJobs.has(selectedJobId) ? (
                        <>
                          <Loader2 className="h-4 w-4 animate-spin mr-2" />
                          Downloading...
                        </>
                      ) : (
                        <>
                          <Download className="h-4 w-4 mr-2" />
                          Download ZIP
                        </>
                      )}
                    </Button>
                  </div>
                </div>

                {/* Info Messages */}
                <div className="space-y-2">
                  {selectedBrand && getFilteredJobs().length === 0 && (
                    <div className="text-center py-2 px-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-lg">
                      <p className="text-sm text-blue-700 dark:text-blue-300">No jobs found for "{selectedBrand}"</p>
                    </div>
                  )}

                  {selectedBrand && getFilteredJobs().length > 0 && getJobsWithS3DataForBrand().length === 0 && (
                    <div className="text-center py-2 px-4 bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-700 rounded-lg">
                      <p className="text-sm text-yellow-700 dark:text-yellow-300">
                        No downloadable S3 data available for "{selectedBrand}" jobs
                      </p>
                    </div>
                  )}

                  {jobsWithS3Data.size === 0 && (
                    <div className="text-center py-2 px-4 bg-gray-50 dark:bg-gray-900/20 border border-gray-200 dark:border-gray-700 rounded-lg">
                      <p className="text-sm text-gray-700 dark:text-gray-300">
                        No jobs currently have downloadable S3 data available
                      </p>
                    </div>
                  )}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Search and Filters */}
        <Card className="mb-6 bg-white/60 dark:bg-white/5 backdrop-blur border border-white/60 dark:border-white/10">
          <CardContent className="p-4">
            <div className="flex flex-col gap-4">
              <div className="flex gap-4">
                <div className="flex-1 relative">
                  <Input
                    placeholder="Search by brand, product, query, or AI model..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === 'Enter') {
                        e.preventDefault()
                        fetchResults(1)
                      }
                    }}
                    disabled={tableLoading}
                    className={`pl-10 h-11 rounded-xl bg-white/60 dark:bg-white/10 border border-white/60 dark:border-white/10 disabled:opacity-50 disabled:cursor-not-allowed ${
                      searchTerm.trim() ? 'border-blue-500 dark:border-blue-400' : ''
                    }`}
                  />
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                </div>
                <Button
                  onClick={() => fetchResults(1)}
                  disabled={tableLoading || !searchTerm.trim()}
                  className="h-11 px-6"
                >
                  {tableLoading ? (
                    <Loader2 className="h-4 w-4 animate-spin mr-2" />
                  ) : (
                    <Search className="h-4 w-4 mr-2" />
                  )}
                  Search
                </Button>
                {searchTerm.trim() && (
                  <Button
                    variant="outline"
                    onClick={() => {
                      setSearchTerm("")
                      fetchResults(1)
                    }}
                    disabled={tableLoading}
                    className="h-11 px-4"
                  >
                    <XCircle className="h-4 w-4 mr-2" />
                    Clear
                  </Button>
                )}
              </div>

              <div className="flex gap-4 items-center">
                <Filter className="h-4 w-4 text-muted-foreground" />
                <div className="flex gap-2 items-center">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={fetchFilterOptions}
                    disabled={filterOptionsLoading}
                    className="h-8 px-2"
                  >
                    {filterOptionsLoading ? (
                      <Loader2 className="h-3 w-3 animate-spin" />
                    ) : (
                      <RefreshCw className="h-3 w-3" />
                    )}
                  </Button>
                </div>
                <div className="flex gap-4 flex-wrap">
                  <select
                    value={brandFilter}
                    onChange={(e) => setBrandFilter(e.target.value)}
                    disabled={tableLoading}
                    className="px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <option value="all">All Brands ({uniqueBrands.length})</option>
                    {filterOptionsLoading ? (
                      <option disabled>Loading brands...</option>
                    ) : uniqueBrands.length === 0 ? (
                      <option disabled>No brands found</option>
                    ) : (
                      uniqueBrands.map((brand) => (
                        <option key={brand} value={brand}>
                          {brand}
                        </option>
                      ))
                    )}
                  </select>

                  <select
                    value={sessionIdFilter}
                    onChange={(e) => setSessionIdFilter(e.target.value)}
                    disabled={brandFilter === "all" || tableLoading}
                    className="px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <option value="all">
                      {brandFilter === "all"
                        ? "Select Brand First"
                        : sessionsLoading
                          ? "Loading..."
                          : `All Sessions (${sessions.length})`}
                    </option>
                    {sessionsLoading ? (
                      <option disabled>Loading sessions...</option>
                    ) : sessions.length === 0 ? (
                      <option disabled>No sessions found</option>
                    ) : (
                      sessions.map((session) => (
                        <option key={session.session_id} value={session.session_id}>
                          {new Date(session.session_start).toLocaleString()} ({session.total_tasks} tasks,{" "}
                          {session.completed_tasks} completed, {session.failed_tasks} failed)
                        </option>
                      ))
                    )}
                  </select>

                  <select
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    disabled={tableLoading}
                    className="px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <option value="all">All Statuses ({uniqueStatuses.length})</option>
                    {filterOptionsLoading ? (
                      <option disabled>Loading statuses...</option>
                    ) : uniqueStatuses.length === 0 ? (
                      <option disabled>No statuses found</option>
                    ) : (
                      uniqueStatuses.map((status) => (
                        <option key={status} value={status}>
                          {status}
                        </option>
                      ))
                    )}
                  </select>

                  <select
                    value={modelFilter}
                    onChange={(e) => setModelFilter(e.target.value)}
                    disabled={tableLoading}
                    className="px-3 py-2 border rounded-md bg-white/60 dark:bg-white/10 border-white/60 dark:border-white/10 text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <option value="all">All AI Models ({uniqueModels.length})</option>
                    {filterOptionsLoading ? (
                      <option disabled>Loading models...</option>
                    ) : uniqueModels.length === 0 ? (
                      <option disabled>No models found</option>
                    ) : (
                      uniqueModels.map((model) => (
                        <option key={model} value={model}>
                          {model}
                        </option>
                      ))
                    )}
                  </select>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

                <Card className="bg-white/60 dark:bg-white/5 backdrop-blur border border-white/60 dark:border-white/10">
          <CardHeader>
            <div className="flex items-center justify-between">
              <CardTitle className="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
                <Eye className="h-6 w-6 text-blue-600" />
                Task Results
              </CardTitle>
              <div className="flex gap-2">
                <Button variant="outline" onClick={() => fetchResults(currentPage)} disabled={tableLoading}>
                  {tableLoading ? (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  ) : (
                    <RefreshCw className="mr-2 h-4 w-4" />
                  )}
                  {tableLoading ? 'Loading...' : 'Refresh'}
                </Button>
              </div>
            </div>
          </CardHeader>
          <CardContent className="space-y-6">
            {getFailedTasksCount() > 0 && (
              <div className="flex flex-wrap gap-2 items-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
                <div className="flex gap-2 items-center">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={selectAllFailedTasks}
                    disabled={selectedFailedTasks.size === getFailedTasksCount()}
                    className="border-blue-300 hover:bg-blue-100 bg-transparent"
                  >
                    Select All Failed
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={clearTaskSelection}
                    disabled={selectedFailedTasks.size === 0}
                    className="border-blue-300 hover:bg-blue-100 bg-transparent"
                  >
                    Clear Selection
                  </Button>
                  <Button
                    onClick={retryFailedTasks}
                    disabled={selectedFailedTasks.size === 0 || isRetrying}
                    className="bg-blue-600 hover:bg-blue-700"
                  >
                    {isRetrying ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Retrying...
                      </>
                    ) : (
                      <>
                        <RefreshCw className="mr-2 h-4 w-4" />
                        Retry Selected ({selectedFailedTasks.size})
                      </>
                    )}
                  </Button>
                </div>

                <div className="h-4 w-px bg-blue-300 dark:bg-blue-700" />

                {/* <div className="flex gap-2 items-center flex-wrap">
                  <span className="text-sm text-blue-700 dark:text-blue-300 font-medium">Quick Retry:</span>
                  {modelFilter !== "all" && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => retryAllFailedByModel(modelFilter)}
                      className="border-blue-300 hover:bg-blue-100 text-xs"
                    >
                      <RotateCcw className="mr-1 h-3 w-3" />
                      All Failed ({modelFilter})
                    </Button>
                  )}
                  {brandFilter !== "all" && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => retryAllFailedByBrand(brandFilter)}
                      className="border-blue-300 hover:bg-blue-100 text-xs"
                    >
                      <RotateCcw className="mr-1 h-3 w-3" />
                      All Failed ({brandFilter})
                    </Button>
                  )}
                </div> */}
              </div>
            )}

            <div className="rounded-md border border-white/60 dark:border-gray-700/60 overflow-hidden relative">
              {tableLoading && (
                <div className="absolute inset-0 bg-white/80 dark:bg-gray-900/80 backdrop-blur-sm z-10 flex items-center justify-center">
                  <div className="text-center">
                    <Loader2 className="h-8 w-8 animate-spin mx-auto mb-2" />
                    <p className="text-sm text-muted-foreground">Loading results...</p>
                  </div>
                </div>
              )}
              <Table>
                <TableHeader>
                  <TableRow className="bg-white/60 dark:bg-gray-800/60">
                    <TableHead className="w-12">
                      <Checkbox
                        checked={selectedFailedTasks.size > 0 && selectedFailedTasks.size === getFailedTasksCount()}
                        onCheckedChange={(checked) => {
                          if (checked) {
                            selectAllFailedTasks()
                          } else {
                            clearTaskSelection()
                          }
                        }}
                        className="border-blue-300"
                      />
                    </TableHead>
                    <TableHead>Brand</TableHead>
                    <TableHead>Product</TableHead>
                    <TableHead>Query</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>AI Model</TableHead>
                    <TableHead>Date</TableHead>
                    <TableHead>Actions</TableHead>
                    <TableHead>Download</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {tableData.map((row) => (
                    <TableRow
                      key={row.id}
                      className={`
                        ${
                          selectedFailedTasks.has(row.id)
                            ? "bg-blue-50 dark:bg-blue-900/20 border-l-4 border-l-blue-500"
                            : "hover:bg-white/60 dark:hover:bg-gray-800/60"
                        }
                      `}
                    >
                      <TableCell>
                        {row.status === "failed" && (
                          <Checkbox
                            checked={selectedFailedTasks.has(row.id)}
                            onCheckedChange={() => toggleTaskSelection(row.id)}
                            className="border-blue-300"
                          />
                        )}
                      </TableCell>
                      <TableCell className="font-medium">
                        <div className="max-w-[120px]">
                          <p className="truncate" title={row.brand}>
                            {row.brand}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="max-w-[200px]">
                          <p className="truncate" title={row.productName}>
                            {row.productName}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="max-w-[300px]">
                          <p className="truncate" title={row.query}>
                            {row.query}
                          </p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge className={getStatusBadgeColor(row.status)}>{row.status}</Badge>
                      </TableCell>
                      <TableCell>
                        <Badge className={getModelBadgeColor(row.llmModel)}>{row.llmModel}</Badge>
                      </TableCell>
                      <TableCell>
                        <div className="text-sm text-muted-foreground">
                          {new Date(row.createdAt).toLocaleDateString()}
                        </div>
                      </TableCell>
                      <TableCell>
                        <div className="flex gap-1">
                          <Button variant="outline" size="sm" onClick={() => setSelectedTask(row.task)}>
                            <Eye className="h-4 w-4 mr-1" />
                            View
                          </Button>
                          {row.status === "failed" && (
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => initiateRetry("single", [row], `task: ${row.brand} - ${row.productName}`)}
                              disabled={singleTaskRetrying.has(row.id)}
                              className="border-blue-300 hover:bg-blue-100 text-blue-700"
                            >
                              {singleTaskRetrying.has(row.id) ? (
                                <Loader2 className="h-3 w-3 animate-spin" />
                              ) : (
                                <RotateCcw className="h-3 w-3" />
                              )}
                            </Button>
                          )}
                        </div>
                      </TableCell>
                      <TableCell>
                        {row.task.s3_output_path ? (
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => downloadTaskResult(row.task)}
                            title="Download task result"
                          >
                            <Download className="h-4 w-4" />
                          </Button>
                        ) : (
                          <span className="text-muted-foreground text-xs">No data</span>
                        )}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-4">
                <div className="text-sm text-muted-foreground">
                  Showing {((currentPage - 1) * ROWS_PER_PAGE) + 1}-{Math.min(currentPage * ROWS_PER_PAGE, results.total_count)} of {results.total_count} results
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => fetchResults(currentPage - 1)}
                    disabled={currentPage === 1 || tableLoading}
                  >
                    {tableLoading ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <ChevronLeft className="h-4 w-4" />
                    )}
                    Previous
                  </Button>

                  <div className="flex items-center gap-1">
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      let pageNum
                      if (totalPages <= 5) {
                        pageNum = i + 1
                      } else if (currentPage <= 3) {
                        pageNum = i + 1
                      } else if (currentPage >= totalPages - 2) {
                        pageNum = totalPages - 4 + i
                      } else {
                        pageNum = currentPage - 2 + i
                      }

                      return (
                        <Button
                          key={pageNum}
                          variant={currentPage === pageNum ? "default" : "outline"}
                          size="sm"
                          onClick={() => fetchResults(pageNum)}
                          disabled={tableLoading}
                          className="w-8 h-8 p-0"
                        >
                          {pageNum}
                        </Button>
                      )
                    })}
                  </div>

                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => fetchResults(currentPage + 1)}
                    disabled={currentPage === totalPages || tableLoading}
                  >
                    Next
                    {tableLoading ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <ChevronRight className="h-4 w-4" />
                    )}
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Dialog open={showRetryConfirmation} onOpenChange={setShowRetryConfirmation}>
          <DialogContent className="max-w-md">
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-blue-600" />
                Confirm Retry
              </DialogTitle>
              <DialogDescription>
                Are you sure you want to retry {retryConfirmationData?.description}?
              </DialogDescription>
            </DialogHeader>

            {retryConfirmationData && (
              <div className="space-y-3">
                <div className="bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg">
                  <p className="text-sm font-medium text-blue-800 dark:text-blue-200">
                    Tasks to retry: {retryConfirmationData.tasks.length}
                  </p>
                  <div className="mt-2 space-y-1 max-h-32 overflow-y-auto">
                    {retryConfirmationData.tasks.slice(0, 5).map((task, index) => (
                      <div key={index} className="text-xs text-blue-700 dark:text-blue-300">
                        • {task.brand} - {task.productName}
                      </div>
                    ))}
                    {retryConfirmationData.tasks.length > 5 && (
                      <div className="text-xs text-blue-600 dark:text-blue-400">
                        ... and {retryConfirmationData.tasks.length - 5} more
                      </div>
                    )}
                  </div>
                </div>
              </div>
            )}

            <DialogFooter>
              <Button variant="outline" onClick={() => setShowRetryConfirmation(false)}>
                Cancel
              </Button>
              <Button onClick={confirmRetry} className="bg-blue-600 hover:bg-blue-700">
                <RefreshCw className="mr-2 h-4 w-4" />
                Retry Tasks
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        <Sheet open={selectedTask !== null} onOpenChange={(open) => !open && setSelectedTask(null)}>
          <SheetContent
            className="overflow-y-auto"
            side="right"
            resizable
            defaultWidth={900}
            minWidth={500}
            maxWidth={1000}
          >
            {selectedTask && (
              <>
                <SheetHeader>
                  <SheetTitle className="text-left">
                    {selectedTask.brand_name} - {selectedTask.product_name}
                  </SheetTitle>
                </SheetHeader>
                <div className="mt-6">
                  <TaskResponseContent task={selectedTask} onDownload={downloadTaskResult} />
                </div>
              </>
            )}
          </SheetContent>
        </Sheet>
      </div>
    </div>
  )
}

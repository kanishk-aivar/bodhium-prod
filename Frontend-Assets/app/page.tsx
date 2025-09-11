"use client"

import type React from "react"

import { useState, useEffect, useRef } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Loader2,
  CheckCircle,
  XCircle,
  Clock,
  RefreshCw,
  ChevronDown,
  ChevronUp,
  ArrowRight,
  Upload,
  FileText,
  Timer,
  Package,
} from "lucide-react"
import { useToast } from "./hooks/use-toast"
import type { ScrapeJob } from "./lib/types"

export default function HomePage() {
  const [jobs, setJobs] = useState<ScrapeJob[]>([])
  const [brandUrl, setBrandUrl] = useState("")
  const [brandName, setBrandName] = useState("")
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [isWorkflowsExpanded, setIsWorkflowsExpanded] = useState(true)
  const [currentTime, setCurrentTime] = useState(Date.now())

  // Ad hoc mode state
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [isUploading, setIsUploading] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const { toast } = useToast()
  const router = useRouter()

  useEffect(() => {
    fetchJobs()

    // Auto-refresh every 3 seconds
    const interval = setInterval(() => {
      fetchJobs()
    }, 3000)

    // Cleanup interval on component unmount
    return () => clearInterval(interval)
  }, [])

  // Update current time every second for stopwatch
  useEffect(() => {
    const timer = setInterval(() => {
      setCurrentTime(Date.now())
    }, 1000)

    return () => clearInterval(timer)
  }, [])

  const fetchJobs = async () => {
    try {
      const response = await fetch("/api/jobs")
      const data = await response.json()

      if (Array.isArray(data)) {
        setJobs(data)
      } else {
        console.error("Jobs API returned non-array:", data)
        setJobs([])
        toast({
          title: "Warning",
          description: "Failed to load job history",
          variant: "destructive",
        })
      }
    } catch (error) {
      console.error("Error fetching jobs:", error)
      setJobs([])
      toast({
        title: "Error",
        description: "Failed to fetch jobs",
        variant: "destructive",
      })
    }
  }

  const submitBrandUrl = async () => {
    if (!brandUrl.trim()) {
      toast({
        title: "Error",
        description: "Please enter a valid brand URL",
        variant: "destructive",
      })
      return
    }

    // Validate brand name - cannot be "Ad-hoc"
    if (brandName.trim().toLowerCase() === "ad-hoc") {
      toast({
        title: "Invalid Brand Name",
        description: "Brand name cannot be 'Ad-hoc'. This name is reserved for Ad Hoc mode.",
        variant: "destructive",
      })
      return
    }

    setIsSubmitting(true)
    try {
      const requestBody: { url: string; brand_name?: string } = { url: brandUrl }
      if (brandName.trim()) {
        requestBody.brand_name = brandName.trim()
      }

      const response = await fetch("/api/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(requestBody),
      })

      const data = await response.json()

      if (response.ok) {
        toast({
          title: "Success",
          description: `Job submitted successfully!`,
        })

        const newJob: ScrapeJob = {
          job_id: data.job_id,
          source_url: brandUrl,
          status: "SUBMITTED",
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          brand_name: brandName.trim() || new URL(brandUrl).hostname,
          product_count: 0,
        }

        setJobs((prev) => [newJob, ...prev])
        setBrandUrl("")
        setBrandName("")

        // Poll job status and navigate to products when complete
        pollJobStatus(data.job_id)
      } else {
        throw new Error(data.error || "Failed to submit job")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to submit job",
        variant: "destructive",
      })
    } finally {
      setIsSubmitting(false)
    }
  }

  const pollJobStatus = async (jobId: string) => {
    const pollInterval = setInterval(async () => {
      try {
        const response = await fetch(`/api/jobs/${jobId}`)
        const job = await response.json()

        setJobs((prev) => prev.map((j) => (j.job_id === jobId ? job : j)))

        if (job.status === "JOB_SUCCESS") {
          clearInterval(pollInterval)
          toast({
            title: "Success",
            description: "Scraping completed! Redirecting to products page...",
          })
          setTimeout(() => router.push("/products"), 1500)
        } else if (job.status === "JOB_FAILED") {
          clearInterval(pollInterval)
          toast({
            title: "Job Failed",
            description: "The scraping job failed. Please try again.",
            variant: "destructive",
          })
        }
      } catch (error) {
        console.error("Error polling job status:", error)
      }
    }, 5000)

    setTimeout(() => clearInterval(pollInterval), 600000)
  }

  // JSON Schema validation
  const validateJsonSchema = (jsonData: any): { valid: boolean; error?: string } => {
    try {
      // Check if it's an object
      if (typeof jsonData !== "object" || jsonData === null) {
        return { valid: false, error: "JSON must be an object" }
      }

      // Check required fields
      if (!jsonData.queries) {
        return { valid: false, error: "Missing required field: 'queries'" }
      }

      // Validate queries array
      if (!Array.isArray(jsonData.queries)) {
        return { valid: false, error: "'queries' must be an array" }
      }

      if (jsonData.queries.length === 0) {
        return { valid: false, error: "'queries' array cannot be empty" }
      }

      // Check each query is a string
      for (let i = 0; i < jsonData.queries.length; i++) {
        if (typeof jsonData.queries[i] !== "string") {
          return { valid: false, error: `Query at index ${i} must be a string` }
        }
        if (jsonData.queries[i].trim() === "") {
          return { valid: false, error: `Query at index ${i} cannot be empty` }
        }
      }

      return { valid: true }
    } catch (error) {
      return { valid: false, error: "Invalid JSON structure" }
    }
  }

  // Ad hoc mode functions
  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      // Validate file type
      if (!file.name.endsWith(".json")) {
        toast({
          title: "Invalid File",
          description: "Please select a JSON file",
          variant: "destructive",
        })
        return
      }

      // Validate JSON content
      try {
        const text = await file.text()
        const jsonData = JSON.parse(text)
        const validation = validateJsonSchema(jsonData)

        if (!validation.valid) {
          toast({
            title: "Invalid JSON Schema",
            description: validation.error,
            variant: "destructive",
          })
          return
        }

        setSelectedFile(file)
      } catch (error) {
        toast({
          title: "Invalid JSON",
          description: "Please select a valid JSON file",
          variant: "destructive",
        })
      }
    }
  }

  const handleFileDrop = async (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault()
    const file = event.dataTransfer.files[0]
    if (file) {
      // Validate file type
      if (!file.name.endsWith(".json")) {
        toast({
          title: "Invalid File",
          description: "Please select a JSON file",
          variant: "destructive",
        })
        return
      }

      // Validate JSON content
      try {
        const text = await file.text()
        const jsonData = JSON.parse(text)
        const validation = validateJsonSchema(jsonData)

        if (!validation.valid) {
          toast({
            title: "Invalid JSON Schema",
            description: validation.error,
            variant: "destructive",
          })
          return
        }

        setSelectedFile(file)
      } catch (error) {
        toast({
          title: "Invalid JSON",
          description: "Please select a valid JSON file",
          variant: "destructive",
        })
      }
    }
  }

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault()
  }

  const removeFile = () => {
    setSelectedFile(null)
    if (fileInputRef.current) {
      fileInputRef.current.value = ""
    }
  }

  const uploadAdHocFile = async () => {
    if (!selectedFile) {
      toast({
        title: "Error",
        description: "Please select a JSON file",
        variant: "destructive",
      })
      return
    }

    setIsUploading(true)
    try {
      const formData = new FormData()
      formData.append("file", selectedFile)

      const response = await fetch("/api/adhoc-upload", {
        method: "POST",
        body: formData,
      })

      const data = await response.json()

      if (response.ok) {
        toast({
          title: "Success",
          description: "File uploaded successfully! Processing started. Redirecting to results...",
        })

        // Reset form
        setSelectedFile(null)
        if (fileInputRef.current) {
          fileInputRef.current.value = ""
        }

        // Redirect to results page
        setTimeout(() => router.push("/results"), 1500)
      } else {
        throw new Error(data.error || "Failed to upload file")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to upload file",
        variant: "destructive",
      })
    } finally {
      setIsUploading(false)
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "JOB_SUCCESS":
      case "completed":
        return <CheckCircle className="h-5 w-5 text-green-500" />
      case "JOB_FAILED":
      case "failed":
        return <XCircle className="h-5 w-5 text-red-500" />
      case "JOB_RUNNING":
      case "processing":
        return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />
      default:
        return <Clock className="h-5 w-5 text-amber-500" />
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case "JOB_SUCCESS":
      case "completed":
        return "bg-green-50 text-green-700 border-green-200 dark:bg-green-900/20 dark:text-green-300 dark:border-green-800"
      case "JOB_FAILED":
      case "failed":
        return "bg-red-50 text-red-700 border-red-200 dark:bg-red-900/20 dark:text-red-300 dark:border-red-800"
      case "JOB_RUNNING":
      case "processing":
        return "bg-blue-50 text-blue-700 border-blue-200 dark:bg-blue-900/20 dark:text-blue-300 dark:border-blue-800"
      case "llm_generated":
        return "bg-purple-50 text-purple-700 border-purple-200 dark:bg-purple-900/20 dark:text-purple-300 dark:border-purple-800"
      default:
        return "bg-amber-50 text-amber-700 border-amber-200 dark:bg-amber-900/20 dark:text-amber-300 dark:border-amber-800"
    }
  }

  const formatElapsedTime = (startTime: string, endTime?: string) => {
    const start = new Date(startTime).getTime()
    const end = endTime ? new Date(endTime).getTime() : currentTime
    const elapsed = Math.floor((end - start) / 1000)
    const minutes = Math.floor(elapsed / 60)
    const seconds = elapsed % 60
    return `${minutes}m ${seconds}s`
  }

  return (
    <div className="container mx-auto px-6 md:px-8 py-12 max-w-5xl">
      <div className="mb-10">
        <h1 className="text-4xl md:text-5xl font-semibold tracking-tight mb-3 bg-gradient-to-r from-[hsl(var(--foreground))] to-[hsl(var(--accent))] bg-clip-text text-transparent">
          Bodhium Measurement Tool
        </h1>
        <p className="text-muted-foreground text-base">
          Start your brand analysis by submitting a URL or upload queries directly in Ad Hoc mode
        </p>
      </div>

      {/* Mode Selection Tabs */}
      <Card className="mb-8 bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10">
        <CardContent className="p-6">
          <Tabs defaultValue="brand-url" className="w-full">
            <TabsList className="w-full mb-6">
              <TabsTrigger value="brand-url" className="flex-1">
                Submit Brand URL
              </TabsTrigger>
              <TabsTrigger value="adhoc" className="flex-1">
                Ad Hoc Mode
              </TabsTrigger>
            </TabsList>

            {/* Brand URL Tab */}
            <TabsContent value="brand-url" className="space-y-4">
              <div className="space-y-3">
                <Input
                  placeholder="Enter brand URL (e.g., https://store.example.com)"
                  value={brandUrl}
                  onChange={(e) => setBrandUrl(e.target.value)}
                  disabled={isSubmitting}
                  className="h-12 rounded-xl bg-white/60 dark:bg-white/10 backdrop-blur-md border border-white/60 dark:border-white/10 placeholder:text-muted-foreground/70 focus-visible:ring-2 focus-visible:ring-[hsl(var(--accent))]/50 focus-visible:border-[hsl(var(--accent))]/50"
                />
                <div className="relative">
                  <Input
                    placeholder="Brand name (optional)"
                    value={brandName}
                    onChange={(e) => setBrandName(e.target.value)}
                    disabled={isSubmitting}
                    className="h-12 rounded-xl bg-white/60 dark:bg-white/10 backdrop-blur-md border border-white/60 dark:border-white/10 placeholder:text-muted-foreground/70 focus-visible:ring-2 focus-visible:ring-[hsl(var(--accent))]/50 focus-visible:border-[hsl(var(--accent))]/50"
                  />
                  <p className="text-xs text-muted-foreground mt-1.5 pl-1">
                    Leave empty to auto-detect from URL hostname
                  </p>
                </div>
                <Button
                  onClick={submitBrandUrl}
                  disabled={isSubmitting}
                  className="px-6 h-11 min-w-[120px] rounded-xl shadow-md"
                >
                  {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : "Submit"}
                </Button>
              </div>
            </TabsContent>

            {/* Ad Hoc Mode Tab */}
            <TabsContent value="adhoc" className="space-y-4">
              <div className="space-y-4">
                {/* File Upload Area */}
                <div
                  className="border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-xl p-8 text-center hover:border-gray-400 dark:hover:border-gray-500 transition-colors cursor-pointer"
                  onDrop={handleFileDrop}
                  onDragOver={handleDragOver}
                  onClick={() => fileInputRef.current?.click()}
                >
                  {selectedFile ? (
                    <div className="space-y-3">
                      <FileText className="h-12 w-12 mx-auto text-green-500" />
                      <div>
                        <p className="font-medium text-green-700 dark:text-green-400">{selectedFile.name}</p>
                        <p className="text-sm text-muted-foreground">{(selectedFile.size / 1024).toFixed(2)} KB</p>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation()
                          removeFile()
                        }}
                      >
                        Remove
                      </Button>
                    </div>
                  ) : (
                    <div className="space-y-3">
                      <Upload className="h-12 w-12 mx-auto text-gray-400" />
                      <div>
                        <p className="text-lg font-medium">Upload JSON File</p>
                        <p className="text-sm text-muted-foreground">
                          Drag and drop your JSON file here, or click to browse
                        </p>
                      </div>
                    </div>
                  )}
                </div>

                <input ref={fileInputRef} type="file" accept=".json" onChange={handleFileSelect} className="hidden" />

                <div className="text-xs text-muted-foreground space-y-1 pl-1">
                  <p>• Only JSON files are accepted</p>
                  <p>• File must match the required schema format</p>
                  <p>• File will be uploaded to S3 and processed automatically</p>
                  <p>• Processing results will appear on the Results page</p>
                  <details className="mt-3">
                    <summary className="cursor-pointer text-muted-foreground hover:text-foreground font-medium">
                      Show JSON Schema Example
                    </summary>
                    <div className="mt-2 p-3 bg-muted/50 rounded-lg">
                      <pre className="text-xs overflow-x-auto">
                        {`{
  "queries": [
    "What are the best laptops under $1000 in USA?",
    "What are the best hair bands for women in USA?",
    "What are the best hair ties for women in USA?"
  ]
}`}
                      </pre>
                      <div className="mt-2 text-xs space-y-1">
                        <p>
                          <strong>Required fields:</strong>
                        </p>
                        <p>• queries: Array of non-empty strings</p>
                      </div>
                    </div>
                  </details>
                </div>

                <Button
                  onClick={uploadAdHocFile}
                  disabled={!selectedFile || isUploading}
                  className="px-6 h-11 min-w-[140px] rounded-xl shadow-md"
                >
                  {isUploading ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin mr-2" />
                      Uploading...
                    </>
                  ) : (
                    <>
                      <Upload className="h-4 w-4 mr-2" />
                      Upload & Process
                    </>
                  )}
                </Button>
              </div>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      <Card className="bg-transparent backdrop-blur supports-[backdrop-filter]:bg-transparent border border-white/60 shadow-lg dark:bg-transparent dark:border-white/10">
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <CardTitle className="text-xl font-semibold">Products Crawl History</CardTitle>
              <div className="flex items-center gap-2 px-3 py-1 bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-300 rounded-full border border-green-200 dark:border-green-800">
                <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse" />
                <span className="text-xs font-medium">Auto-refresh</span>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Button size="sm" variant="outline" onClick={fetchJobs} className="h-8 w-8 p-0 bg-transparent">
                <RefreshCw className="h-4 w-4" />
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setIsWorkflowsExpanded(!isWorkflowsExpanded)}
                className="h-8 w-8 p-0"
              >
                {isWorkflowsExpanded ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
              </Button>
            </div>
          </div>
        </CardHeader>
        <CardContent className="pt-0">
          {isWorkflowsExpanded && (
            <div className="space-y-4 max-h-[600px] overflow-y-auto pr-2">
              {Array.isArray(jobs) && jobs.length > 0 ? (
                jobs
                  .filter((job) => job.brand_name !== "Ad-hoc")
                  .map((job) => (
                  <div
                    key={job.job_id}
                    className="group relative p-6 bg-white/70 dark:bg-white/5 backdrop-blur-md rounded-2xl border border-white/60 dark:border-white/10 shadow-sm hover:shadow-lg hover:bg-white/80 dark:hover:bg-white/10 transition-all duration-200"
                  >
                    {/* Header Section */}
                    <div className="flex items-start justify-between mb-4">
                      <div className="flex items-center gap-4">
                        <div className="flex items-center justify-center w-12 h-12 bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/30 dark:to-blue-800/30 rounded-xl border border-blue-200 dark:border-blue-800">
                          {getStatusIcon(job.status ?? "")}
                        </div>
                        <div className="space-y-1">
                          <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
                            {job.brand_name || "Unknown Brand"}
                          </h3>
                          <div className="flex items-center gap-2 text-sm text-muted-foreground">
                            <Timer className="h-3 w-3" />
                            <span>Started {new Date(job.created_at).toLocaleString()}</span>
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-3">
                        <Badge className={`px-3 py-1 text-sm font-medium border ${getStatusColor(job.status ?? "")}`}>
                          {job.status?.replace("JOB_", "").toLowerCase()}
                        </Badge>
                        {job.status === "JOB_SUCCESS" && (
                          <Button
                            size="sm"
                            onClick={() => router.push("/products")}
                            className="h-8 px-3 text-sm bg-blue-500 hover:bg-blue-600 text-white shadow-sm"
                          >
                            View Products <ArrowRight className="ml-1 h-3 w-3" />
                          </Button>
                        )}
                        {job.status === "llm_generated" && (
                          <Button
                            size="sm"
                            onClick={() => router.push("/results")}
                            className="h-8 px-3 text-sm bg-purple-500 hover:bg-purple-600 text-white shadow-sm"
                          >
                            View Results <ArrowRight className="ml-1 h-3 w-3" />
                          </Button>
                        )}
                      </div>
                    </div>

                    {/* Job Statistics */}
                    <div className="space-y-4">
                      {/* Stats Grid */}
                      <div className="grid grid-cols-2 gap-4">
                        <div className="text-center p-4 bg-gradient-to-br from-emerald-50 to-green-50 dark:from-emerald-900/20 dark:to-green-900/20 rounded-xl border border-emerald-200 dark:border-emerald-800">
                          <div className="flex items-center justify-center gap-2 mb-2">
                            <Package className="h-4 w-4 text-emerald-600 dark:text-emerald-400" />
                            <span className="text-xs font-medium text-emerald-700 dark:text-emerald-300 uppercase tracking-wide">
                              Products Found
                            </span>
                          </div>
                          <div className="text-2xl font-bold text-emerald-600 dark:text-emerald-400">
                            {job.product_count || 0}
                          </div>
                        </div>

                        <div className="text-center p-4 bg-gradient-to-br from-blue-50 to-blue-100 dark:from-blue-900/20 dark:to-blue-800/20 rounded-xl border border-blue-200 dark:border-blue-800">
                          <div className="flex items-center justify-center gap-2 mb-2">
                            <Timer className="h-4 w-4 text-blue-600 dark:text-blue-400" />
                            <span className="text-xs font-medium text-blue-700 dark:text-blue-300 uppercase tracking-wide">
                              {job.status === "JOB_SUCCESS" || job.status === "llm_generated" ? "Total Time" : "Elapsed Time"}
                            </span>
                          </div>
                          <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                            {job.status === "JOB_SUCCESS" || job.status === "llm_generated" 
                              ? formatElapsedTime(job.created_at, job.updated_at)
                              : formatElapsedTime(job.created_at)
                            }
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div className="text-center py-12">
                  <div className="w-16 h-16 mx-auto mb-4 bg-gray-100 dark:bg-gray-800 rounded-2xl flex items-center justify-center">
                    <FileText className="w-8 h-8 text-gray-400" />
                  </div>
                  <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">No crawls found</h3>
                  <p className="text-muted-foreground">
                    Start by submitting a brand URL or uploading a JSON file above
                  </p>
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
"use client"

import { useState, useEffect } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"


import { Loader2, RefreshCw, Download, FileSpreadsheet, Clock, Database } from "lucide-react"
import { useToast } from "../hooks/use-toast"

interface AdHocJob {
  job_id: string
  created_at: string
  status: string
  brand_name: string
  total_tasks: number
  completed_tasks: number
  // Enhanced fields from backend response
  csv_details?: {
    generated: boolean
    s3_location?: string
    filename?: string
  }
  download?: {
    presigned_url?: string
    expires_in?: string
    direct_download: boolean
  }
}

interface AdHocJobsResponse {
  jobs: AdHocJob[]
  total_count: number
}

interface S3CsvFile {
  job_id: string
  filename: string
  s3_key: string
  last_modified: string
  size: number
}

interface DownloadResponse {
  success: boolean
  download_type?: 'presigned_url'
  presigned_url?: string
  expires_in?: string
  filename?: string
  metadata?: {
    size_info: string
    generated_at: string
  }
}

export default function AdHocJobsPage() {
  const [jobs, setJobs] = useState<AdHocJob[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [downloadingJobs, setDownloadingJobs] = useState<Set<string>>(new Set())
  const [generatingJobs, setGeneratingJobs] = useState<Set<string>>(new Set())
  const [s3CsvFiles, setS3CsvFiles] = useState<Map<string, S3CsvFile>>(new Map())

  const { toast } = useToast()

  const fetchS3CsvFiles = async () => {
    try {
      const response = await fetch("/api/adhoc-csv")
      const data = await response.json()
      
      if (response.ok && data.success) {
        const csvFilesMap = new Map<string, S3CsvFile>()
        data.csv_files.forEach((file: S3CsvFile) => {
          csvFilesMap.set(file.job_id, file)
        })
        setS3CsvFiles(csvFilesMap)
      } else {
        console.error('Failed to fetch S3 CSV files:', data.error)
      }
    } catch (error) {
      console.error('Error fetching S3 CSV files:', error)
    }
  }

  useEffect(() => {
    fetchAdHocJobs()
    fetchS3CsvFiles()
  }, [])

  const fetchAdHocJobs = async () => {
    try {
      setIsLoading(true)
      const response = await fetch("/api/adhoc-jobs")
      const data: AdHocJobsResponse = await response.json()
      
      if (response.ok) {
        setJobs(data.jobs)
      } else {
        throw new Error("Failed to fetch Ad-hoc jobs")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to fetch Ad-hoc jobs",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const refreshData = async () => {
    await Promise.all([fetchAdHocJobs(), fetchS3CsvFiles()])
  }

  const downloadFromS3 = async (s3File: S3CsvFile) => {
    setDownloadingJobs(prev => new Set(prev).add(s3File.job_id))
    
    try {
      const response = await fetch("/api/adhoc-csv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: s3File.job_id, action: "download" }),
      })

      if (!response.ok) {
        throw new Error("Failed to get download URL")
      }

      const downloadData: DownloadResponse = await response.json()

      // Handle presigned URL response - redirect immediately
      if (downloadData.download_type === 'presigned_url' && downloadData.presigned_url) {
        // Redirect to S3 presigned URL immediately
        window.open(downloadData.presigned_url, '_blank')
        
        toast({
          title: "Success",
          description: "Redirecting to download...",
        })
      } else {
        throw new Error("Invalid response format")
      }
    } catch (error) {
      console.error("CSV download error:", error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to get download URL",
        variant: "destructive",
      })
    } finally {
      setDownloadingJobs(prev => {
        const newSet = new Set(prev)
        newSet.delete(s3File.job_id)
        return newSet
      })
    }
  }

  const generateAndDownloadCSV = async (jobId: string) => {
    setGeneratingJobs(prev => new Set(prev).add(jobId))
    
    try {
      // Submit CSV generation as an event to Lambda
      const statusResponse = await fetch("/api/adhoc-csv", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ job_id: jobId }),
      })

      if (!statusResponse.ok) {
        throw new Error("Failed to submit CSV generation")
      }

      const statusData = await statusResponse.json()
      
      if (!statusData.success) {
        throw new Error("CSV generation submission failed")
      }

      // Show success notification for event submission
      toast({
        title: "Success",
        description: "CSV Report Generation Submitted Successfully",
      })

      // Refresh S3 files to check for any existing CSV
      await fetchS3CsvFiles()

      // Check if CSV is already available for immediate download
      const s3File = s3CsvFiles.get(jobId)
      if (s3File) {
        toast({
          title: "CSV Available",
          description: "CSV is ready for download!",
        })
        await downloadFromS3(s3File)
      } else {
        toast({
          title: "Processing",
          description: "CSV generation is in progress. Check back later or refresh to see when it's ready.",
        })
      }
    } catch (error) {
      console.error("CSV generation error:", error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to submit CSV generation",
        variant: "destructive",
      })
    } finally {
      setGeneratingJobs(prev => {
        const newSet = new Set(prev)
        newSet.delete(jobId)
        return newSet
      })
    }
  }



  const getCompletionRate = (completed: number, total: number) => {
    if (total === 0) return 0
    return Math.round((completed / total) * 100)
  }



  return isLoading ? (
    <div className="min-h-screen flex items-center justify-center">
      <div className="text-center">
        <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
        <p className="text-sm text-muted-foreground">Loading Ad-hoc jobs...</p>
      </div>
    </div>
  ) : (
    <main className="container mx-auto px-6 md:px-8 py-10 max-w-7xl">
      <header className="mb-8 flex items-start justify-between gap-4">
        <div className="space-y-2">
          <h1 className="text-3xl md:text-4xl font-semibold tracking-tight text-balance">
            Ad-hoc Jobs
          </h1>
          <p className="text-muted-foreground text-pretty">
            Manage and download CSV results for Ad-hoc processing jobs
          </p>
          <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
            <span className="rounded-full bg-muted px-2.5 py-1 text-muted-foreground">
              {jobs.length} total jobs
            </span>
          </div>
        </div>
        <div className="flex gap-2">
          <Button
            variant="outline"
            onClick={refreshData}
            disabled={isLoading}
            aria-label="Refresh jobs list and CSV files"
          >
            <RefreshCw className={`mr-2 h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
      </header>

      {/* Jobs Grid */}
      {jobs.length === 0 ? (
        <Card className="border">
          <CardContent className="p-12">
            <div className="text-center">
              <FileSpreadsheet className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
              <h3 className="text-lg font-medium mb-2">No Ad-hoc Jobs Found</h3>
              <p className="text-muted-foreground">
                Ad-hoc jobs will appear here after processing JSON uploads.
              </p>
            </div>
          </CardContent>
        </Card>
      ) : (
        <section aria-label="Jobs" className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-6 mb-8">
          {jobs.map((job) => {
            const completionRate = getCompletionRate(job.completed_tasks, job.total_tasks)
            const isProcessing = generatingJobs.has(job.job_id)
            const isDownloading = downloadingJobs.has(job.job_id)
            const s3File = s3CsvFiles.get(job.job_id)
  
            return (
              <Card key={job.job_id} className="border">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <CardTitle className="text-sm font-mono truncate" title={job.job_id}>
                        {job.job_id}
                      </CardTitle>
                      <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                        <span className="truncate" title={job.brand_name}>
                          {job.brand_name}
                        </span>
                        <span aria-hidden="true">•</span>
                        <time dateTime={new Date(job.created_at).toISOString()}>
                          {new Date(job.created_at).toLocaleDateString()}
                        </time>
                      </div>
                    </div>
                  </div>
                </CardHeader>
  
                <CardContent className="pt-0">
                  <div className="space-y-4">
                    {/* Progress */}
                    <div aria-label="Job progress">
                      <div className="flex justify-between text-sm mb-2">
                        <span>Progress</span>
                        <span className="text-muted-foreground">
                          {job.completed_tasks}/{job.total_tasks} tasks
                        </span>
                      </div>
                      <div className="flex items-center gap-2" title={`${completionRate}% complete`}>
                        <div
                          className="flex-1 bg-muted rounded-full h-2"
                          role="progressbar"
                          aria-valuenow={completionRate}
                          aria-valuemin={0}
                          aria-valuemax={100}
                          aria-label={`Completion ${completionRate} percent`}
                        >
                          <div
                            className="bg-green-500 h-2 rounded-full transition-all duration-300"
                            style={{ width: `${completionRate}%` }}
                          />
                        </div>
                        <span className="text-xs text-muted-foreground min-w-[35px] text-right">
                          {completionRate}%
                        </span>
                      </div>
                    </div>

                    {/* CSV Details */}
                    {job.csv_details && (
                      <div className="space-y-3 p-3 bg-green-50/50 rounded-lg border border-green-200/50">
                        <h4 className="text-sm font-medium text-green-900">CSV Details</h4>
                        <div className="space-y-2 text-xs">
                          <div className="flex items-center justify-between">
                            <span className="text-green-700 font-medium">Status:</span>
                            <span className={`px-2 py-1 rounded-full text-xs ${
                              job.csv_details.generated 
                                ? 'bg-green-100 text-green-800' 
                                : 'bg-yellow-100 text-yellow-800'
                            }`}>
                              {job.csv_details.generated ? 'Generated' : 'Pending'}
                            </span>
                          </div>
                        </div>
                      </div>
                    )}

                    {/* Download Information */}
                    {job.download && (
                      <div className="space-y-3 p-3 bg-purple-50/50 rounded-lg border border-purple-200/50">
                        <h4 className="text-sm font-medium text-purple-900">Download</h4>
                        <div className="space-y-2 text-xs">
                          <div className="flex items-center justify-between">
                            <span className="text-purple-700 font-medium">Type:</span>
                            <span className="text-purple-600">
                              {job.download.direct_download ? 'Direct Download' : 'Presigned URL'}
                            </span>
                          </div>
                          {job.download.expires_in && (
                            <div className="flex items-center justify-between">
                              <span className="text-purple-700 font-medium">Expires:</span>
                              <span className="text-purple-600">{job.download.expires_in}</span>
                            </div>
                          )}
                        </div>
                      </div>
                    )}


  
                    {/* S3 File Status */}
                    {s3File && (
                      <div className="space-y-3 p-3 bg-blue-50/50 rounded-lg border border-blue-200/50">
                        <h4 className="text-sm font-medium text-blue-900">Available CSV</h4>
                        <div className="space-y-2 text-xs">
                          <div className="flex items-center justify-between">
                            <span className="text-blue-700 font-medium">Status:</span>
                            <span className="px-2 py-1 bg-blue-100 text-blue-800 rounded-full text-xs">
                              Ready
                            </span>
                          </div>

                          <div className="flex items-center gap-3 text-xs text-blue-600">
                            <div className="flex items-center gap-1">
                              <Clock className="h-3 w-3" />
                              <span>
                                Generated {new Date(s3File.last_modified).toLocaleString()}
                              </span>
                            </div>
                            <span aria-hidden="true">•</span>
                            <span>
                              Size: {(s3File.size / 1024).toFixed(1)} KB
                            </span>
                          </div>
                        </div>
                      </div>
                    )}
  
                    {/* Actions */}
                    <div className="flex gap-2">
                      
  
                      {s3File && (
                        <Button
                          size="sm"
                          onClick={() => downloadFromS3(s3File)}
                          disabled={isDownloading}
                          aria-busy={isDownloading}
                          aria-label={`Download CSV for ${job.job_id}`}
                        >
                          {isDownloading ? (
                            <Loader2 className="h-4 w-4 animate-spin" />
                          ) : (
                            <>
                              <Download className="h-4 w-4 mr-2" />
                              Download
                            </>
                          )}
                        </Button>
                      )}
                      <Button
                        variant="outline"
                        size="sm"
                        className="flex-1"
                        onClick={() => generateAndDownloadCSV(job.job_id)}
                        disabled={isProcessing || isDownloading}
                        aria-busy={isProcessing}
                        aria-label={s3File ? `Regenerate CSV for ${job.job_id}` : `Generate CSV for ${job.job_id}`}
                      >
                        {isProcessing ? (
                          <>
                            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                            Submitting...
                          </>
                        ) : s3File ? (
                          <>
                            <RefreshCw className="h-4 w-4 mr-2" />
                            Regenerate CSV
                          </>
                        ) : (
                          <>
                            <FileSpreadsheet className="h-4 w-4 mr-2" />
                            Generate CSV
                          </>
                        )}
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )
          })}
        </section>
      )}
  
      {/* Available CSV Files */}
      {s3CsvFiles.size > 0 && (
        <section aria-label="Available CSV files">
          <Card className="border">
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                <Database className="h-5 w-5" />
                Available CSV Files ({s3CsvFiles.size} files)
              </CardTitle>
              <p className="text-sm text-muted-foreground">
                CSV files available for download from S3
              </p>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {Array.from(s3CsvFiles.entries()).map(([jobId, s3File]) => (
                  <div key={jobId} className="p-4 border rounded-lg bg-muted/30">
                    <div className="space-y-3">
                      <div>
                        <p className="font-mono text-sm truncate" title={jobId}>
                          {jobId}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {s3File.filename} ({(s3File.size / 1024).toFixed(1)} KB)
                        </p>
                      </div>
  
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3 text-xs text-muted-foreground">
                          <div className="flex items-center gap-1">
                            <Clock className="h-3 w-3" />
                            <span>
                              Generated {new Date(s3File.last_modified).toLocaleString()}
                            </span>
                          </div>
                        </div>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => downloadFromS3(s3File)}
                          aria-label={`Download CSV for ${jobId}`}
                        >
                          <Download className="h-4 w-4 mr-1" />
                          Download
                        </Button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </section>
      )}
    </main>
  )
}
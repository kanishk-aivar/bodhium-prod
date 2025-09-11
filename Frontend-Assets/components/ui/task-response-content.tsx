"use client"

import { useState, useEffect } from "react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Download, ExternalLink, AlertTriangle, Loader2 } from "lucide-react"
import { Markdown } from "@/components/ui/markdown"
import type { RDSTaskResult, TaskContentResponse } from "../../app/lib/types"

interface TaskResponseContentProps {
  task: RDSTaskResult
  onDownload: (task: RDSTaskResult) => void
}

export function TaskResponseContent({ task, onDownload }: TaskResponseContentProps) {
  const [contentData, setContentData] = useState<TaskContentResponse | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchTaskContent()
  }, [task.task_id])

  const fetchTaskContent = async () => {
    if (task.status !== 'completed') return

    setIsLoading(true)
    setError(null)

    try {
      const response = await fetch(`/api/task-content/${task.task_id}`)
      const data = await response.json()
      
      if (response.ok) {
        setContentData(data)
      } else {
        setError(data.error || "Failed to load content")
      }
    } catch (err) {
      setError("Failed to fetch task content")
    } finally {
      setIsLoading(false)
    }
  }

  const getStatusBadgeColor = (status: string | null) => {
    switch (status) {
      case "completed":
        return "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border border-emerald-500/20"
      case "failed":
        return "bg-red-500/15 text-red-700 dark:text-red-300 border border-red-500/20"
      case "processing":
        return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
      default:
        return "bg-amber-500/15 text-amber-700 dark:text-amber-300 border border-amber-500/20"
    }
  }

  const getModelBadgeColor = (model: string | null) => {
    if (!model) return "bg-gray-500/15 text-gray-700 dark:text-gray-300 border border-gray-500/20"
    
    const modelLower = model.toLowerCase()
    if (modelLower.includes("chatgpt")) {
      return "bg-orange-500/15 text-orange-700 dark:text-orange-300 border border-orange-500/20"
    } else if (modelLower.includes("perplexity")) {
      return "bg-purple-500/15 text-purple-700 dark:text-purple-300 border border-purple-500/20"
    } else if (modelLower.includes("aimode") || modelLower.includes("ai_mode")) {
      return "bg-green-500/15 text-green-700 dark:text-green-300 border border-green-500/20"
    } else if (modelLower.includes("aioverview") || modelLower.includes("aio")) {
      return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
    }
    return "bg-gray-500/15 text-gray-700 dark:text-gray-300 border border-gray-500/20"
  }

  const renderFailedTask = () => (
    <div className="space-y-4">
      <div className="flex items-center gap-2 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
        <AlertTriangle className="h-5 w-5 text-red-600 dark:text-red-400" />
        <div className="flex-1">
          <h4 className="font-semibold text-red-800 dark:text-red-200">Task Failed</h4>
          <p className="text-sm text-red-600 dark:text-red-300">This task encountered an error during processing.</p>
        </div>
      </div>

      {task.error_message && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Error Details</h4>
          <div className="bg-muted/50 rounded-lg p-4 border border-red-200 dark:border-red-800">
            <pre className="text-sm text-red-700 dark:text-red-300 whitespace-pre-wrap font-mono">
              {task.error_message}
            </pre>
          </div>
        </div>
      )}
    </div>
  )

  const renderCompletedTask = () => {
    if (isLoading) {
      return (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-6 w-6 animate-spin mr-2" />
          <span className="text-sm text-muted-foreground">Loading content...</span>
        </div>
      )
    }

    if (error) {
      return (
        <div className="p-4 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg">
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-4 w-4 text-amber-600 dark:text-amber-400" />
            <span className="text-sm text-amber-800 dark:text-amber-200">{error}</span>
          </div>
        </div>
      )
    }

    if (!contentData?.s3_content) {
      return (
        <div className="p-4 bg-muted/30 rounded-lg">
          <p className="text-sm text-muted-foreground">No content available for this task.</p>
        </div>
      )
    }

    const content = contentData.s3_content

    // Check if it's the new flat Perplexity format
    const isNewFormat = content && typeof content === 'object' && 
                       'content' in content && 'citations' in content && 'model' in content

    return (
      <div className="space-y-4">
        <div>
          <h4 className="font-semibold text-sm mb-2">Response Content</h4>
          <div className="bg-muted/50 rounded-lg p-4">
            {isNewFormat ? (
              // New flat format (Perplexity, etc.)
              <Markdown content={content.content} />
            ) : typeof content === 'string' ? (
              // String content
              <Markdown content={content} />
            ) : content.formatted_markdown ? (
              // Legacy format with formatted_markdown
              <Markdown content={content.formatted_markdown} />
            ) : content.content ? (
              // Legacy format with content field
              <Markdown content={content.content} />
            ) : (
              // Fallback to raw JSON
              <div>
                <p className="text-sm text-muted-foreground mb-2">Raw S3 Content:</p>
                <pre className="text-sm whitespace-pre-wrap bg-muted/30 p-3 rounded border overflow-x-auto">
                  {JSON.stringify(content, null, 2)}
                </pre>
              </div>
            )}
          </div>
        </div>

        {/* Citations section (new format) */}
        {isNewFormat && content.citations && content.citations.length > 0 && (
          <div>
            <h4 className="font-semibold text-sm mb-2">Citations</h4>
            <div className="space-y-2">
              {content.citations.map((citation: string, index: number) => (
                <a
                  key={index}
                  href={citation}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-200 hover:underline"
                >
                  <ExternalLink className="h-3 w-3 shrink-0" />
                  <span className="truncate">{citation}</span>
                </a>
              ))}
            </div>
          </div>
        )}

        {/* Links section (legacy format) */}
        {!isNewFormat && content && typeof content === 'object' && content.links && content.links.length > 0 && (
          <div>
            <h4 className="font-semibold text-sm mb-2">Related Links</h4>
            <div className="space-y-2">
              {content.links.map((link: any, index: number) => (
                <a
                  key={index}
                  href={link.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-2 text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-200 hover:underline"
                >
                  <ExternalLink className="h-3 w-3 shrink-0" />
                  <span className="truncate">{link.text}</span>
                </a>
              ))}
            </div>
          </div>
        )}

        {/* Related questions section (both formats) */}
        {content && typeof content === 'object' && content.related_questions && content.related_questions.length > 0 && (
          <div>
            <h4 className="font-semibold text-sm mb-2">Related Questions</h4>
            <ul className="space-y-1">
              {content.related_questions.map((question: string, index: number) => (
                <li key={index} className="text-sm text-muted-foreground flex items-start gap-2">
                  <span className="text-primary mt-1 shrink-0">•</span>
                  <span>{question}</span>
                </li>
              ))}
            </ul>
          </div>
        )}

        {/* Additional metadata for new format */}
        {isNewFormat && (
          <div>
            <h4 className="font-semibold text-sm mb-2">Query Details</h4>
            <div className="bg-muted/30 rounded-lg p-3 space-y-1">
              {content.query && (
                <div className="text-sm">
                  <span className="text-muted-foreground">Query:</span>
                  <p className="mt-1 text-foreground">{content.query}</p>
                </div>
              )}
              {content.timestamp && (
                <div className="flex justify-between text-xs pt-2 border-t border-border">
                  <span className="text-muted-foreground">Response Generated:</span>
                  <span className="font-mono">{new Date(content.timestamp).toLocaleString()}</span>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    )
  }

  const renderPendingTask = () => (
    <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
      <div className="flex items-center gap-2">
        <Loader2 className="h-4 w-4 animate-spin text-blue-600 dark:text-blue-400" />
        <span className="text-sm text-blue-800 dark:text-blue-200">
          Task is {task.status === 'processing' ? 'currently processing' : 'pending processing'}...
        </span>
      </div>
    </div>
  )

  const renderContent = () => {
    switch (task.status) {
      case 'failed':
        return renderFailedTask()
      case 'completed':
        return renderCompletedTask()
      case 'processing':
      default:
        return renderPendingTask()
    }
  }

  return (
    <div className="space-y-6">
      {/* Header with metadata */}
      <div className="flex items-start justify-between">
        <div className="space-y-2">
          <div className="flex items-center gap-2 flex-wrap">
            <Badge className={getModelBadgeColor(task.llm_model_name)}>
              {task.llm_model_name || "Unknown Model"}
            </Badge>
            <Badge className={getStatusBadgeColor(task.status)}>
              {task.status || "unknown"}
            </Badge>
            {task.query_id && (
              <span className="text-sm text-muted-foreground">
                Query #{task.query_id}
              </span>
            )}
          </div>
          <div>
            <h3 className="font-medium text-base">{task.query_text || "No query text available"}</h3>
            <p className="text-sm text-muted-foreground">
              Created: {new Date(task.created_at).toLocaleString()}
              {task.completed_at && (
                <> • Completed: {new Date(task.completed_at).toLocaleString()}</>
              )}
            </p>
          </div>
        </div>
        
        {task.status === 'completed' && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => onDownload(task)}
            className="shrink-0"
          >
            <Download className="h-4 w-4 mr-2" />
            Download
          </Button>
        )}
      </div>

      {/* Content based on status */}
      {renderContent()}

      {/* Task metadata */}
      <div>
        <h4 className="font-semibold text-sm mb-2">Task Details</h4>
        <div className="bg-muted/30 rounded-lg p-3 space-y-1">
          <div className="flex justify-between text-xs">
            <span className="text-muted-foreground">Task ID:</span>
            <span className="font-mono">{task.task_id}</span>
          </div>
          {task.job_id && (
            <div className="flex justify-between text-xs">
              <span className="text-muted-foreground">Job ID:</span>
              <span className="font-mono">{task.job_id}</span>
            </div>
          )}
          {task.s3_output_path && (
            <div className="flex justify-between text-xs">
              <span className="text-muted-foreground">S3 Path:</span>
              <span className="font-mono text-xs break-all">{task.s3_output_path}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

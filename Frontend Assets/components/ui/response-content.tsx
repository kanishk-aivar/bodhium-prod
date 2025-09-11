"use client"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Download, ExternalLink } from "lucide-react"
import { Markdown } from "@/components/ui/markdown"
import type { S3WorkerResult } from "../../app/lib/types"

interface ResponseContentProps {
  result: S3WorkerResult
  productName: string
  brand: string
  onDownload: (result: S3WorkerResult, productName: string, brand: string) => void
}

export function ResponseContent({ result, productName, brand, onDownload }: ResponseContentProps) {
  const getWorkerBadgeColor = (model: string) => {
    const modelLower = model.toLowerCase()
    if (modelLower.includes("google") && modelLower.includes("overview")) {
      return "bg-blue-500/15 text-blue-700 dark:text-blue-300 border border-blue-500/20"
    } else if (modelLower.includes("google") && modelLower.includes("mode")) {
      return "bg-green-500/15 text-green-700 dark:text-green-300 border border-green-500/20"
    } else if (modelLower.includes("perplexity")) {
      return "bg-purple-500/15 text-purple-700 dark:text-purple-300 border border-purple-500/20"
    } else if (modelLower.includes("chatgpt")) {
      return "bg-orange-500/15 text-orange-700 dark:text-orange-300 border border-orange-500/20"
    }
    return "bg-gray-500/15 text-gray-700 dark:text-gray-300 border border-gray-500/20"
  }

  const renderAIOContent = () => (
    <div className="space-y-4">
      {result.formatted_markdown && (
        <div>
          <h4 className="font-semibold text-sm mb-2">AI Overview Response</h4>
          <div className="bg-muted/50 rounded-lg p-4">
            <Markdown content={result.formatted_markdown} />
          </div>
        </div>
      )}
      {result.content && result.content !== result.formatted_markdown && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Raw Content</h4>
          <div className="bg-muted/30 rounded-lg p-4">
            <Markdown content={result.content} />
          </div>
        </div>
      )}
    </div>
  )

  const renderAIMContent = () => (
    <div className="space-y-4">
      <div>
        <h4 className="font-semibold text-sm mb-2">AI Mode Response</h4>
        <div className="bg-muted/50 rounded-lg p-4">
          <Markdown content={result.content} />
        </div>
      </div>
      {result.metadata?.navigation_method && (
        <div className="text-xs text-muted-foreground">
          <strong>Navigation Method:</strong> {result.metadata.navigation_method}
        </div>
      )}
    </div>
  )

  const renderPerplexityContent = () => (
    <div className="space-y-4">
      <div>
        <h4 className="font-semibold text-sm mb-2">Perplexity Response</h4>
        <div className="bg-muted/50 rounded-lg p-4">
          <Markdown content={result.content} />
        </div>
      </div>
    </div>
  )

  const renderChatGPTContent = () => (
    <div className="space-y-4">
      {result.formatted_markdown ? (
        <div>
          <h4 className="font-semibold text-sm mb-2">ChatGPT Response</h4>
          <div className="bg-muted/50 rounded-lg p-4">
            <Markdown content={result.formatted_markdown} />
          </div>
        </div>
      ) : (
        <div>
          <h4 className="font-semibold text-sm mb-2">ChatGPT Response</h4>
          <div className="bg-muted/50 rounded-lg p-4">
            <Markdown content={result.content} />
          </div>
        </div>
      )}
    </div>
  )

  const renderContent = () => {
    const model = result.model.toLowerCase()
    
    if (model.includes("overview") || model.includes("aio")) {
      return renderAIOContent()
    } else if (model.includes("ai_mode") || model.includes("aim")) {
      return renderAIMContent()
    } else if (model.includes("perplexity")) {
      return renderPerplexityContent()
    } else if (model.includes("chatgpt")) {
      return renderChatGPTContent()
    } else {
      // Fallback for unknown types
      return (
        <div>
          <h4 className="font-semibold text-sm mb-2">Response</h4>
          <div className="bg-muted/50 rounded-lg p-4">
            {result.formatted_markdown ? (
              <Markdown content={result.formatted_markdown} />
            ) : (
              <Markdown content={result.content} />
            )}
          </div>
        </div>
      )
    }
  }

  return (
    <div className="space-y-6">
      {/* Header with metadata */}
      <div className="flex items-start justify-between">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <Badge className={getWorkerBadgeColor(result.model)}>
              {result.model}
            </Badge>
            <span className="text-sm text-muted-foreground">
              Query #{result.query_id}
            </span>
          </div>
          <div>
            <h3 className="font-medium text-base">{result.query}</h3>
            <p className="text-sm text-muted-foreground">
              {new Date(result.timestamp).toLocaleString()}
            </p>
          </div>
        </div>
        <Button
          variant="outline"
          size="sm"
          onClick={() => onDownload(result, productName, brand)}
          className="shrink-0"
        >
          <Download className="h-4 w-4 mr-2" />
          Download
        </Button>
      </div>

      {/* Response content */}
      {renderContent()}

      {/* Links section */}
      {result.links && result.links.length > 0 && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Related Links</h4>
          <div className="space-y-2">
            {result.links.map((link, index) => (
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

      {/* Related questions section */}
      {result.related_questions && result.related_questions.length > 0 && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Related Questions</h4>
          <ul className="space-y-1">
            {result.related_questions.map((question, index) => (
              <li key={index} className="text-sm text-muted-foreground flex items-start gap-2">
                <span className="text-primary mt-1 shrink-0">•</span>
                <span>{question}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Metadata section */}
      {result.metadata && Object.keys(result.metadata).length > 0 && (
        <div>
          <h4 className="font-semibold text-sm mb-2">Technical Details</h4>
          <div className="bg-muted/30 rounded-lg p-3 space-y-1">
            {Object.entries(result.metadata).map(([key, value]) => (
              <div key={key} className="flex justify-between text-xs">
                <span className="text-muted-foreground capitalize">
                  {key.replace(/_/g, ' ')}:
                </span>
                <span className="font-mono">{String(value)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

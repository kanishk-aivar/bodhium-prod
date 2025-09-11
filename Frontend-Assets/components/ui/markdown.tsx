"use client"

import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { cn } from "@/lib/utils"

interface MarkdownProps {
  content: string
  className?: string
}

export function Markdown({ content, className }: MarkdownProps) {
  return (
    <div className={cn("prose prose-sm max-w-none dark:prose-invert", className)}>
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        components={{
          // Headings
          h1: ({ children }) => <h1 className="text-xl font-bold mb-3 mt-4 text-foreground border-b pb-1">{children}</h1>,
          h2: ({ children }) => <h2 className="text-lg font-semibold mb-2 mt-3 text-foreground">{children}</h2>,
          h3: ({ children }) => <h3 className="text-base font-semibold mb-2 mt-3 text-foreground">{children}</h3>,
          h4: ({ children }) => <h4 className="text-sm font-semibold mb-1 mt-2 text-foreground">{children}</h4>,
          h5: ({ children }) => <h5 className="text-sm font-medium mb-1 mt-2 text-foreground">{children}</h5>,
          h6: ({ children }) => <h6 className="text-xs font-medium mb-1 mt-2 text-muted-foreground">{children}</h6>,
          
          // Paragraphs and text
          p: ({ children }) => <p className="text-sm mb-3 leading-relaxed text-foreground whitespace-pre-wrap">{children}</p>,
          
          // Lists
          ul: ({ children }) => <ul className="text-sm mb-3 ml-4 list-disc space-y-1">{children}</ul>,
          ol: ({ children }) => <ol className="text-sm mb-3 ml-4 list-decimal space-y-1">{children}</ol>,
          li: ({ children }) => <li className="mb-1 leading-relaxed">{children}</li>,
          
          // Text formatting
          strong: ({ children }) => <strong className="font-semibold text-foreground">{children}</strong>,
          em: ({ children }) => <em className="italic text-foreground">{children}</em>,
          del: ({ children }) => <del className="line-through text-muted-foreground">{children}</del>,
          
          // Task lists (checkbox support)
          input: ({ type, checked, ...props }) => {
            if (type === 'checkbox') {
              return (
                <input
                  type="checkbox"
                  checked={checked}
                  readOnly
                  className="mr-2 rounded border-border focus:ring-primary"
                  {...props}
                />
              )
            }
            return <input type={type} {...props} />
          },
          
          // Code
          code: ({ children }) => (
            <code className="bg-muted px-1.5 py-0.5 rounded text-xs font-mono text-foreground border">{children}</code>
          ),
          pre: ({ children }) => (
            <pre className="bg-muted border rounded-lg p-3 text-xs font-mono overflow-x-auto mb-3 text-foreground">{children}</pre>
          ),
          
          // Quotes
          blockquote: ({ children }) => (
            <blockquote className="border-l-4 border-primary/30 pl-4 py-2 my-3 bg-muted/20 rounded-r italic text-muted-foreground">
              {children}
            </blockquote>
          ),
          
          // Links
          a: ({ href, children }) => (
            <a 
              href={href} 
              target="_blank" 
              rel="noopener noreferrer"
              className="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-200 underline underline-offset-2 transition-colors"
            >
              {children}
            </a>
          ),
          
          // Tables
          table: ({ children }) => (
            <div className="overflow-x-auto mb-6 rounded-lg border-2 border-border shadow-sm">
              <table className="min-w-full border-collapse">{children}</table>
            </div>
          ),
          thead: ({ children }) => <thead className="bg-muted/90 border-b-2 border-border">{children}</thead>,
          tbody: ({ children }) => <tbody className="bg-background">{children}</tbody>,
          tr: ({ children }) => <tr className="border-b border-border hover:bg-muted/20 transition-colors">{children}</tr>,
          th: ({ children }) => (
            <th className="px-6 py-4 text-left text-sm font-bold text-foreground uppercase tracking-wider border-r border-border last:border-r-0 bg-muted/90">
              {children}
            </th>
          ),
          td: ({ children }) => (
            <td className="px-6 py-4 text-sm text-foreground border-r border-border last:border-r-0 align-top leading-relaxed">
              {children}
            </td>
          ),
          
          // Horizontal rule
          hr: () => <hr className="border-border my-4" />,
          
          // Line breaks
          br: () => <br className="mb-1" />,
          
          // Images (if any)
          img: ({ src, alt, title }) => (
            <img 
              src={src} 
              alt={alt} 
              title={title}
              className="max-w-full h-auto rounded-lg border border-border my-2"
            />
          ),
        }}
      >
        {content}
      </ReactMarkdown>
    </div>
  )
}

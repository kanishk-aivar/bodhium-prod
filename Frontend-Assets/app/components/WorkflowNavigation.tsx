"use client"

import { CheckCircle, ExternalLink } from "lucide-react"

interface WorkflowNavigationProps {
  currentStep: number
}

export default function WorkflowNavigation({ currentStep }: WorkflowNavigationProps) {
  const steps = [
    { number: 1, label: "Submit URL" },
    { number: 2, label: "Scraping" },
    { number: 3, label: "Select Products" },
    { number: 4, label: "Select Queries" },
    { number: 5, label: "Processing" },
  ]

  return (
    <div className="fixed bottom-0 left-0 right-0 border-t shadow-lg backdrop-blur supports-[backdrop-filter]:bg-white/50 bg-white/60 dark:bg-white/5 dark:border-white/10">
      <div className="relative">
        <div className="pointer-events-none absolute inset-0 bg-gradient-to-r from-transparent via-[hsl(var(--accent))]/10 to-transparent" />
      </div>
      <div className="relative container mx-auto px-4 py-4">
        <div className="flex items-center justify-center space-x-8">
          {steps.map((step, index) => (
            <div key={step.number} className="flex items-center">
              <div className="flex flex-col items-center">
                <div
                  className={`flex items-center justify-center w-9 h-9 rounded-full border-2 shadow-sm ${
                    currentStep > step.number
                      ? "bg-[hsl(var(--accent))] border-[hsl(var(--accent))] text-[hsl(var(--accent-foreground))]"
                      : currentStep === step.number
                        ? "bg-[hsl(var(--primary))] border-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))]"
                        : "bg-white/60 border-white/60 text-foreground/60"
                  }`}
                >
                  {currentStep > step.number ? (
                    <CheckCircle className="w-5 h-5" />
                  ) : (
                    <span className="text-sm font-medium">{step.number}</span>
                  )}
                </div>
                <span className={`text-xs mt-1 ${currentStep >= step.number ? "text-foreground" : "text-muted-foreground"}`}>
                  {step.label}
                </span>
              </div>
              {index < steps.length - 1 && (
                <div className={`w-12 h-0.5 mx-4 ${
                  currentStep > step.number ? "bg-[hsl(var(--accent))]" : "bg-white/60"
                }`} />
              )}
            </div>
          ))}

          {/* Results Button - Always Visible */}
          <div className="flex items-center ml-8">
            <div className="w-12 h-0.5 bg-white/60" />
            <button
              onClick={() => window.open("/results", "_blank")}
              className="flex flex-col items-center ml-4 p-2 rounded-lg hover:bg-white/60 transition-colors"
            >
              <div className="flex items-center justify-center w-9 h-9 rounded-full border-2 bg-[hsl(var(--accent))] border-[hsl(var(--accent))] text-[hsl(var(--accent-foreground))] shadow-sm">
                <ExternalLink className="w-4 h-4" />
              </div>
              <span className="text-xs mt-1 text-foreground">Results</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}

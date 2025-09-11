"use client"

import { Card, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import type { Query } from "../lib/types"

interface QuerySelectorProps {
  queries: Query[]
  selectedQueries: number[]
  onSelectionChange: (selected: number[]) => void
  disabled?: boolean
}

export default function QuerySelector({
  queries,
  selectedQueries,
  onSelectionChange,
  disabled = false,
}: QuerySelectorProps) {
  const toggleQuery = (queryId: number) => {
    if (disabled) return

    if (selectedQueries.includes(queryId)) {
      onSelectionChange(selectedQueries.filter((id) => id !== queryId))
    } else {
      onSelectionChange([...selectedQueries, queryId])
    }
  }

  const selectAll = () => {
    if (disabled) return
    onSelectionChange(queries.map((q) => q.query_id))
  }

  const selectNone = () => {
    if (disabled) return
    onSelectionChange([])
  }

  // Filter selectedQueries to only count those that exist in this component's queries
  const relevantSelectedQueries = selectedQueries.filter(id => 
    queries.some(q => q.query_id === id)
  )

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">
            {relevantSelectedQueries.length} of {queries.length} queries selected
          </span>
        </div>
        {!disabled && (
          <div className="flex gap-2">
            <Button size="sm" variant="outline" onClick={selectAll}>
              Select All
            </Button>
            <Button size="sm" variant="outline" onClick={selectNone}>
              Select None
            </Button>
          </div>
        )}
      </div>

      <div className="grid gap-3 max-h-96 overflow-y-auto p-3">
        {queries.map((query) => {
          const isSelected = selectedQueries.includes(query.query_id)

          return (
            <Card
              key={query.query_id}
              className={`rounded-xl border border-white/60 bg-white/60 dark:bg-white/5 dark:border-white/10 backdrop-blur-md shadow-sm transition-colors ${
                isSelected
                  ? "ring-2 ring-[hsl(var(--accent))] bg-white/70"
                  : "hover:bg-white/70"
              }`}
            >
              <CardContent className="p-4">
                <div className="flex items-start gap-3">
                  <Checkbox
                    checked={isSelected}
                    onCheckedChange={() => toggleQuery(query.query_id)}
                    disabled={disabled}
                    className="mt-1"
                  />

                  <div className="flex-1 min-w-0">
                    <p className="text-sm leading-relaxed">{query.query_text}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )
        })}

        {queries.length === 0 && (
          <div className="text-center py-8 text-muted-foreground">
            <p>No queries available. Queries will appear here after generation.</p>
          </div>
        )}
      </div>
    </div>
  )
}

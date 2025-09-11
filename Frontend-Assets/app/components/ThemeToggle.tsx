"use client"

import { useEffect, useState } from "react"
import { Moon, Sun } from "lucide-react"
import { Button } from "@/components/ui/button"

function getPreferredTheme(): "light" | "dark" {
  if (typeof window === "undefined") return "light"
  const stored = localStorage.getItem("theme") as "light" | "dark" | null
  if (stored === "light" || stored === "dark") return stored
  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
  return prefersDark ? "dark" : "light"
}

export default function ThemeToggle() {
  const [theme, setTheme] = useState<"light" | "dark">("light")

  useEffect(() => {
    const initial = getPreferredTheme()
    setTheme(initial)
    document.documentElement.classList.toggle("dark", initial === "dark")
  }, [])

  const toggleTheme = () => {
    const next = theme === "light" ? "dark" : "light"
    setTheme(next)
    localStorage.setItem("theme", next)
    document.documentElement.classList.toggle("dark", next === "dark")
  }

  return (
    <Button onClick={toggleTheme} variant="outline" size="sm" className="rounded-xl">
      {theme === "dark" ? (
        <div className="flex items-center gap-2">
          <Sun className="h-4 w-4" />
          <span>Light</span>
        </div>
      ) : (
        <div className="flex items-center gap-2">
          <Moon className="h-4 w-4" />
          <span>Dark</span>
        </div>
      )}
    </Button>
  )
}



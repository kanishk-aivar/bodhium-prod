"use client"

import { useRouter, usePathname } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Home, Package, MessageSquare, BarChart } from "lucide-react"
import ThemeToggle from "./ThemeToggle"

export default function Navigation() {
  const router = useRouter()
  const pathname = usePathname()

  const navItems = [
    { 
      label: "Home", 
      path: "/", 
      icon: Home,
      description: "Submit URLs" 
    },
    { 
      label: "Products", 
      path: "/products", 
      icon: Package,
      description: "Select products" 
    },
    { 
      label: "Queries", 
      path: "/queries", 
      icon: MessageSquare,
      description: "Manage queries" 
    },
    { 
      label: "Results", 
      path: "/results", 
      icon: BarChart,
      description: "View results" 
    },
  ]

  return (
    <div className="flex gap-2">
      <ThemeToggle />
      {navItems.map((item) => {
        const Icon = item.icon
        const isActive = pathname === item.path
        
        return (
          <Button
            key={item.path}
            variant={isActive ? "default" : "outline"}
            size="sm"
            onClick={() => router.push(item.path)}
            className="flex items-center gap-2"
          >
            <Icon className="h-4 w-4" />
            {item.label}
          </Button>
        )
      })}
    </div>
  )
}

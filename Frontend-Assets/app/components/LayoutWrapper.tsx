"use client"

import { usePathname } from "next/navigation"
import { AppSidebar } from "./AppSidebar"
import { SidebarProvider, SidebarInset } from "@/components/ui/sidebar"

interface LayoutWrapperProps {
  children: React.ReactNode
}

export function LayoutWrapper({ children }: LayoutWrapperProps) {
  const pathname = usePathname()
  const isLoginPage = pathname === "/login"

  if (isLoginPage) {
    // Login page - no sidebar, full width content
    return (
      <div className="relative min-h-screen overflow-hidden bg-gradient-to-br from-[hsl(var(--primary))]/20 via-white to-white dark:from-[hsl(var(--primary))]/25 dark:via-slate-900 dark:to-slate-950">
        {/* Decorative gradients - behind everything */}
        <div className="pointer-events-none absolute -top-40 -left-40 h-[36rem] w-[36rem] rounded-full bg-[radial-gradient(ellipse_at_center,_rgba(255,255,255,0.04),_transparent_60%)] blur-2xl z-0" />
        <div className="pointer-events-none absolute -bottom-40 -right-40 h-[36rem] w-[36rem] rounded-full bg-[radial-gradient(ellipse_at_center,_rgba(34,197,94,0.22),_transparent_60%)] blur-3xl z-0" />
        
        {/* Full width content for login page */}
        <main className="relative z-10">
          {children}
        </main>
      </div>
    )
  }

  // Regular pages - with sidebar
  return (
    <div className="relative min-h-screen overflow-hidden bg-gradient-to-br from-[hsl(var(--primary))]/20 via-white to-white dark:from-[hsl(var(--primary))]/25 dark:via-slate-900 dark:to-slate-950">
      {/* Decorative gradients - behind everything */}
      <div className="pointer-events-none absolute -top-40 -left-40 h-[36rem] w-[36rem] rounded-full bg-[radial-gradient(ellipse_at_center,_rgba(255,255,255,0.04),_transparent_60%)] blur-2xl z-0" />
      <div className="pointer-events-none absolute -bottom-40 -right-40 h-[36rem] w-[36rem] rounded-full bg-[radial-gradient(ellipse_at_center,_rgba(34,197,94,0.22),_transparent_60%)] blur-3xl z-0" />

      {/* App shell with sidebar */}
      <SidebarProvider>
        <AppSidebar />
        <main className="relative z-10 flex-1 overflow-auto">
          {/* Content container with subtle padding so glass surfaces breathe */}
          <div className="px-4 py-6 sm:px-6 lg:px-8">
            {children}
          </div>
        </main>
      </SidebarProvider>
    </div>
  )
}

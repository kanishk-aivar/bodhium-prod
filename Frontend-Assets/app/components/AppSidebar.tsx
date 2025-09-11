"use client"

import { Home, Package, MessageSquare, BarChart, FileSpreadsheet, LogOut, User } from "lucide-react"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarHeader,
} from "@/components/ui/sidebar"
import { Button } from "@/components/ui/button"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { useSession, signOut } from "next-auth/react"
import { useState } from "react"
import ThemeToggle from "./ThemeToggle"

const items = [
  {
    title: "Home",
    url: "/",
    icon: Home,
    description: "Submit URLs & History"
  },
  {
    title: "Products",
    url: "/products",
    icon: Package,
    description: "Browse Database"
  },
  {
    title: "Queries",
    url: "/queries", 
    icon: MessageSquare,
    description: "Select & Create"
  },
  {
    title: "Results",
    url: "/results",
    icon: BarChart,
    description: "View Analysis"
  },
  {
    title: "Ad-hoc Jobs",
    url: "/adhoc",
    icon: FileSpreadsheet,
    description: "Download CSV Results"
  },
]

// Component to handle profile image with fallback
function ProfileImage({ src, alt, className }: { src?: string; alt: string; className: string }) {
  const [imageError, setImageError] = useState(false)
  const [imageLoaded, setImageLoaded] = useState(false)

  // If no src or image failed to load, show fallback
  if (!src || imageError) {
    return (
      <div className={`flex items-center justify-center rounded-full bg-gradient-to-br from-[hsl(var(--primary))] to-[hsl(var(--accent))] text-primary-foreground ${className}`}>
        <User className="h-4 w-4" />
      </div>
    )
  }

  return (
    <img 
      src={src} 
      alt={alt}
      className={`rounded-full object-cover border border-border ${className} ${!imageLoaded ? 'opacity-0' : 'opacity-100'} transition-opacity duration-200`}
      onError={() => setImageError(true)}
      onLoad={() => setImageLoaded(true)}
    />
  )
}

export function AppSidebar() {
  const pathname = usePathname()
  const { data: session, status } = useSession()

  const handleLogout = async () => {
    await signOut({ callbackUrl: "/login" })
  }

  return (
    <Sidebar className="fixed left-0 top-0 z-50 h-screen">
      <SidebarHeader className="p-4">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-[hsl(var(--primary))] to-[hsl(var(--accent))] text-primary-foreground">
            <BarChart className="h-4 w-4" />
          </div>
          <div className="flex flex-col">
            <span className="text-lg font-semibold bg-gradient-to-r from-[hsl(var(--foreground))] to-[hsl(var(--accent))] bg-clip-text text-transparent">
              Bodhium
            </span>
            <span className="text-xs text-muted-foreground">AI Brand Analysis</span>
          </div>
        </div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Workflow</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {items.map((item) => (
                <SidebarMenuItem key={item.title}>
                  <SidebarMenuButton asChild isActive={pathname === item.url} tooltip={item.title}>
                    <Link href={item.url} className="flex items-center gap-2 w-full">
                      <item.icon className="h-4 w-4" />
                      <span>{item.title}</span>
                    </Link>
                  </SidebarMenuButton>
                </SidebarMenuItem>
              ))}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Settings</SidebarGroupLabel>
          <SidebarGroupContent>
            <div className="px-2 py-1">
              <ThemeToggle />
            </div>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter className="mt-auto p-4 border-t">
        {session?.user && (
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-sm">
              <ProfileImage 
                src={session.user.image} 
                alt={session.user.name || session.user.email || "User"} 
                className="h-8 w-8"
              />
              <div className="flex flex-col overflow-hidden">
                <span className="font-medium truncate">{session.user.name || session.user.email}</span>
                <span className="text-xs text-muted-foreground truncate">{session.user.email}</span>
              </div>
            </div>
            <Button 
              variant="outline" 
              size="sm" 
              onClick={handleLogout}
              className="w-full justify-start gap-2"
            >
              <LogOut className="h-4 w-4" />
              Sign out
            </Button>
          </div>
        )}
      </SidebarFooter>
    </Sidebar>
  )
}

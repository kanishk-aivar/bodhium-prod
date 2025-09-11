  import type React from "react"
import type { Metadata } from "next"
import Script from "next/script"
import { Toaster } from "./components/Toaster"
import { LayoutWrapper } from "./components/LayoutWrapper"
import { Providers } from "./providers"

  export const metadata: Metadata = {
  title: "Bodhium Workflow - AI-Powered Brand Analysis",
  description: "Automate brand analysis with AI-powered insights. Submit URLs, select products, generate queries, and get comprehensive results.",
  keywords: ["AI", "brand analysis", "workflow automation", "data scraping", "business intelligence"],
  authors: [{ name: "Bodhium" }],
  creator: "Bodhium",
  publisher: "Bodhium",
  viewport: "width=device-width, initial-scale=1",
}

  export default function RootLayout({
    children,
  }: Readonly<{
    children: React.ReactNode
  }>) {
      return (
    <html lang="en">
      <head>
      <link rel="stylesheet" href="/tailwind.css" />
      </head>
      <body className="text-foreground">
        <Providers>
          <LayoutWrapper>
            {children}
          </LayoutWrapper>
          <Toaster /> 
        </Providers>
      </body>
    </html>
  )
  }

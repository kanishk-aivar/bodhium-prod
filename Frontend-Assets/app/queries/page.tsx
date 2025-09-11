"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Loader2, ArrowRight, RefreshCw, Search, ChevronDown, ChevronRight } from "lucide-react"
import { useToast } from "../hooks/use-toast"
import ProductSelector from "../components/ProductSelector"
import QuerySelector from "../components/QuerySelector"
import type { ScrapeJob, Product, Query } from "../lib/types"

type ProductQueries = {
  product_id: number
  queries: Query[]
}

export default function QueriesPage() {
  const [brands, setBrands] = useState<string[]>([])
  const [selectedBrand, setSelectedBrand] = useState<string>("")
  const [jobs, setJobs] = useState<ScrapeJob[]>([])
  const [filteredJobs, setFilteredJobs] = useState<ScrapeJob[]>([])
  const [selectedJob, setSelectedJob] = useState<ScrapeJob | null>(null)
  const [products, setProducts] = useState<Product[]>([])
  const [selectedProducts, setSelectedProducts] = useState<number[]>([])
  const [queriesByProduct, setQueriesByProduct] = useState<ProductQueries[]>([])
  const [selectedQueries, setSelectedQueries] = useState<number[]>([])
  const [selectedCustomQueries, setSelectedCustomQueries] = useState<number[]>([])
  const [newQueries, setNewQueries] = useState<string[]>([])
  const [customQuery, setCustomQuery] = useState("")
  const [isLoading, setIsLoading] = useState(true)
  const [isLoadingProducts, setIsLoadingProducts] = useState(false)
  const [isGeneratingQueries, setIsGeneratingQueries] = useState(false)
  const [isProcessingQueries, setIsProcessingQueries] = useState(false)
  const [isSubmittingProducts, setIsSubmittingProducts] = useState(false)
  const [isRefreshingQueries, setIsRefreshingQueries] = useState(false)
  const [queriesExist, setQueriesExist] = useState(false)
  
  // New state for the redesigned UI
  const [selectedProductId, setSelectedProductId] = useState<number | null>(null)
  const [productSearchTerm, setProductSearchTerm] = useState("")
  const [isLoadingQueries, setIsLoadingQueries] = useState(false)
  const [isInitialQueriesLoading, setIsInitialQueriesLoading] = useState(false)
  const [selectedProductsForBatch, setSelectedProductsForBatch] = useState<number[]>([])
  const [expandedQueryTypes, setExpandedQueryTypes] = useState<Set<string>>(new Set())

  const { toast } = useToast()
  const router = useRouter()

  // Utility: store brand in localStorage
  const persistSelectedBrand = (brand: string) => {
    if (brand) localStorage.setItem("selectedBrand", brand)
    else localStorage.removeItem("selectedBrand")
  }

  // Run fetchJobs initially, but actual brand restore happens only after brands is set (see brands effect)
  useEffect(() => {
    fetchJobs()
  }, [])

  // Restore selected brand if present in localStorage *and* available in brands
  useEffect(() => {
    const storedBrand = localStorage.getItem("selectedBrand")
    if (brands.length === 0 || !storedBrand) return
    if (brands.includes(storedBrand)) {
      setSelectedBrand(storedBrand)
      setIsLoading(true)
      // Find latest job for this brand and set it
      const brandJobs = jobs.filter(job => job.brand_name === storedBrand)
      setFilteredJobs(brandJobs)
      if (brandJobs.length > 0) {
        const latestJob = brandJobs.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0]
        setSelectedJob(latestJob)
        setIsLoadingProducts(true)
        fetchProducts(latestJob.job_id).then(() => {
          setIsLoadingProducts(false)
          setIsInitialQueriesLoading(true)
          const allProductIds = products.length
            ? products.map(p => p.product_id)
            : []
          if (allProductIds.length > 0) {
            checkExistingQueries(latestJob.job_id, allProductIds).finally(() => setIsInitialQueriesLoading(false))
          } else {
            setIsInitialQueriesLoading(false)
          }
          console.log('Loaded brand from localStorage:', storedBrand)
        })
      }
      setIsLoading(false)
    }
    // else: do nothing, dropdown will be shown
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [brands])

  const resetAllSelections = () => {
    setSelectedBrand("")
    persistSelectedBrand("")
    setSelectedJob(null)
    setProducts([])
    setSelectedProducts([])
    setQueriesByProduct([])
    setSelectedQueries([])
    setSelectedCustomQueries([])
    setNewQueries([])
    setSelectedProductId(null)
    setProductSearchTerm("")
    setSelectedProductsForBatch([])
    setExpandedQueryTypes(new Set())
    setQueriesExist(false)
  }

  // Fetch jobs, also resets main UI
  const fetchJobs = async () => {
    try {
      setIsLoading(true)
      const response = await fetch("/api/jobs")
      const data = await response.json()
      if (Array.isArray(data)) {
        const successfulJobs = data.filter((job: ScrapeJob) => job.status === "JOB_SUCCESS")
        setJobs(successfulJobs)
        // Extract unique brands
        const uniqueBrands = Array.from(new Set(
          successfulJobs
            .map(job => job.brand_name)
            .filter((brand): brand is string => Boolean(brand))
        )).sort()
        setBrands(uniqueBrands)
        setFilteredJobs([])
        setSelectedJob(null)
        setProducts([])
        setSelectedProducts([])
        setQueriesByProduct([])
      } else {
        setJobs([])
        setBrands([])
        setFilteredJobs([])
        setSelectedJob(null)
        setProducts([])
        setSelectedProducts([])
        setQueriesByProduct([])
        toast({
          title: "Warning",
          description: "Failed to load jobs",
          variant: "destructive",
        })
      }
    } catch (error) {
      setJobs([])
      setBrands([])
      setFilteredJobs([])
      setSelectedJob(null)
      setProducts([])
      setSelectedProducts([])
      setQueriesByProduct([])
      toast({
        title: "Error",
        description: "Failed to fetch jobs",
        variant: "destructive",
      })
    } finally {
      setIsLoading(false)
    }
  }

  const fetchProducts = async (jobId: string) => {
    try {
      setIsLoadingProducts(true)
      const response = await fetch(`/api/products/${jobId}`)
      const data = await response.json()
      setProducts(data)
      if (data && data.length > 0) {
        setIsInitialQueriesLoading(true)
        try {
          const allProductIds = data.map((p: Product) => p.product_id)
          await checkExistingQueries(jobId, allProductIds)
        } finally {
          setIsInitialQueriesLoading(false)
        }
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to fetch products",
        variant: "destructive",
      })
      setProducts([])
    } finally {
      setIsLoadingProducts(false)
    }
  }

  // Handles UI, triggers api, and persists selection
  const handleBrandSelect = (brand: string) => {
    setSelectedBrand(brand)
    persistSelectedBrand(brand)
    setSelectedJob(null)
    setProducts([])
    setSelectedProducts([])
    setQueriesByProduct([])
    setSelectedQueries([])
    setSelectedCustomQueries([])
    setNewQueries([])
    setQueriesExist(false)
    setSelectedProductId(null)
    setProductSearchTerm("")
    // Filter jobs by selected brand
    const brandJobs = jobs.filter(job => job.brand_name === brand)
    setFilteredJobs(brandJobs)
    if (brandJobs.length > 0) {
      const latestJob = brandJobs.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0]
      setSelectedJob(latestJob)
      setIsLoadingProducts(true)
      fetchProducts(latestJob.job_id).then(() => setIsLoadingProducts(false))
    }
  }

  const handleJobSelect = (job: ScrapeJob) => {
    setSelectedJob(job)
    setSelectedProducts([])
    setQueriesByProduct([])
    setSelectedQueries([])
    setSelectedCustomQueries([])
    setNewQueries([])
    setQueriesExist(false)
    setSelectedProductId(null)
    setProductSearchTerm("")
    fetchProducts(job.job_id)
  }

  const handleProductSelectionChange = (newSelection: number[]) => {
    setSelectedProducts(newSelection)
    setQueriesByProduct([])
    setSelectedQueries([])
    setSelectedCustomQueries([])
    setNewQueries([])
    setQueriesExist(false)
  }

  // New handlers for redesigned UI
  const handleProductClick = async (productId: number) => {
    setSelectedProductId(productId)
    if (selectedJob && queriesByProduct.length === 0) {
      const allProductIds = products.map(p => p.product_id)
      await checkExistingQueries(selectedJob.job_id, allProductIds)
    }
  }

  const refreshCurrentProductQueries = async () => {
    if (!selectedJob) return
    setIsRefreshingQueries(true)
    try {
      const allProductIds = products.map(p => p.product_id)
      await checkExistingQueries(selectedJob.job_id, allProductIds)
    } finally {
      setIsRefreshingQueries(false)
    }
  }

  const submitProductSelection = async () => {
    if (selectedProducts.length === 0) {
      toast({
        title: "Error",
        description: "Please select at least one product",
        variant: "destructive",
      })
      return
    }

    if (!selectedJob) {
      toast({
        title: "Error",
        description: "Please select a job first",
        variant: "destructive",
      })
      return
    }

    setIsSubmittingProducts(true)
    try {
      await checkExistingQueries(selectedJob.job_id, selectedProducts)
    } finally {
      setIsSubmittingProducts(false)
    }
  }

  const checkExistingQueries = async (jobId: string, productIds: number[]) => {
    try {
      setIsLoadingQueries(true)
      const response = await fetch(`/api/queries/${jobId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ product_ids: productIds }),
      })
      const data = await response.json()
      if (Array.isArray(data) && data.length > 0) {
        setQueriesByProduct(data)
        setQueriesExist(true)
      } else {
        setQueriesByProduct([])
        setQueriesExist(false)
      }
    } catch (error) {
      setQueriesByProduct([])
      setQueriesExist(false)
    } finally {
      setIsLoadingQueries(false)
    }
  }

  const generateQueries = async () => {
    if (!selectedJob) return
    setIsGeneratingQueries(true)
    try {
      const response = await fetch("/api/generate-queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: selectedJob.job_id,
          product_ids: selectedProducts,
        }),
      })
      if (response.ok) {
        toast({
          title: "Success",
          description: "Query generation started!",
        })
        setTimeout(() => {
          checkExistingQueries(selectedJob.job_id, selectedProducts)
        }, 3000)
      } else {
        throw new Error("Failed to generate queries")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to generate queries",
        variant: "destructive",
      })
    } finally {
      setIsGeneratingQueries(false)
    }
  }

  const addCustomQuery = () => {
    if (!customQuery.trim()) return
    setNewQueries(prev => [...prev, customQuery.trim()])
    setCustomQuery("")
    toast({
      title: "Query Added",
      description: "Custom query has been added to your selection.",
    })
  }

  const processQueries = async () => {
    const selectedCustomQueriesText = selectedCustomQueries.map(id => {
      const customQuery = customQueriesAsObjects.find(q => q.query_id === id)
      return customQuery?.query_text || ''
    }).filter(text => text !== '')

    if (selectedQueries.length === 0 && selectedCustomQueriesText.length === 0) {
      toast({
        title: "Error",
        description: "Please select at least one query",
        variant: "destructive",
      })
      return
    }

    if (!selectedJob) return

    const productsWithSelectedQueries = new Set<number>()
    selectedQueries.forEach(queryId => {
      const query = allQueries.find(q => q.query_id === queryId)
      if (query && query.product_id) {
        productsWithSelectedQueries.add(query.product_id)
      }
    })
    if (selectedCustomQueriesText.length > 0) {
      if (selectedProductId) {
        productsWithSelectedQueries.add(selectedProductId)
      } else {
        products.forEach(p => productsWithSelectedQueries.add(p.product_id))
      }
    }

    setIsProcessingQueries(true)
    try {
      const response = await fetch("/api/process-queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: selectedJob.job_id,
          existing_query_ids: selectedQueries.length > 0 ? selectedQueries.map(id => Number(id)) : undefined,
          new_queries: selectedCustomQueriesText.length > 0 ? selectedCustomQueriesText : undefined,
          selected_products: Array.from(productsWithSelectedQueries),
        }),
      })
      if (response.ok) {
        const result = await response.json()
        toast({
          title: "Success",
          description: `Query processing started! (${result.total_queries} total queries) Redirecting to results page...`,
        })
        setTimeout(() => router.push("/results"), 1500)
      } else {
        throw new Error("Failed to process queries")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to process queries",
        variant: "destructive",
      })
    } finally {
      setIsProcessingQueries(false)
    }
  }

  // Queries for processing
  const allQueries = queriesByProduct.flatMap(pq => pq.queries)

  // Helpers for product display
  const getProductName = (productId: number) => {
    const product = products.find(p => p.product_id === productId)
    if (product && product.product_data) {
      let productData = product.product_data
      if (typeof productData === "string") {
        try {
          productData = JSON.parse(productData)
        } catch {
          return `Product ${productId}`
        }
      }
      return productData?.productname || productData?.name || productData?.title || `Product ${productId}`
    }
    return `Product ${productId}`
  }

  const getProductImage = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.image || productData?.image_url || productData?.thumbnail || productData?.photo || null
  }
  const getProductRating = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    const rating = productData?.rating || productData?.stars || productData?.score
    if (!rating || String(rating).toLowerCase() === "n/a") return null
    return String(rating)
  }
  const getSelectedProduct = () =>
    selectedProductId ? products.find(p => p.product_id === selectedProductId) : null
  const getProductSelectedQueriesCount = (productId: number) => {
    const productQueries = queriesByProduct.find(pq => pq.product_id === productId)?.queries || []
    return productQueries.filter(query => selectedQueries.includes(query.query_id)).length
  }
  const toggleQueryType = (queryType: string) => {
    setExpandedQueryTypes(prev => {
      const newSet = new Set(prev)
      if (newSet.has(queryType)) {
        newSet.delete(queryType)
      } else {
        newSet.add(queryType)
      }
      return newSet
    })
  }
  const toggleProductForBatch = (productId: number) => {
    setSelectedProductsForBatch(prev =>
      prev.includes(productId)
        ? prev.filter(id => id !== productId)
        : [...prev, productId]
    )
  }
  const selectAllProductsForBatch = () => setSelectedProductsForBatch(products.map(p => p.product_id))
  const clearAllProductsForBatch = () => setSelectedProductsForBatch([])
  const generateQueriesForBatch = async () => {
    if (!selectedJob) return
    const productIdsToUse = selectedProductsForBatch.length > 0
      ? selectedProductsForBatch
      : products.map(p => p.product_id)
    if (productIdsToUse.length === 0) {
      toast({
        title: "Error",
        description: "No products available to generate queries for",
        variant: "destructive",
      })
      return
    }
    setIsGeneratingQueries(true)
    try {
      const response = await fetch("/api/generate-queries", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          job_id: selectedJob.job_id,
          product_ids: productIdsToUse,
        }),
      })
      if (response.ok) {
        toast({
          title: "Success",
          description: `Batch query generation started for ${productIdsToUse.length} products!`,
        })
        setTimeout(() => {
          const allProductIds = products.map(p => p.product_id)
          checkExistingQueries(selectedJob.job_id, allProductIds)
        }, 3000)
        setSelectedProductsForBatch([])
      } else {
        throw new Error("Failed to generate queries")
      }
    } catch (error) {
      toast({
        title: "Error",
        description: "Failed to generate batch queries",
        variant: "destructive",
      })
    } finally {
      setIsGeneratingQueries(false)
    }
  }
  const filteredProducts = products.filter(product => {
    if (!productSearchTerm) return true
    const productName = getProductName(product.product_id).toLowerCase()
    return productName.includes(productSearchTerm.toLowerCase()) ||
           product.product_id.toString().includes(productSearchTerm)
  })

  const currentProductQueries = selectedProductId
    ? queriesByProduct.find(pq => pq.product_id === selectedProductId)?.queries || []
    : []

  const formatQueryTypeTitle = (type: string) =>
    type.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ') + ' Queries'

  // Convert custom queries to Query objects for consistent handling
  const customQueriesAsObjects: Query[] = newQueries.map((queryText, index) => ({
    query_id: -(index + 1), // Use negative IDs to avoid conflicts with existing queries
    product_id: null,
    query_text: queryText,
    query_type: "custom",
    is_active: true,
  }))

  // -- CONDITIONAL INITIAL DROPDOWN: Only show dropdown if not loading and no selected brand --
  if (!isLoading && !selectedBrand) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center gap-8">
        <div className="text-2xl font-semibold text-gray-800 mb-4">Select a brand to continue</div>
        <select
          value={selectedBrand}
          onChange={e => handleBrandSelect(e.target.value)}
          className="h-10 px-3 rounded-lg bg-white/60 border border-gray-200 min-w-52 text-lg"
        >
          <option value="">Select a brand...</option>
          {brands.map((brand) => (
            <option key={brand} value={brand}>{brand}</option>
          ))}
        </select>
      </div>
    )
  }

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
          <p>Loading...</p>
        </div>
      </div>
    )
  }

  // Else, show full UI
  return (
    <div className="h-screen flex flex-col">
      {/* HEADER */}
      <div className="px-6 md:px-8 py-6 border-b border-gray-200 dark:border-gray-700">
        <h1 className="text-3xl md:text-4xl font-semibold tracking-tight mb-2 bg-gradient-to-r from-[hsl(var(--foreground))] to-[hsl(var(--accent))] bg-clip-text text-transparent">
          Select Products & Queries
        </h1>
        <p className="text-muted-foreground text-sm">Choose products and create queries for AI analysis</p>
      </div>

      <div className="flex-1 overflow-hidden px-6 md:px-8">
        {brands.length === 0 ? (
          <div className="flex-1 flex items-center justify-center">
            <Card className="bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10">
              <CardContent className="p-8 text-center">
                <p className="text-muted-foreground mb-4">No successful jobs found. Please submit a URL first.</p>
                <p className="text-sm text-muted-foreground">Please submit a URL on the Home page to get started.</p>
              </CardContent>
            </Card>
          </div>
        ) : (
          <div className="flex flex-col h-full">
            {/* Combined Product & Query Selection - Takes full height */}
            <Card className="flex-1 flex flex-col bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10 overflow-hidden">
              <CardHeader className="pb-3 flex-shrink-0">
                <div className="space-y-4">
                  {/* Brand Selection Row */}
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-3">
                      <CardTitle className="text-lg whitespace-nowrap">Brand:</CardTitle>
                    <select
                      value={selectedBrand}
                      onChange={(e) => handleBrandSelect(e.target.value)}
                        className="h-10 px-3 rounded-lg bg-white/60 dark:bg-white/10 border border-white/60 dark:border-white/10 min-w-48"
                    >
                      <option value="">Select a brand...</option>
                      {brands.map((brand) => (
                        <option key={brand} value={brand}>
                          {brand}
                        </option>
                      ))}
                    </select>
                  </div>

                  {/* Show selected job info */}
                  {selectedJob && (
                      <div className="flex-1 text-center">
                        <p className="text-sm font-medium text-muted-foreground">
                          Scraped on {new Date(selectedJob.created_at).toLocaleDateString()}
                        </p>
                        <p className="text-xs text-muted-foreground truncate">
                          {selectedJob.source_url}
                        </p>
                    </div>
                  )}

                                        <div className="flex items-center gap-2">
                      {(selectedQueries.length > 0 || selectedCustomQueries.length > 0) && (
                        <Button 
                          onClick={processQueries} 
                          disabled={isProcessingQueries}
                          className="bg-blue-600 hover:bg-blue-700 text-white"
                        >
                          {isProcessingQueries ? (
                            <>
                              <Loader2 className="h-4 w-4 animate-spin mr-2" />
                              Processing...
                            </>
                          ) : (
                            <>
                              Process Queries ({selectedQueries.length + selectedCustomQueries.length})
                              <ArrowRight className="ml-2 h-4 w-4" />
                            </>
                          )}
                        </Button>
                      )}
                      
                      {selectedJob && (
                        <Button 
                          size="sm" 
                          onClick={generateQueriesForBatch}
                          disabled={isGeneratingQueries}
                          className="h-8 px-3 text-sm bg-green-600 hover:bg-green-700 text-white shadow-sm"
                        >
                          {isGeneratingQueries ? (
                            <Loader2 className="h-4 w-4 animate-spin mr-2" />
                          ) : null}
                          {selectedProductsForBatch.length > 0 
                            ? `Generate (${selectedProductsForBatch.length})` 
                            : 'Generate All Queries'
                          }
                        </Button>
                      )}
                      
                      <Button 
                        size="sm" 
                        variant="outline" 
                        onClick={async () => {
                          if (selectedJob) {
                            // If we have a selected job, reload products and queries for it
                            await fetchProducts(selectedJob.job_id)
                            const allProductIds = products.map(p => p.product_id)
                            if (allProductIds.length > 0) {
                              await checkExistingQueries(selectedJob.job_id, allProductIds)
                            }
                            toast({
                              title: "Reload Complete",
                              description: "Products and queries have been refreshed",
                            })
                          } else {
                            // If no job selected, just reload jobs
                            await fetchJobs()
                            toast({
                              title: "Reload Complete",
                              description: "Jobs have been refreshed",
                            })
                          }
                        }} 
                        disabled={isLoading} 
                        title="Reload products and queries for current job, or reload jobs if no job selected"
                        className="h-8 w-8 p-0"
                      >
                        {isLoading ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <RefreshCw className="h-4 w-4" />
                        )}
                      </Button>
                    </div>
                  </div>
                </div>
                </CardHeader>
                  <CardContent className="p-0 flex-1 flex flex-col min-h-0 overflow-hidden">
                  {isLoadingProducts ? (
                      <div className="flex-1 flex items-center justify-center">
                        <div className="text-center">
                      <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
                      <p>Loading products...</p>
                        </div>
                    </div>
                  ) : products.length === 0 ? (
                      <div className="flex-1 flex items-center justify-center">
                      <p className="text-muted-foreground">No products found for this job.</p>
                    </div>
                  ) : (
                      <div className="flex-1 flex min-h-0 overflow-hidden">
                        {/* Left Sidebar - Products */}
                        <div className="w-80 border-r border-gray-200 dark:border-gray-700 flex flex-col min-h-0 overflow-hidden">
                          {/* Search Bar & Batch Controls - Fixed */}
                          <div className="p-4 border-b border-gray-200 dark:border-gray-700 flex-shrink-0 space-y-3">
                            <div className="relative">
                              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4" />
                              <Input
                                placeholder="Search products..."
                                value={productSearchTerm}
                                onChange={(e) => setProductSearchTerm(e.target.value)}
                                className="pl-10 h-10 rounded-lg bg-white/60 dark:bg-white/10 border border-gray-200 dark:border-gray-600"
                        />
                      </div>
                            
                            {/* Batch Selection Controls */}
                            <div className="text-xs text-muted-foreground mb-2">
                            </div>
                            <div className="flex items-center justify-between text-xs">
                              <div className="flex items-center gap-2">
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  onClick={selectAllProductsForBatch}
                                  className="h-7 px-2 text-xs"
                                >
                                  Select All
                                </Button>
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  onClick={clearAllProductsForBatch}
                                  className="h-7 px-2 text-xs"
                                  disabled={selectedProductsForBatch.length === 0}
                                >
                                  Clear
                          </Button>
                        </div>
                            </div>
                          </div>
                          
                          {/* Products List - Scrollable */}
                          <div className="flex-1 min-h-0">
                            {isInitialQueriesLoading ? (
                              <div className="flex-1 flex items-center justify-center p-4">
                                <div className="text-center">
                                  <Loader2 className="h-6 w-6 animate-spin text-green-500 mx-auto mb-2" />
                                  <p className="text-xs text-muted-foreground">Loading queries...</p>
                                </div>
                              </div>
                            ) : (
                            <div className="h-full overflow-y-auto p-4 space-y-3">
                              {filteredProducts.map((product) => {
                                const hasSelectedQueries = getProductSelectedQueriesCount(product.product_id) > 0
                                const isSelectedForBatch = selectedProductsForBatch.includes(product.product_id)
                                const productQueries = queriesByProduct.find(pq => pq.product_id === product.product_id)?.queries || []
                                
                                return (
                                  <div
                                    key={product.product_id}
                                    className={`p-3 rounded-lg border transition-all duration-200 ${
                                      selectedProductId === product.product_id
                                        ? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20 shadow-md'
                                        : hasSelectedQueries
                                        ? 'border-green-400 bg-green-50 dark:bg-green-900/20 shadow-sm'
                                        : isSelectedForBatch
                                        ? 'border-purple-400 bg-purple-50 dark:bg-purple-900/20 shadow-sm'
                                        : 'border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600 hover:shadow-sm'
                                    }`}
                                  >
                                    <div className="flex gap-3">
                                      {/* Batch Selection Checkbox */}
                                      <div className="flex-shrink-0 flex items-start pt-1">
                                        <input
                                          type="checkbox"
                                          checked={isSelectedForBatch}
                                          onChange={(e) => {
                                            e.stopPropagation()
                                            toggleProductForBatch(product.product_id)
                                          }}
                                          className="w-4 h-4 text-purple-600 bg-gray-100 border-gray-300 rounded focus:ring-purple-500 dark:focus:ring-purple-600 dark:ring-offset-gray-800 focus:ring-2 dark:bg-gray-700 dark:border-gray-600"
                                        />
                                      </div>
                                      {/* Product Image */}
                                      <div className="flex-shrink-0">
                                        {getProductImage(product.product_data) ? (
                                          <img
                                            src={getProductImage(product.product_data)}
                                            alt={getProductName(product.product_id)}
                                            className="w-12 h-12 object-cover rounded-md border border-gray-200 dark:border-gray-600 cursor-pointer"
                                            onClick={() => handleProductClick(product.product_id)}
                                            onError={(e) => {
                                              e.currentTarget.style.display = 'none'
                                              e.currentTarget.nextElementSibling?.classList.remove('hidden')
                                            }}
                                          />
                                        ) : null}
                                        <div 
                                          className={`w-12 h-12 bg-gray-100 dark:bg-gray-800 rounded-md flex items-center justify-center cursor-pointer ${getProductImage(product.product_data) ? 'hidden' : ''}`}
                                          onClick={() => handleProductClick(product.product_id)}
                                        >
                                          <span className="text-xs text-gray-400 font-medium">
                                            {getProductName(product.product_id).charAt(0).toUpperCase()}
                                          </span>
                                        </div>
                                      </div>

                                      {/* Product Info */}
                                      <div className="flex-1 min-w-0 cursor-pointer" onClick={() => handleProductClick(product.product_id)}>
                                        <div className="flex items-start justify-between">
                                          <div className="flex-1 min-w-0">
                                            <h4 className="font-medium text-sm text-gray-900 dark:text-gray-100 truncate">
                                              {getProductName(product.product_id)}
                                            </h4>
                                            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
                                              ID: {product.product_id}
                                            </p>
                                            {/* {getProductPrice(product.product_data) && (
                                              <p className="text-xs font-medium text-green-600 dark:text-green-400 mt-1">
                                                {getProductPrice(product.product_data)}
                                              </p>
                                            )} */}
                                            {hasSelectedQueries && (
                                              <p className="text-xs font-medium text-green-600 dark:text-green-400 mt-1">
                                                {getProductSelectedQueriesCount(product.product_id)} selected
                                              </p>
                                            )}
                                          </div>
                                          
                                          {/* Right side info */}
                                          <div className="flex flex-col items-end gap-1 ml-2">
                                            {getProductRating(product.product_data) && (
                                              <div className="text-xs text-yellow-600 dark:text-yellow-400">
                                                ⭐ {getProductRating(product.product_data)}
                                              </div>
                                            )}
                                            <div className="text-xs text-blue-600 dark:text-blue-400">
                                              {productQueries.length} queries
                                            </div>
                                            
                                            {/* Selection Indicators */}
                                            <div className="flex gap-1">
                                              {selectedProductId === product.product_id && (
                                                <div className="h-2 w-2 bg-blue-500 rounded-full"></div>
                                              )}
                                              {hasSelectedQueries && (
                                                <div className="h-2 w-2 bg-green-500 rounded-full"></div>
                                              )}
                                              {isSelectedForBatch && (
                                                <div className="h-2 w-2 bg-purple-500 rounded-full"></div>
                                              )}
                                            </div>
                                          </div>
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                )
                              })}
                            </div>
                            )}
                          </div>
                      </div>

                        {/* Right Main Area - Queries */}
                        <div className="flex-1 flex flex-col min-h-0 overflow-hidden">
                          {!selectedProductId ? (
                            <div className="flex-1 flex items-center justify-center">
                              <div className="text-center">
                                <div className="w-16 h-16 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                                  <Search className="h-8 w-8 text-gray-400" />
                                </div>
                                <h3 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">
                                  Select a Product
                                </h3>
                                <div className="text-sm text-blue-600 dark:text-blue-400 bg-blue-50 dark:bg-blue-900/20 p-3 rounded-lg border border-blue-200 dark:border-blue-800">
                                  <p className="font-medium mb-1">💡 Query Generation</p>
                                  <p>Use the "Generate All Queries" button above to create queries for all products, or select specific products in the sidebar for targeted generation.</p>
                                </div>
                              </div>
                            </div>
                          ) : (
                            <div className="flex-1 flex flex-col min-h-0">
                                                                                                                        {/* Product Header - Fixed */}
                              <div className="p-6 border-b border-gray-200 dark:border-gray-700 flex-shrink-0">
                                <div className="flex items-start justify-between">
                                  {/* Product Info */}
                                  <div className="flex-1">
                                    <div className="flex items-start justify-between">
                                      <div className="flex-1">
                                        <h3 className="text-xl font-semibold text-gray-900 dark:text-gray-100">
                                          {getProductName(selectedProductId)}
                                        </h3>
                                        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
                                          Product ID: {selectedProductId}
                                        </p>
                                      </div>
                                      
                                      {/* Right side product info */}
                                      <div className="flex flex-col items-end gap-1 ml-4">
                                      {/* {getProductPrice(getSelectedProduct()?.product_data) && (
                                          <p className="text-sm font-medium text-green-600 dark:text-green-400 mt-1">
                                            {getProductPrice(getSelectedProduct()?.product_data)}
                                          </p>
                                        )} */}
                                        {getProductRating(getSelectedProduct()?.product_data) && (
                                          <div className="text-sm text-yellow-600 dark:text-yellow-400">
                                            Rating: ⭐ {getProductRating(getSelectedProduct()?.product_data)}
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                  </div>


                                </div>
                              </div>

                              {/* Queries Content - Scrollable */}
                              <div className="flex-1 min-h-0 overflow-hidden flex flex-col">
                                <div className="flex-1 overflow-y-auto p-6">
                              {isInitialQueriesLoading ? (
                                <div className="text-center py-12">
                                  <div className="w-16 h-16 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                                    <Loader2 className="h-8 w-8 animate-spin text-green-500" />
                                  </div>
                                  <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">
                                    Loading Queries
                                  </h4>
                                  <p className="text-gray-500 dark:text-gray-400 max-w-sm mx-auto">
                                    Fetching queries for all products...
                                  </p>
                                </div>
                              ) : isLoadingQueries ? (
                                <div className="text-center py-12">
                                  <div className="w-16 h-16 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                                    <Loader2 className="h-8 w-8 animate-spin text-blue-500" />
                                  </div>
                                  <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">
                                    Loading from Database
                                  </h4>
                                  <p className="text-gray-500 dark:text-gray-400 max-w-sm mx-auto">
                                    Fetching existing queries for the selected products...
                                  </p>
                                </div>
                              ) : currentProductQueries.length === 0 ? (
                                <div className="text-center py-12">
                                  <div className="w-16 h-16 bg-gray-100 dark:bg-gray-800 rounded-full flex items-center justify-center mx-auto mb-4">
                                    <ArrowRight className="h-8 w-8 text-gray-400" />
                                  </div>
                                  <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100 mb-2">
                                    No Queries Available
                                  </h4>
                    </div>
                  ) : (
                                                                <div className="space-y-4">
                                  {/* Group queries by type - Accordion Format */}
                                  {(() => {
                                    const groupedByType = currentProductQueries.reduce(
                                      (acc, query) => {
                                        const type = query.query_type || "other"
                                        if (!acc[type]) acc[type] = []
                                        acc[type].push(query)
                                        return acc
                                      },
                                      {} as Record<string, Query[]>,
                                    )

                                    return Object.entries(groupedByType).map(([type, typeQueries]) => {
                                      const isExpanded = expandedQueryTypes.has(type)
                                      const selectedCount = typeQueries.filter(q => selectedQueries.includes(q.query_id)).length
                                      
                                      return (
                                        <div key={type} className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                                          {/* Accordion Header */}
                                          <button
                                            onClick={() => toggleQueryType(type)}
                                            className="w-full px-4 py-3 bg-gray-50 dark:bg-gray-800 hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors duration-200 flex items-center justify-between text-left"
                                          >
                                            <div className="flex items-center gap-3">
                                              {isExpanded ? (
                                                <ChevronDown className="h-4 w-4 text-gray-500" />
                                              ) : (
                                                <ChevronRight className="h-4 w-4 text-gray-500" />
                                              )}
                                              <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100">
                            {formatQueryTypeTitle(type)}
                                              </h4>
                                            </div>
                                            <div className="flex items-center gap-2">
                                              {selectedCount > 0 && (
                                                <Badge variant="secondary" className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                                                  {selectedCount} selected
                                                </Badge>
                                              )}
                                              <Badge variant="outline">
                                                {typeQueries.length} queries
                                              </Badge>
                                            </div>
                                          </button>
                                          
                                          {/* Accordion Content */}
                                          {isExpanded && (
                                            <div className="p-4 bg-white dark:bg-gray-900">
                          <QuerySelector
                            queries={typeQueries}
                            selectedQueries={selectedQueries}
                            onSelectionChange={setSelectedQueries}
                            disabled={false}
                          />
                        </div>
                                          )}
                                        </div>
                                      )
                                    })
                                                                    })()}

                      {/* Custom Queries Section */}
                      {customQueriesAsObjects.length > 0 && (
                                    <div className="space-y-4 mt-6">
                                      <div className="border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                                        {/* Custom Queries Header */}
                                        <div className="px-4 py-3 bg-gray-50 dark:bg-gray-800 flex items-center justify-between">
                                          <h4 className="text-lg font-medium text-gray-900 dark:text-gray-100">Custom Queries</h4>
                                          <div className="flex items-center gap-2">
                                            <Badge variant="secondary" className="bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200">
                                              {selectedCustomQueries.length} selected
                                            </Badge>
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => {
                                setNewQueries([])
                                setSelectedCustomQueries([])
                              }}
                                              className="text-red-600 hover:text-red-800 hover:bg-red-100 dark:text-red-400 dark:hover:text-red-300 dark:hover:bg-red-900/20 h-6 px-2 text-xs"
                            >
                                              Clear All
                            </Button>
                                          </div>
                                        </div>
                                        
                                        {/* Custom Queries Content */}
                                        <div className="p-4 bg-white dark:bg-gray-900">
                                          <QuerySelector
                                            queries={customQueriesAsObjects}
                                            selectedQueries={selectedCustomQueries}
                                            onSelectionChange={setSelectedCustomQueries}
                                            disabled={false}
                                          />
                                        </div>
                          </div>
                        </div>
                      )}
                                </div>
                              )}
                                </div>

                                {/* Fixed Bottom Section - Custom Query Input */}
                                <div className="border-t border-gray-200 dark:border-gray-700 p-4 bg-gray-50 dark:bg-gray-800 flex-shrink-0">
                        <div className="flex gap-2">
                          <Input
                            placeholder="Add custom query..."
                            value={customQuery}
                            onChange={(e) => setCustomQuery(e.target.value)}
                            onKeyPress={(e) => e.key === 'Enter' && addCustomQuery()}
                                      className="h-10 rounded-lg bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-600"
                          />
                                    <Button onClick={addCustomQuery} variant="outline" size="sm">
                            Add Query
                          </Button>
                        </div>
                          </div>
                              </div>
                            </div>
                          )}
                        </div>
                      </div>
                  )}
                </CardContent>
              </Card>
          </div>
        )}
        </div>
    </div>
  )
}

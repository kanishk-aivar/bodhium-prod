"use client"

import { useState, useEffect } from "react"
import { useRouter } from "next/navigation"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Loader2, RefreshCw, Search, ExternalLink, Package } from "lucide-react"
import { useToast } from "../hooks/use-toast"
import type { Product } from "../lib/types"

export default function ProductsPage() {
  const [products, setProducts] = useState<Product[]>([])
  const [filteredProducts, setFilteredProducts] = useState<Product[]>([])
  const [brands, setBrands] = useState<string[]>([])
  const [searchTerm, setSearchTerm] = useState("")
  const [brandFilter, setBrandFilter] = useState<string>("all")
  const [isLoading, setIsLoading] = useState(true)

  const { toast } = useToast()
  const router = useRouter()

  useEffect(() => {
    fetchAllProducts()
  }, [])

  useEffect(() => {
    applyFilters()
  }, [products, searchTerm, brandFilter])

  const fetchAllProducts = async () => {
    try {
      setIsLoading(true)
      
      // First fetch all successful jobs
      const jobsResponse = await fetch("/api/jobs")
      const jobs = await jobsResponse.json()

      if (!Array.isArray(jobs)) {
        throw new Error("Invalid jobs response")
      }

      const successfulJobs = jobs.filter((job: any) => job.status === "JOB_SUCCESS")
      
      // Extract unique brands from jobs (same logic as queries page)
      const uniqueBrands = Array.from(new Set(
        successfulJobs
          .map((job: any) => job.brand_name)
          .filter((brand): brand is string => Boolean(brand))
      )).sort()
      setBrands(uniqueBrands)
      
      // Fetch products from successful jobs that have brand names
      const allProducts: Product[] = []
      const jobsWithBrands = successfulJobs.filter((job: any) => job.brand_name)
      for (const job of jobsWithBrands) {
        try {
          const productsResponse = await fetch(`/api/products/${job.job_id}`)
          const jobProducts = await productsResponse.json()
          if (Array.isArray(jobProducts)) {
            // Ensure each product has the brand_name from the job (prioritize job brand name)
            const productsWithBrand = jobProducts.map((product: Product) => ({
              ...product,
              brand_name: job.brand_name || product.brand_name
            }))
            allProducts.push(...productsWithBrand)
          }
        } catch (error) {
          console.error(`Error fetching products for job ${job.job_id}:`, error)
        }
      }

      setProducts(allProducts)
    } catch (error) {
      console.error("Error fetching products:", error)
      toast({
        title: "Error",
        description: "Failed to fetch products",
        variant: "destructive",
      })
      setProducts([])
      setBrands([])
    } finally {
      setIsLoading(false)
    }
  }

  const applyFilters = () => {
    let filtered = products

    // Apply search filter (only product names)
    if (searchTerm.trim()) {
      filtered = filtered.filter((product) => {
        const productData = getProductData(product.product_data)
        const name = getProductName(productData).toLowerCase()
        const search = searchTerm.toLowerCase()
        
        return name.includes(search)
      })
    }

    // Apply brand filter
    if (brandFilter !== "all") {
      filtered = filtered.filter((product) => product.brand_name === brandFilter)
    }

    setFilteredProducts(filtered)
  }

  const getProductData = (productData: any) => {
    if (typeof productData === "string") {
      try {
        return JSON.parse(productData)
      } catch {
        return {}
      }
    }
    return productData || {}
  }

  const getProductName = (productData: any) => {
    return productData?.productname || productData?.name || productData?.title || "Unknown Product"
  }

  // const getProductPrice = (productData: any) => {
  //   return productData?.current_price || productData?.price || productData?.cost || null
  // }

  const getUniquebrands = () => {
    return brands
  }

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
          <p>Loading products...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="container mx-auto px-6 md:px-8 py-12 max-w-7xl">
        <div className="mb-10">
          <h1 className="text-4xl md:text-5xl font-semibold tracking-tight mb-3 bg-gradient-to-r from-[hsl(var(--foreground))] to-[hsl(var(--accent))] bg-clip-text text-transparent">
            Products Database
          </h1>
          <p className="text-muted-foreground text-base">Browse all scraped products from your jobs</p>
        </div>

        {/* Filters */}
        <Card className="mb-6 bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Search className="h-5 w-5" />
              Search & Filter
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex gap-4">
              <div className="flex-1">
                <Input
                  placeholder="Search products by name..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="h-11 rounded-xl bg-white/60 dark:bg-white/10 border border-white/60 dark:border-white/10"
                />
              </div>
              <div className="w-48">
                <select
                  value={brandFilter}
                  onChange={(e) => setBrandFilter(e.target.value)}
                  className="w-full h-11 px-3 rounded-xl bg-white/60 dark:bg-white/10 border border-white/60 dark:border-white/10"
                >
                  <option value="all">All Brands</option>
                  {getUniquebrands().map((brand) => (
                    <option key={brand} value={brand}>
                      {brand}
                    </option>
                  ))}
                </select>
              </div>
              <Button variant="outline" onClick={() => applyFilters()}>
                <Search className="h-4 w-4" />
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Results Summary */}
        <Card className="mb-6 bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-4">
                <div className="flex items-center gap-2">
                  <Package className="h-4 w-4 text-muted-foreground" />
                  <span className="text-sm font-medium">
                    {filteredProducts.length} of {products.length} products
                  </span>
                </div>
                {searchTerm && (
                  <Badge variant="secondary">
                    Product name: "{searchTerm}"
                  </Badge>
                )}
                {brandFilter !== "all" && (
                  <Badge variant="secondary">
                    Brand: {brandFilter}
                  </Badge>
                )}
              </div>
              <Button variant="outline" size="sm" onClick={() => router.push("/queries")}>
                Go to Queries to Select Products
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Products Table */}
        <Card className="bg-white/60 backdrop-blur supports-[backdrop-filter]:bg-white/50 border border-white/60 shadow-lg dark:bg-white/5 dark:border-white/10">
          <CardContent className="p-0">
            {filteredProducts.length === 0 ? (
              <div className="p-8 text-center">
                <Package className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
                <h3 className="text-lg font-medium mb-2">No Products Found</h3>
                <p className="text-muted-foreground">
                  {products.length === 0 
                    ? "No products have been scraped yet. Submit a URL on the home page to get started."
                    : "No products match your current filters. Try adjusting your search criteria."
                  }
                </p>
              </div>
            ) : (
              <div className="w-full">
                <Table>
                  <TableHeader>
                    <TableRow className="border-white/20">
                      <TableHead className="w-16">ID</TableHead>
                      <TableHead className="min-w-0">Product Name</TableHead>
                      <TableHead className="w-32">Brand</TableHead>
                      {/* <TableHead className="w-24">Price</TableHead> */}
                      <TableHead className="w-12"></TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {filteredProducts.map((product) => {
                      const productData = getProductData(product.product_data)
                      const name = getProductName(productData)
                      // const price = getProductPrice(productData)
                      
                      return (
                        <TableRow key={product.product_id} className="border-white/20 hover:bg-white/20">
                          <TableCell className="font-mono text-xs text-muted-foreground w-16">
                            {product.product_id}
                          </TableCell>
                          <TableCell className="font-medium min-w-0">
                            <div className="break-words hyphens-auto leading-tight" title={name}>
                              {name}
                            </div>
                          </TableCell>
                          <TableCell className="w-32">
                            {product.brand_name ? (
                              <Badge variant="secondary" className="text-xs whitespace-nowrap">
                                {product.brand_name}
                              </Badge>
                            ) : (
                              <span className="text-muted-foreground text-xs">Unknown</span>
                            )}
                          </TableCell>
                          {/* <TableCell className="w-24">
                            {price ? (
                              <span className="font-medium text-sm whitespace-nowrap">{price}</span>
                            ) : (
                              <span className="text-muted-foreground text-xs">-</span>
                            )}
                          </TableCell> */}
                          <TableCell className="w-12">
                            {product.source_url && (
                              <Button variant="ghost" size="sm" asChild>
                                <a href={product.source_url} target="_blank" rel="noopener noreferrer">
                                  <ExternalLink className="h-3 w-3" />
                                </a>
                              </Button>
                            )}
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
    </div>
  )
}

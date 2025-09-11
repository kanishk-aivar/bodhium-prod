"use client"

import { useState } from "react"
import { Card, CardContent } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Input } from "@/components/ui/input"
import { Star, ExternalLink, Search } from "lucide-react"
import type { Product } from "../lib/types"

interface ProductSelectorProps {
  products: Product[]
  selectedProducts: number[]
  onSelectionChange: (selected: number[]) => void
  disabled?: boolean
}

export default function ProductSelector({
  products,
  selectedProducts,
  onSelectionChange,
  disabled = false,
}: ProductSelectorProps) {
  const [searchTerm, setSearchTerm] = useState("")

  const toggleProduct = (productId: number) => {
    if (disabled) return

    if (selectedProducts.includes(productId)) {
      onSelectionChange(selectedProducts.filter((id) => id !== productId))
    } else {
      onSelectionChange([...selectedProducts, productId])
    }
  }

  const getProductName = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return "Unknown Product"
      }
    }
    // Fix: Use productname instead of name
    return productData?.productname || productData?.name || productData?.title || "Unknown Product"
  }

  const getProductPrice = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.price || productData?.cost || null
  }

  const getProductCurrentPrice = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return (
      productData?.current_price ||
      productData?.price ||
      productData?.cost ||
      null
    )
  }

  const getProductOriginalPrice = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.original_price || productData?.mrp || null
  }

  const getProductRating = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    const rating = productData?.rating
    if (!rating || String(rating).toLowerCase() === "n/a") return null
    return String(rating)
  }

  const parseNumericRating = (rating: string | null): number | null => {
    if (!rating) return null
    const match = String(rating).match(/\d+(\.\d+)?/)
    if (!match) return null
    const value = Math.max(0, Math.min(5, parseFloat(match[0])))
    return isNaN(value) ? null : value
  }

  const getProductDescription = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.description || productData?.desc || null
  }

  const getProductSourceUrl = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.source_url || productData?.url || null
  }

  const getProductImage = (productData: any) => {
    if (typeof productData === "string") {
      try {
        productData = JSON.parse(productData)
      } catch {
        return null
      }
    }
    return productData?.image || productData?.image_url || null
  }

  // Filter products based on search term
  const filteredProducts = products.filter((product) => {
    const productName = getProductName(product.product_data).toLowerCase()
    const brandName = product.brand_name?.toLowerCase() || ""
    const searchLower = searchTerm.toLowerCase()
    
    return productName.includes(searchLower) || brandName.includes(searchLower)
  })

  const selectAll = () => {
    if (disabled) return
    // Select all filtered products
    const filteredIds = filteredProducts.map((p) => p.product_id)
    const newSelection = [...Array.from(new Set([...selectedProducts, ...filteredIds]))]
    onSelectionChange(newSelection)
  }

  const selectNone = () => {
    if (disabled) return
    // Deselect all filtered products
    const filteredIds = new Set(filteredProducts.map((p) => p.product_id))
    const newSelection = selectedProducts.filter(id => !filteredIds.has(id))
    onSelectionChange(newSelection)
  }

  return (
    <div className="space-y-4">
      {/* Search Bar */}
      <div className="relative p-1">
        <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 h-4 w-4 text-muted-foreground z-10" />
        <Input
          placeholder="Search products by name or brand..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="pl-10 h-11 rounded-xl bg-white/60 dark:bg-white/10 backdrop-blur-md border border-white/60 dark:border-white/10 placeholder:text-muted-foreground/70 focus-visible:ring-2 focus-visible:ring-[hsl(var(--accent))]/50 focus-visible:border-[hsl(var(--accent))]/50 focus-visible:ring-offset-0"
        />
      </div>

      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">
            {selectedProducts.length} of {products.length} products selected
            {searchTerm && (
              <span className="ml-2 text-xs">
                ({filteredProducts.length} shown)
              </span>
            )}
          </span>
        </div>
        {!disabled && (
          <div className="flex gap-2">
            <Button size="sm" variant="outline" onClick={selectAll}>
              Select All {searchTerm && `Filtered`}
            </Button>
            <Button size="sm" variant="outline" onClick={selectNone}>
              Deselect All {searchTerm && `Filtered`}
            </Button>
          </div>
        )}
      </div>

      {filteredProducts.length === 0 && searchTerm ? (
        <div className="text-center py-8">
          <Search className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
          <p className="text-muted-foreground">No products found matching "{searchTerm}"</p>
          <Button 
            variant="outline" 
            size="sm" 
            onClick={() => setSearchTerm("")}
            className="mt-2"
          >
            Clear Search
          </Button>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 overflow-x-hidden p-2">
          {filteredProducts.map((product) => {
          const isSelected = selectedProducts.includes(product.product_id)
          const productName = getProductName(product.product_data)
          const productCurrentPrice = getProductCurrentPrice(product.product_data)
          const productOriginalPrice = getProductOriginalPrice(product.product_data)
          const productImage = getProductImage(product.product_data)
          const productRating = getProductRating(product.product_data)
          const numericRating = parseNumericRating(productRating)
          const productDescription = getProductDescription(product.product_data)
          const productSourceUrl = getProductSourceUrl(product.product_data)

          return (
            <Card
              key={product.product_id}
              className={`w-full rounded-xl border border-white/60 bg-white/60 backdrop-blur-md shadow-sm transition-colors ${
                isSelected
                  ? "ring-2 ring-[hsl(var(--accent))] bg-white/70 dark:bg-[hsl(var(--accent))]/10"
                  : "hover:bg-white/70 dark:bg-white/5 dark:border-white/10"
              }`}
            >
              <CardContent className="p-0">
                <div className="relative">
                  {productImage ? (
                    <img
                      src={productImage}
                      alt={productName}
                      className="w-full h-40 object-cover rounded-t-xl border-b border-white/60"
                      onError={(e) => {
                        ;(e.target as HTMLImageElement).src = "/placeholder.svg?height=160&width=320"
                      }}
                    />
                  ) : (
                    <div className="w-full h-40 rounded-t-xl border-b border-white/60 dark:border-white/10 bg-muted dark:bg-white/10" />
                  )}
                  <div className="absolute top-2 left-2">
                    <Checkbox
                      checked={isSelected}
                      onCheckedChange={() => toggleProduct(product.product_id)}
                      disabled={disabled}
                      className="bg-white/70 backdrop-blur rounded-md"
                    />
                  </div>
                  {productRating && (
                    <div className="absolute top-2 right-2 rounded-md bg-black/50 text-white text-xs px-2 py-0.5 flex items-center gap-1">
                      <Star className="h-3 w-3 fill-current" />
                      <span>{productRating}</span>
                    </div>
                  )}
                </div>
                <div className="p-4 space-y-2">
                  <h3 className="font-medium line-clamp-2">{productName}</h3>
                  <p className="text-sm text-muted-foreground line-clamp-1">{product.brand_name}</p>
                  <div className="flex items-center gap-2">
                    {numericRating !== null ? (
                      <div className="flex items-center gap-1" aria-label={`Rating ${numericRating} out of 5`}>
                        {Array.from({ length: 5 }).map((_, idx) => {
                          const filled = idx < Math.round(numericRating)
                          return (
                            <Star
                              key={idx}
                              className={
                                "h-3.5 w-3.5 " + (filled ? "text-yellow-500 fill-current" : "text-muted-foreground/40")
                              }
                            />
                          )
                        })}
                        <span className="text-xs text-muted-foreground">{numericRating.toFixed(1)}/5</span>
                      </div>
                    ) : (
                      <span className="text-xs text-muted-foreground">rating not available</span>
                    )}
                  </div>
                  {productDescription && (
                    <p className="text-sm text-muted-foreground line-clamp-2">{productDescription}</p>
                  )}
                  {productSourceUrl && (
                    <a
                      href={productSourceUrl}
                      target="_blank"
                      rel="noreferrer noopener"
                      className="inline-flex items-center gap-1 text-xs text-foreground/70 hover:underline"
                    >
                      <ExternalLink className="h-3 w-3" /> Source
                    </a>
                  )}
                  <div className="flex items-center justify-between">
                    {productCurrentPrice ? (
                      <div className="flex items-center gap-2">
                        <Badge variant="secondary">{productCurrentPrice}</Badge>
                        {productOriginalPrice && productOriginalPrice !== productCurrentPrice ? (
                          <span className="text-xs text-muted-foreground line-through">{productOriginalPrice}</span>
                        ) : null}
                      </div>
                    ) : (
                      <span />
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          )
        })}
        </div>
      )}
    </div>
  )
}

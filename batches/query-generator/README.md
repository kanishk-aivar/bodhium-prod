# Query Generator Batch Job

This batch job generates product-specific and market-specific questions using Google's Gemini AI API.

## Recent Updates

### 1. Simplified Product Data Input
- **Before**: Required complete product records with nested `product_data`
- **After**: Now accepts only the `product_data` JSON directly
- **Benefit**: Cleaner input format, easier to work with

### 2. Updated Gemini API Usage
- **Before**: Used `google.generativeai` package with `GenerativeModel`
- **After**: Uses `google.genai` package with new `Client` pattern
- **Benefit**: Latest API version, better performance, more features

## Input Format

### New Format (Recommended)
```json
[
    {
        "rating": "N/A",
        "review": "N/A",
        "category": "Kids Sale Shoes",
        "description": "Older Kids' Shoes",
        "productname": "Jordan Spizike Low",
        "availability": "In Stock",
        "current_price": "₹ 8,827.00",
        "original_price": "N/A"
    }
]
```

### Legacy Format (Still Supported)
```json
[
    {
        "product_id": "1458",
        "product_data": {
            "rating": "N/A",
            "review": "N/A",
            "category": "Kids Sale Shoes",
            "image_url": "https://www.nike.com/in/t/jordan-spizike-low-older-shoes-40FbDq/FQ3950-103",
            "source_url": "https://www.nike.com/in/",
            "description": "Older Kids' Shoes",
            "productname": "Jordan Spizike Low",
            "availability": "In Stock",
            "extracted_at": "2025-08-23T14:59:13.766562",
            "current_price": "₹ 8,827.00",
            "original_price": "N/A"
        },
        "source_url": "https://www.nike.com/in/",
        "first_scraped_at": "2025-08-23T14:59:14.251Z",
        "brand_name": "www.nike.com"
    }
]
```

## Usage

### Environment Variables
```bash
export GEMINI_API_KEY="your_api_key_here"
export PRODUCTS_JSON='[{"rating":"N/A","review":"N/A","category":"Kids Sale Shoes","description":"Older Kids\' Shoes","productname":"Jordan Spizike Low","availability":"In Stock","current_price":"₹ 8,827.00","original_price":"N/A"}]'
export NUM_QUESTIONS=25
```

### Command Line
```bash
python batch_job.py --products-file products.json --job-id test_job_001
```

### S3 Input
```bash
export INPUT_S3_BUCKET="my-bucket"
export INPUT_S3_KEY="products/products.json"
python batch_job.py
```

## Dependencies

- `google-genai>=0.1.0` - New Google AI client
- `boto3>=1.34.0` - AWS SDK
- `psycopg[binary]>=3.1.0` - PostgreSQL adapter

## Testing

Run the test script to verify the new implementation:

```bash
export GEMINI_API_KEY="your_api_key_here"
python test_new_format.py
```

## Output

The batch job generates two types of questions:
1. **Product-specific questions**: About the specific product features, benefits, usage
2. **Market-specific questions**: About the product's market position, reputation, ranking

Questions are saved to both:
- PostgreSQL database (for immediate use)
- S3 (for backup and analysis)

## Performance

- **Parallel processing**: Uses ThreadPoolExecutor for multiple products
- **Lazy imports**: Heavy dependencies loaded only when needed
- **API key caching**: Secrets Manager calls minimized
- **Timeout handling**: Configurable thread timeouts

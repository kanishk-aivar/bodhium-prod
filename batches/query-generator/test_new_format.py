#!/usr/bin/env python3
"""
Test script for the new product_data format and Gemini API usage
"""

import json
import os
from google import genai
from google.genai import types

# Sample product_data from user
SAMPLE_PRODUCT_DATA = {
    "rating": "N/A",
    "review": "N/A",
    "category": "Kids Sale Shoes",
    "description": "Older Kids' Shoes",
    "productname": "Jordan Spizike Low",
    "availability": "In Stock",
    "current_price": "₹ 8,827.00",
    "original_price": "N/A"
}

def test_gemini_api():
    """Test the new Gemini API pattern"""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY environment variable not set")
        return False
    
    try:
        client = genai.Client(api_key=api_key)
        model = "gemini-2.5-flash"
        
        # Test prompt
        test_prompt = f"""You are a product analyst assistant. 
Based on the following product information, generate 3 intelligent, varied, and helpful 
natural-language questions a user might ask **about this specific product**.

Product Information:
{json.dumps(SAMPLE_PRODUCT_DATA, indent=2)}

Output: numbered list of 3 unique questions.
You should mention the product name and brand name in the question."""

        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_text(text=test_prompt),
                ],
            ),
        ]
        
        generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_budget=0,
            ),
            response_mime_type="application/json",
        )

        print("Testing Gemini API with new pattern...")
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=generate_content_config,
        )
        
        print("✅ Gemini API test successful!")
        print(f"Response: {response.text}")
        return True
        
    except Exception as e:
        print(f"❌ Gemini API test failed: {e}")
        return False

def test_product_data_format():
    """Test the product_data format processing"""
    print("Testing product_data format...")
    
    # Simulate what the generate_questions function would do
    summary = "\n".join(f"{k}: {v}" for k, v in SAMPLE_PRODUCT_DATA.items())
    print(f"Product summary length: {len(summary)} characters")
    print(f"Product name: {SAMPLE_PRODUCT_DATA.get('productname', 'unknown')}")
    print(f"Category: {SAMPLE_PRODUCT_DATA.get('category', 'unknown')}")
    print(f"Price: {SAMPLE_PRODUCT_DATA.get('current_price', 'unknown')}")
    print(f"Description: {SAMPLE_PRODUCT_DATA.get('description', 'unknown')}")
    
    print("✅ Product data format test successful!")
    return True

def main():
    """Main test function"""
    print("🧪 Testing new query generator implementation...")
    print("=" * 50)
    
    # Test 1: Product data format
    test_product_data_format()
    print()
    
    # Test 2: Gemini API
    test_gemini_api()
    print()
    
    print("=" * 50)
    print("🎯 Test completed!")

if __name__ == "__main__":
    main()

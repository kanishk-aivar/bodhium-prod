import os
import tempfile
import uuid
import shutil
import atexit
import asyncio
import time
import json
import boto3
from datetime import datetime, timezone
from typing import List
import re
import random
from playwright.async_api import async_playwright
import logging
import psycopg2
from botocore.exceptions import ClientError

# Add this at the top of your file after the imports
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# CRITICAL FIX: Set up ephemeral storage and environment variables BEFORE any crawl4ai imports
def setup_crawl4ai_environment():
    """Setup crawl4ai environment variables before importing crawl4ai"""
    instance_id = str(uuid.uuid4())
    ephemeral_base = tempfile.mkdtemp(prefix=f'crawl4ai_{instance_id}_')
    
    # Create all necessary subdirectories for crawl4ai
    subdirs = ['cache', 'models', 'html_content', 'cleaned_html', 'markdown_content',
               'extracted_content', 'screenshots', '.crawl4ai', '.crawl4ai/robots']
    for subdir in subdirs:
        os.makedirs(os.path.join(ephemeral_base, subdir), exist_ok=True)
    
    # Set ALL possible environment variable variations BEFORE importing crawl4ai
    env_vars = {
        'CRAWL4AI_BASE_DIRECTORY': ephemeral_base,
        'CRAWL4_AI_BASE_DIRECTORY': ephemeral_base,
        'CRAWL4AI_DB_PATH': os.path.join(ephemeral_base, 'crawl4ai.db'),
        'CRAWL4AI_CACHE_DIR': os.path.join(ephemeral_base, 'cache'),
        'CRAWL4AI_MODELS_DIR': os.path.join(ephemeral_base, 'models'),
        'CRAWL4AI_HOME': ephemeral_base,
        'CRAWL4AI_CONFIG_DIR': ephemeral_base,
        'HOME': ephemeral_base
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
    
    print("DEBUG: Using ephemeral crawl4ai storage")
    print("DEBUG: CRAWL4AI_BASE_DIRECTORY:", os.environ.get('CRAWL4AI_BASE_DIRECTORY'))
    print("DEBUG: CRAWL4AI_DB_PATH:", os.environ.get('CRAWL4AI_DB_PATH'))
    print("DEBUG: Instance ID:", instance_id)
    
    def cleanup_ephemeral_storage():
        try:
            if os.path.exists(ephemeral_base):
                shutil.rmtree(ephemeral_base)
                print(f"Cleaned up ephemeral storage: {ephemeral_base}")
        except Exception as e:
            print(f"Cleanup error: {e}")
    
    atexit.register(cleanup_ephemeral_storage)
    return ephemeral_base, instance_id


# Call this FIRST, before any other imports
ephemeral_base, instance_id = setup_crawl4ai_environment()


# Import crawl4ai AFTER environment setup
try:
    from crawl4ai import AsyncWebCrawler
    print("crawl4ai imported successfully")
    CRAWL4AI_AVAILABLE = True
except Exception as e:
    print(f"crawl4ai import failed: {e}")
    print("Continuing without crawl4ai - will use basic HTML extraction")
    CRAWL4AI_AVAILABLE = False


# Bright Data Configuration
# Fetch credentials securely from AWS Secrets Manager
def get_brightdata_auth():
    """Get Bright Data credentials from AWS Secrets Manager"""
    secret_name = "BRIGHTDATA_AUTH"
    region_name = "us-east-1"
    
    try:
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )
        
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        
        secret_string = get_secret_value_response['SecretString']
        
        # Handle both JSON and plain string formats
        try:
            # Try to parse as JSON first
            secret_json = json.loads(secret_string)
            # If it's JSON, extract the BRIGHTDATA_AUTH value
            if isinstance(secret_json, dict) and 'BRIGHTDATA_AUTH' in secret_json:
                return secret_json['BRIGHTDATA_AUTH']
            else:
                return secret_string
        except json.JSONDecodeError:
            # If it's not JSON, return as-is
            return secret_string
        
    except ClientError as e:
        print(f"Error fetching Bright Data credentials from Secrets Manager: {e}")
        raise e

# Get credentials and build WebSocket URL
try:
    AUTH = get_brightdata_auth()
    SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222'
    print("✅ Bright Data credentials loaded from Secrets Manager")
except Exception as e:
    print(f"❌ Failed to load Bright Data credentials: {e}")
    raise




async def extract_chatgpt_response_html_and_process(page, query, timestamp):
    """Extract ChatGPT response HTML and process it with Crawl4AI for proper markdown with clickable links"""
    
    # Extract ONLY the ChatGPT response HTML (not full page)
    print("🎯 Extracting ChatGPT response HTML...")
    response_html = ""
    
    try:
        response_elements = await page.query_selector_all('[data-message-author-role="assistant"]')
        if response_elements:
            # Get the last (most recent) response HTML
            response_html = await response_elements[-1].inner_html()
            print(f" Extracted ChatGPT response HTML ({len(response_html)} characters)")
        else:
            print("No assistant response elements found")
            return "", []
    except Exception as e:
        print(f"Error extracting response HTML: {e}")
        return "", []
    
    if not response_html:
        print("No response HTML content found")
        return "", []
    
    # Process the response HTML with Crawl4AI
    if CRAWL4AI_AVAILABLE:
        try:
            print("🔧 Processing ChatGPT response HTML with Crawl4AI for clickable links...")
            
            # Create a minimal HTML document with just the ChatGPT response content
            complete_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>ChatGPT Response</title>
</head>
<body>
    <div class="chatgpt-response">
        {response_html}
    </div>
</body>
</html>"""
            
            # Create temporary HTML file for crawl4ai processing
            temp_html_path = os.path.join(ephemeral_base, f"temp_chatgpt_response_{timestamp}.html")
            with open(temp_html_path, 'w', encoding='utf-8') as f:
                f.write(complete_html)
            
            # Use crawl4ai with settings that preserve links
            async with AsyncWebCrawler(
                cache_mode="none",
                use_database=False,
                cache_dir=os.path.join(ephemeral_base, 'cache')
            ) as crawler:
                crawl_result = await crawler.arun(
                    url=f"file://{os.path.abspath(temp_html_path)}"
                )
                
                if crawl_result and crawl_result.markdown:
                    # Crawl4AI automatically converts <a> tags to [text](url) format
                    clean_markdown = crawl_result.markdown
                    
                    # Extract all links for sources section
                    links_list = []
                    if crawl_result.links:
                        internal_links = crawl_result.links.get("internal", [])
                        external_links = crawl_result.links.get("external", [])
                        
                        # Combine all links and filter for external sources only
                        all_links = internal_links + external_links
                        for link in all_links:
                            if isinstance(link, dict):
                                href = link.get('href', '')
                                text = link.get('text', '')
                                # Only include external links that aren't ChatGPT internal
                                if (href and text and 
                                    'chatgpt.com' not in href.lower() and 
                                    len(text.strip()) > 2):
                                    links_list.append({'text': text.strip(), 'url': href})
                    
                    print(f"Crawl4AI processed content: {len(clean_markdown)} characters")
                    print(f"Found {len(links_list)} external links")
                    
                    # Clean up temporary file
                    if os.path.exists(temp_html_path):
                        os.remove(temp_html_path)
                    
                    return clean_markdown, links_list
                else:
                    print("Crawl4AI returned empty result")
                    
            # Clean up temporary file if processing failed
            if os.path.exists(temp_html_path):
                os.remove(temp_html_path)
                
        except Exception as e:
            print(f"Crawl4AI processing failed: {e}")
    
    # Fallback: return basic text extraction
    print("Using fallback text extraction")
    response_text = re.sub(r'<[^>]+>', '', response_html)
    response_text = re.sub(r'\s+', ' ', response_text).strip()
    return response_text, []


def create_properly_formatted_markdown(chatgpt_content, links_list, query, timestamp):
    """Create properly formatted markdown with clean structure and clickable links"""
    
    if not chatgpt_content:
        chatgpt_content = "No response content extracted."
    
    # Clean and format the ChatGPT response
    formatted_response = chatgpt_content.strip()
    
    # Create the markdown structure
    markdown_content = f"""# ChatGPT Response

**Query:** {query}  
**Timestamp:** {timestamp}  
**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")}

---

## Response Content

{formatted_response}"""
    
    # Add sources section if links were found
    if links_list and len(links_list) > 0:
        markdown_content += f"""

---

## Sources

"""
        # Remove duplicates while preserving order
        seen_urls = set()
        unique_links = []
        for link in links_list:
            if link['url'] not in seen_urls:
                unique_links.append(link)
                seen_urls.add(link['url'])
        
        for i, link in enumerate(unique_links, 1):
            markdown_content += f"{i}. [{link['text']}]({link['url']})\n"
    
    markdown_content += f"""

---

## Metadata

- **Extraction Method:** Playwright + Crawl4AI with Link Preservation
- **Response Length:** {len(formatted_response)} characters
- **Links Found:** {len(links_list) if links_list else 0}
- **Status:** Success
- **Source:** https://chatgpt.com

---

*Generated by ChatGPT Automation System*
"""
    
    return markdown_content


def upload_html_to_s3(html_content, bucket_name, s3_path, timestamp, folder_name):
    """Upload HTML content to S3 (old format)"""
    try:
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=bucket_name)
        
        html_filename = f"{timestamp}_chatgpt_{folder_name.replace('_', '_')}_response.html"
        html_s3_key = f"{s3_path}/{folder_name}/{html_filename}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=html_s3_key,
            Body=html_content,
            ContentType='text/html'
        )
        
        print(f"HTML uploaded to S3: s3://{bucket_name}/{html_s3_key}")
        return html_s3_key
        
    except Exception as e:
        print(f"Error uploading HTML to S3: {e}")
        return None

def upload_to_new_bucket_structure(content, job_id, product_id, query_id, content_type='text/markdown', file_extension='md'):
    """Upload content to new bucket structure: brand_name/job_id/product_name/query_text/mode/response.md/json"""
    try:
        new_bucket = 'bodhium-temp'
        s3_client = boto3.client('s3')
        
        # Get brand name and product name from database
        brand_name = get_brand_name_from_db(job_id)
        product_name = get_product_name_from_db(product_id) if product_id else 'Unknown Product'
        
        # Get query text from the content or use a default
        query_text = "unknown_query"
        if isinstance(content, dict) and 'query' in content:
            query_text = content['query']
        elif isinstance(content, str) and len(content) > 0:
            # Extract first few words as query text
            query_text = content.split('\n')[0][:50] if '\n' in content else content[:50]
        
        # Sanitize names for S3 path (replace spaces with underscores, remove special chars)
        brand_name_safe = re.sub(r'[^a-zA-Z0-9\s]', '', brand_name).replace(' ', '_').strip()
        product_name_safe = re.sub(r'[^a-zA-Z0-9\s]', '', product_name).replace(' ', '_').strip()
        query_text_safe = re.sub(r'[^a-zA-Z0-9\s]', '', query_text).replace(' ', '_').strip()
        
        # Create new S3 key following the required structure: brand_name/job_id/product_name/query_text/mode/response.md/json
        s3_key = f"{brand_name_safe}/{job_id}/{product_name_safe}/{query_text_safe}/cgpt/response.{file_extension}"
        
        if isinstance(content, str):
            body = content
        else:
            body = json.dumps(content, indent=2, ensure_ascii=False)
        
        s3_client.put_object(
            Bucket=new_bucket,
            Key=s3_key,
            Body=body,
            ContentType=content_type
        )
        
        print(f"Content uploaded to new bucket structure: s3://{new_bucket}/{s3_key}")
        return f"s3://{new_bucket}/{s3_key}"
        
    except Exception as e:
        print(f"Error uploading to new bucket structure: {e}")
        return None


def upload_screenshot_to_s3(screenshot_path, bucket_name, s3_path, timestamp, folder_name):
    """Upload screenshot to S3"""
    try:
        s3_client = boto3.client('s3')
        screenshot_filename = f"{timestamp}_chatgpt_{folder_name.replace('_', '_')}_screenshot.png"
        screenshot_s3_key = f"{s3_path}/{folder_name}/{screenshot_filename}"
        
        with open(screenshot_path, "rb") as f:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=screenshot_s3_key,
                Body=f,
                ContentType='image/png'
            )
        print(f"Screenshot uploaded to S3: s3://{bucket_name}/{screenshot_s3_key}")
        return screenshot_s3_key
    except Exception as e:
        print(f"Error uploading screenshot to S3: {e}")
        return None


def upload_processed_data_to_s3(processed_data, bucket_name, s3_path, timestamp, folder_name):
    """Upload processed data to S3"""
    try:
        s3_client = boto3.client('s3')
        data_filename = f"{timestamp}_chatgpt_{folder_name.replace('_', '_')}_processed.json"
        data_s3_key = f"{s3_path}/{folder_name}/{data_filename}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=data_s3_key,
            Body=json.dumps(processed_data, indent=2, ensure_ascii=False),
            ContentType='application/json'
        )
        print(f"Processed data uploaded to S3: s3://{bucket_name}/{data_s3_key}")
        return data_s3_key
    except Exception as e:
        print(f"Error uploading processed data to S3: {e}")
        return None


def upload_markdown_to_s3(markdown_content, bucket_name, s3_path, timestamp, folder_name):
    """Upload markdown content to S3"""
    try:
        s3_client = boto3.client('s3')
        markdown_filename = f"{timestamp}_chatgpt_{folder_name.replace('_', '_')}_response.md"
        markdown_s3_key = f"{s3_path}/{folder_name}/{markdown_filename}"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=markdown_s3_key,
            Body=markdown_content,
            ContentType='text/markdown'
        )
        
        print(f"Markdown uploaded to S3: s3://{bucket_name}/{markdown_s3_key}")
        return markdown_s3_key
        
    except Exception as e:
        print(f"Error uploading markdown to S3: {e}")
        return None


async def handle_cookie_consent(page):
    """Handle cookie consent banner"""
    try:
        print("Checking for cookie consent...")
        cookie_selectors = [
            'button:has-text("Accept all")',
            'button:has-text("Accept")',
            '[data-testid="cookie-accept"]',
            'button:has-text("OK")'
        ]
        
        for selector in cookie_selectors:
            try:
                cookie_button = await page.wait_for_selector(selector, timeout=3000)
                if cookie_button:
                    print("Accepting cookies...")
                    await cookie_button.click()
                    await asyncio.sleep(2)
                    return True
            except:
                continue
        
        return False
    except Exception as e:
        print(f"Cookie handling error: {e}")
        return False


async def close_popups_and_overlays(page, attempts: int = 3) -> bool:
    """Attempt to close any blocking popups/overlays on chatgpt.com.

    Specifically targets the GPT-5 upsell modal and other common dialogs.
    Returns True if something was closed/hidden.
    """
    something_closed = False
    close_button_selectors = [
        'div[data-testid="modal-no-auth-gpt5-upsell"] button[aria-label="Close"]',
        '[data-testid="modal-no-auth-gpt5-upsell"] button[aria-label="Close"]',
        '[data-testid="modal-no-auth-gpt5-upsell"] [aria-label="Close"]',
        '[role="dialog"][data-state="open"] button[aria-label="Close"]',
        '[data-state="open"] [role="dialog"] button[aria-label="Close"]',
        'button:has-text("Close")',
        'button:has-text("Dismiss")',
        'button:has-text("Got it")',
        'button:has-text("No thanks")',
        'button:has-text("Not now")'
    ]

    overlay_container_selectors = [
        '[data-testid="modal-no-auth-gpt5-upsell"]',
        'div[role="dialog"][data-state="open"]',
        '[data-radix-focus-guard]',
        'div[data-ignore-for-page-load="true"]'
    ]

    for _ in range(max(1, attempts)):
        try:
            # Try ESC first (often closes modals)
            try:
                await page.keyboard.press('Escape')
            except Exception:
                pass

            # Try clicking explicit close buttons
            for selector in close_button_selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(0.5)
                        print(f"Popup closed via selector: {selector}")
                        something_closed = True
                except Exception:
                    continue

            # As a fallback, hide known overlay containers via CSS
            try:
                await page.add_style_tag(content='\n'.join([
                    f"{sel} {{ display: none !important; visibility: hidden !important; pointer-events: none !important; }}"
                    for sel in overlay_container_selectors
                ]))
                # Also explicitly set pointer-events: none on full-screen blockers
                await page.evaluate("""
                    () => {
                        const sels = [
                            '[data-testid="modal-no-auth-gpt5-upsell"]',
                            'div[role="dialog"][data-state="open"]',
                            'div.absolute.inset-0[data-ignore-for-page-load="true"]'
                        ];
                        for (const s of sels) {
                            document.querySelectorAll(s).forEach(el => {
                                el.style.pointerEvents = 'none';
                            });
                        }
                    }
                """)
            except Exception:
                pass

            await asyncio.sleep(0.5)
        except Exception:
            continue

    return something_closed


async def wait_for_response_completion(page):
    """Wait for ChatGPT response to complete"""
    print("Waiting for response to complete...")
    
    response_selectors = [
        '[data-message-author-role="assistant"]',
        '.markdown',
        '[class*="message"]',
        '[class*="response"]'
    ]
    
    max_wait_time = 120  # 2 minutes
    start_time = time.time()
    response_detected = False
    last_response_length = 0
    stable_count = 0
    max_stable_count = 4
    
    while time.time() - start_time < max_wait_time:
        try:
            # Check for loading indicators
            loading_indicators = await page.query_selector_all(
                '[class*="loading"], [class*="thinking"], [class*="generating"], '
                '[class*="typing"], button:has-text("Stop generating"), [data-testid*="stop"]'
            )
            
            streaming_indicators = await page.query_selector_all(
                '[class*="cursor"], [class*="blink"], [class*="animate"]'
            )
            
            is_still_generating = len(loading_indicators) > 0 or len(streaming_indicators) > 0
            
            if not is_still_generating:
                response_found = False
                for selector in response_selectors:
                    response_elements = await page.query_selector_all(selector)
                    if response_elements:
                        current_response = await response_elements[-1].text_content()
                        current_length = len(current_response)
                        
                        if current_length > 0:
                            response_found = True
                            if not response_detected:
                                print("Response detected, monitoring for completion...")
                                response_detected = True
                            
                            if current_length == last_response_length:
                                stable_count += 1
                                print(f"Response stable for {stable_count}/{max_stable_count} checks...")
                                if stable_count >= max_stable_count:
                                    print("Response appears to be complete!")
                                    await asyncio.sleep(5)  # Extra wait
                                    
                                    # Final check
                                    final_response = await response_elements[-1].text_content()
                                    if len(final_response) == current_length:
                                        print("Response confirmed complete!")
                                        return True
                                    else:
                                        print("New content detected, continuing to wait...")
                                        stable_count = 0
                                        last_response_length = len(final_response)
                            else:
                                stable_count = 0
                                last_response_length = current_length
                                print(f"Response growing: {current_length} characters...")
                            
                            break
                        
                if not response_found:
                    await asyncio.sleep(3)
                    continue
                else:
                    if stable_count >= max_stable_count:
                        break
            else:
                print("Still generating... waiting...")
                stable_count = 0
                await asyncio.sleep(3)
        
        except Exception as e:
            print(f"Error while waiting for response: {e}")
            await asyncio.sleep(3)
        
        await asyncio.sleep(1.5)
    
    return response_detected


async def automate_chatgpt_bright_data(query="top 5 mobile phone brands in india"):
    """Automate ChatGPT using Bright Data's Scraping Browser with Crawl4AI for clickable links"""
    async with async_playwright() as playwright:
        print('🚀 Connecting to Bright Data Scraping Browser API...')
        
        try:
            # Connect to Bright Data Scraping Browser
            browser = await playwright.chromium.connect_over_cdp(SBR_WS_CDP)
            
            print('✅ Connected! Creating new page...')
            page = await browser.new_page()
            
            # Navigate to ChatGPT
            print('🌐 Navigating to ChatGPT...')
            await page.goto('https://chatgpt.com/', wait_until='domcontentloaded', timeout=60000)
            
            # Handle cookie consent
            await handle_cookie_consent(page)
            # Close any upsells/popups that may block interaction
            await close_popups_and_overlays(page, attempts=3)
            
            # Wait for page to load
            await asyncio.sleep(random.uniform(3, 5))
            
            # Look for input field
            print("🔍 Looking for input field...")
            input_selectors = [
                'textarea[placeholder*="Message"]',
                'textarea[data-id="root"]',
                '#prompt-textarea',
                'div[contenteditable="true"]',
                'textarea',
                '[role="textbox"]'
            ]
            
            input_element = None
            for selector in input_selectors:
                try:
                    input_element = await page.wait_for_selector(selector, timeout=10000)
                    if input_element:
                        print(f"✅ Found input using selector: {selector}")
                        break
                except:
                    continue
            
            if not input_element:
                # Try closing popups again and retry locating input
                print("Input field not found. Retrying after closing popups...")
                await close_popups_and_overlays(page, attempts=3)
                for selector in input_selectors:
                    try:
                        input_element = await page.wait_for_selector(selector, timeout=8000)
                        if input_element:
                            print(f"✅ Found input on retry using selector: {selector}")
                            break
                    except:
                        continue
                if not input_element:
                    raise Exception("Input field not found")
            
            print(f"💬 Typing query: '{query}'")
            
            # Clear and type query
            await input_element.wait_for_element_state('stable', timeout=10000)
            try:
                await input_element.click()
            except Exception:
                # If click is intercepted, close popups and try one more time
                await close_popups_and_overlays(page, attempts=2)
                await input_element.click()
            await page.keyboard.press('Control+a')
            await input_element.fill(query)
            
            # Submit query
            await asyncio.sleep(random.uniform(1, 2))
            print("📤 Submitting query...")
            await page.keyboard.press('Enter')
            
            # Wait for response
            response_success = await wait_for_response_completion(page)
            if not response_success:
                print("⚠️ Response did not complete successfully")
            
            # Additional wait for full content
            await asyncio.sleep(10)
            
            # Create timestamp and folder name based on query
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Create folder name from query (replace spaces with underscores, remove special chars)
            query_folder = re.sub(r'[^a-zA-Z0-9\s]', '', query).replace(' ', '_')
            folder_name = f"{query_folder}_{timestamp}"
            
            # Extract and process ChatGPT response with Crawl4AI for clickable links
            chatgpt_content, links_list = await extract_chatgpt_response_html_and_process(page, query, timestamp)
            
            # Create properly formatted markdown with clickable links
            print("📝 Creating formatted markdown with clickable links...")
            formatted_markdown = create_properly_formatted_markdown(chatgpt_content, links_list, query, timestamp)
            print(f"✅ Created formatted markdown ({len(formatted_markdown)} characters)")
            if links_list:
                print(f"✅ Included {len(links_list)} clickable links in sources section")
            
            # Get full page HTML for backup
            print("🌐 Getting full page HTML for backup...")
            full_html_content = await page.content()
            
            # Take screenshot
            print("📸 Taking screenshot...")
            screenshot_path = os.path.join(ephemeral_base, f"chatgpt_response_{timestamp}.png")
            await page.screenshot(path=screenshot_path, full_page=True)
            
            # Upload to S3
            S3_BUCKET = os.environ.get('S3_BUCKET')
            S3_PATH = os.environ.get('S3_PATH')
            
            print("☁️ Uploading to S3...")
            html_s3_key = upload_html_to_s3(full_html_content, S3_BUCKET, S3_PATH, timestamp, folder_name)
            screenshot_s3_key = upload_screenshot_to_s3(screenshot_path, S3_BUCKET, S3_PATH, timestamp, folder_name)
            markdown_s3_key = upload_markdown_to_s3(formatted_markdown, S3_BUCKET, S3_PATH, timestamp, folder_name)
            
            # Create response data
            response_data = {
                "url": page.url,
                "query": query,
                "timestamp": datetime.now().isoformat(),
                "extraction": {
                    "chatgpt_content": chatgpt_content,
                    "response_length": len(chatgpt_content),
                    "formatted_markdown": formatted_markdown,
                    "markdown_length": len(formatted_markdown),
                    "links_found": links_list
                },
                "files": {
                    "html_s3_key": html_s3_key,
                    "screenshot_s3_key": screenshot_s3_key,
                    "markdown_s3_key": markdown_s3_key
                },
                "metadata": {
                    "extraction_method": "crawl4ai_with_clickable_links",
                    "bright_data_used": True,
                    "properly_formatted": True,
                    "markdown_structure": True,
                    "clickable_links": len(links_list) if links_list else 0,
                    "crawl4ai_used": CRAWL4AI_AVAILABLE
                },
                "status": "success" if chatgpt_content else "partial"
            }
            
            # Upload processed data
            processed_s3_key = upload_processed_data_to_s3(response_data, S3_BUCKET, S3_PATH, timestamp, folder_name)
            if processed_s3_key:
                response_data["files"]["processed_s3_key"] = processed_s3_key
            
            # Print results
            print("\n" + "="*80)
            print("🎯 CHATGPT RESPONSE EXTRACTED WITH CLICKABLE LINKS:")
            print("="*80)
            print(chatgpt_content[:1000] + "..." if len(chatgpt_content) > 1000 else chatgpt_content)
            print("="*80)
            
            print(f"\n📊 PROCESSING SUMMARY:")
            print(f"   Response Length: {len(chatgpt_content)} characters")
            print(f"   Markdown Length: {len(formatted_markdown)} characters")
            print(f"   Clickable Links: {len(links_list) if links_list else 0}")
            print(f"   Properly Formatted: True")
            print(f"   Crawl4AI Used: {CRAWL4AI_AVAILABLE}")
            print("="*80)
            
            return response_data
            
        except Exception as e:
            print(f"❌ Error in automation: {e}")
            # Best-effort debug screenshot before closing
            try:
                ts_err = datetime.now().strftime("%Y%m%d_%H%M%S")
                debug_path = os.path.join(ephemeral_base, f"error_{ts_err}.png")
                if 'page' in locals() and page:
                    await page.screenshot(path=debug_path, full_page=True)
                    print(f"🧩 Debug screenshot saved to ephemeral storage: {debug_path}")
            except Exception as se:
                print(f"Failed to capture debug screenshot: {se}")
            return {
                "error": str(e),
                "status": "failed",
                "timestamp": datetime.now().isoformat()
            }
        
        finally:
            print("🔌 Closing browser connection...")
            try:
                await browser.close()
            except:
                pass


def get_secret(secret_name=None):
    if secret_name is None:
        secret_name = os.environ.get("RDS_SECRET_NAME", "dev/rds")
    region_name = os.environ.get("secret_region", "us-east-1")
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except ClientError as e:
        logger.error(f"Secrets Manager fetch failed: {str(e)}")
        raise e


def get_db_connection():
    try:
        secret = get_secret(os.environ.get("RDS_SECRET_NAME", "dev/rds"))
        conn = psycopg2.connect(
            host=secret['DB_HOST'],
            database=secret['DB_NAME'],
            user=secret['DB_USER'],
            password=secret['DB_PASSWORD'],
            port=secret['DB_PORT']
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def get_product_name_from_db(product_id):
    """Fetch product name from products table"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT product_data FROM products WHERE product_id = %s",
            (product_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0]:
            product_data = result[0]
            # Extract productname from JSON data
            product_name = product_data.get('productname', 'Unknown Product')
            logger.info(f"Found product name: {product_name} for product_id: {product_id}")
            return product_name
        else:
            logger.warning(f"Product not found for product_id: {product_id}")
            return 'Unknown Product'
            
    except Exception as e:
        logger.error(f"Error fetching product name for product_id {product_id}: {e}")
        return 'Unknown Product'

def get_brand_name_from_db(job_id):
    """Fetch brand name from scrapejobs table using job_id"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT brand_name FROM scrapejobs WHERE job_id = %s",
            (job_id,)
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0]:
            brand_name = result[0]
            logger.info(f"Found brand name: {brand_name} for job_id: {job_id}")
            return brand_name
        else:
            logger.warning(f"Brand name not found for job_id: {job_id}")
            return 'Unknown Brand'
            
    except Exception as e:
        logger.error(f"Error fetching brand name for job_id {job_id}: {e}")
        return 'Unknown Brand'

def create_llm_task(job_id, query_id, llm_model_name="Chatgpt", product_id=None, product_name=None, session_id=None, task_id=None):
    logger.info(f"Creating LLM task for job_id: {job_id}, query_id: {query_id}, session_id: {session_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        import uuid, datetime
        
        # Use provided task_id or generate new one
        if not task_id:
            task_id = str(uuid.uuid4())
        
        # Check if task_id already exists (for retry scenarios)
        if session_id:
            cursor.execute(
                "SELECT status FROM llmtasks WHERE task_id = %s",
                (task_id,)
            )
            existing_task = cursor.fetchone()
            
            if existing_task:
                # Task exists, update status to "retrying"
                cursor.execute(
                    "UPDATE llmtasks SET status = %s, session_id = %s WHERE task_id = %s",
                    ("retrying", session_id, task_id)
                )
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"Updated existing task {task_id} to retrying status")
                return task_id
        
        # Insert new task
        cursor.execute(
            """
            INSERT INTO llmtasks (
                task_id,
                job_id,
                query_id,
                llm_model_name,
                status,
                created_at,
                product_id,
                product_name,
                session_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                task_id,
                job_id,
                query_id,
                llm_model_name,
                "created",
                datetime.datetime.now(datetime.timezone.utc),
                product_id,
                product_name,
                session_id
            )
        )
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Inserted LLM task to DB -- task_id: {task_id}, job_id: {job_id}, query_id: {query_id}, session_id: {session_id}")
        return task_id
    except Exception as e:
        logger.error(f"Error creating LLM task for job_id: {job_id}, query_id: {query_id}: {e}")
        raise

def update_task_status(task_id, status, error_message=None, s3_output_path=None, completed_at=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        update_fields = ["status = %s"]
        params = [status]
        if error_message is not None:
            update_fields.append("error_message = %s")
            params.append(error_message)
        if s3_output_path is not None:
            update_fields.append("s3_output_path = %s")
            params.append(s3_output_path)
        if completed_at is not None:
            update_fields.append("completed_at = %s")
            params.append(completed_at)
        params.append(task_id)
        query = f"UPDATE llmtasks SET {', '.join(update_fields)} WHERE task_id = %s"
        cursor.execute(query, params)
        
        # If task failed, also create a record in failed_tasks
        if status == "failed" and error_message:
            create_failed_task_record(task_id, error_message)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Updated task {task_id} status to {status}")
    except Exception as e:
        logger.error(f"Error updating task status for task_id: {task_id}: {e}")
        raise

def create_failed_task_record(task_id, error_message):
    """Create a record in failed_tasks table when a task fails"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get task details from llmtasks
        cursor.execute(
            """
            SELECT session_id, job_id, query_id, llm_model_name, status, s3_output_path, 
                   created_at, completed_at, product_name, product_id
            FROM llmtasks 
            WHERE task_id = %s
            """,
            (task_id,)
        )
        
        task_data = cursor.fetchone()
        if task_data:
            session_id, job_id, query_id, llm_model_name, status, s3_output_path, created_at, completed_at, product_name, product_id = task_data
            
            # Insert into failed_tasks
            cursor.execute(
                """
                INSERT INTO failed_tasks (
                    task_id, session_id, job_id, query_id, llm_model_name, status,
                    s3_output_path, error_message, created_at, completed_at, product_name, product_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    task_id, session_id, job_id, query_id, llm_model_name, status,
                    s3_output_path, error_message, created_at, completed_at, product_name, product_id
                )
            )
            
            logger.info(f"Created failed_tasks record for task {task_id}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error creating failed_tasks record: {e}")
        # Don't raise - this is not critical for the main flow

def log_orchestration_event(job_id, event_name, details=None):
    """Log an orchestration event to DynamoDB."""
    import uuid, datetime
    table_name = os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    unique_id = str(uuid.uuid4())[:8]
    pk = f"JOB#{job_id}"
    sk = f"{timestamp}#{unique_id}"
    item = {
        'pk': pk,
        'sk': sk,
        'job_id': job_id,
        'event_timestamp_id': sk,
        'eventName': event_name,
        'details': details or {},
        'created_at': timestamp
    }
    try:
        table.put_item(Item=item)
        logger.info(f"Logged orchestration event: {event_name} for job_id: {job_id}")
    except Exception as e:
        logger.error(f"Error logging orchestration event: {e}")


# -------- Lambda Invocation --------

def invoke_citation_scraper_lambda(citations: List[str], job_id: str, query_id: str, product_id: str, user_query: str, task_id: str) -> bool:
    """Invoke the citation-scraper lambda with the extracted citations"""
    try:
        if not citations:
            logger.info("No citations to scrape, skipping citation-scraper invocation")
            return True
            
        lambda_client = boto3.client('lambda')
        
        # Prepare payload for citation-scraper lambda with full context
        payload = {
            'urls': citations,
            'job_id': job_id,
            'product_id': product_id,
            'mode': 'cgpt',  # Use specific mode for ChatGPT
            'query': user_query[:100] if user_query else 'na',  # Truncate query if too long
            'brand_name': get_brand_name_from_db(job_id),  # Pass brand name directly
            'product_name': get_product_name_from_db(product_id) if product_id else 'Unknown Product'  # Pass product name directly
        }
        
        # Get the citation-scraper lambda function name from environment or use default
        citation_lambda_name = os.environ.get('CITATION_SCRAPER_LAMBDA_NAME', 'citation-scraper')
        
        logger.info(f"Invoking citation-scraper lambda: {citation_lambda_name} with {len(citations)} URLs")
        logger.info(f"Payload: {json.dumps(payload, ensure_ascii=False)}")
        
        response = lambda_client.invoke(
            FunctionName=citation_lambda_name,
            InvocationType='Event',  # Asynchronous invocation
            Payload=json.dumps(payload)
        )
        
        logger.info(f"Successfully invoked citation-scraper lambda. Response: {response}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to invoke citation-scraper lambda: {str(e)}")
        return False


def lambda_handler(event, context):
    """AWS Lambda handler function"""
    task_id = None
    try:
        print("🚀 Lambda function started")
        query = event.get('query', 'top 5 mobile phone brands in india')
        
        # Extract job_id, query_id, product_id, session_id, and task_id from event
        job_id = event.get('job_id', str(uuid.uuid4()))
        query_id = event.get('query_id', str(uuid.uuid4()))
        product_id = event.get('product_id', str(uuid.uuid4()))  # NEW: Extract product_id
        session_id = event.get('session_id')  # NEW: Extract session_id
        provided_task_id = event.get('task_id')  # NEW: Extract provided task_id
        
        print(f"📊 Job ID: {job_id}, Query ID: {query_id}, Product ID: {product_id}, Session ID: {session_id}")
        
        # Get product name from database
        product_name = get_product_name_from_db(product_id) if product_id else 'Unknown Product'
        
        # Create LLM task in database
        try:
            task_id = create_llm_task(job_id, query_id, "ChatGPT", product_id, product_name, session_id, provided_task_id)
            print(f"✅ Created LLM task in RDS: {task_id}")
        except Exception as db_error:
            print(f"❌ Failed to create LLM task in RDS: {db_error}")
            # Continue execution even if DB fails
        
        # Update task status to running
        if task_id:
            try:
                update_task_status(task_id, "running")
                print(f"✅ Updated task status to 'running' in RDS")
            except Exception as db_error:
                print(f"❌ Failed to update task status to running: {db_error}")
        
        # Log orchestration event
        try:
            log_orchestration_event(job_id, "task_started", {
                "query": query,
                "task_id": task_id,
                "product_id": product_id,  # NEW: Include product_id in logging
                "timestamp": datetime.now().isoformat()
            })
            print(f"✅ Logged orchestration event to DynamoDB")
        except Exception as db_error:
            print(f"❌ Failed to log orchestration event: {db_error}")
        
        # Execute the main automation
        result = asyncio.run(automate_chatgpt_bright_data(query))
        
        # NEW: Upload to new bucket structure
        if result and result.get('status') != 'failed':
            try:
                # Get the formatted markdown content for new bucket
                formatted_markdown = result.get('extraction', {}).get('formatted_markdown', '')
                if formatted_markdown:
                    new_bucket_path = upload_to_new_bucket_structure(
                        formatted_markdown, job_id, product_id, query_id, 'text/markdown', 'md'
                    )
                    if new_bucket_path:
                        print(f"✅ Uploaded to new bucket structure: {new_bucket_path}")
                        result['new_bucket_path'] = new_bucket_path
                    else:
                        print(f"❌ Failed to upload to new bucket structure")
                else:
                    print(f"❌ No formatted markdown content to upload to new bucket")
                    
            except Exception as new_bucket_error:
                print(f"❌ Error uploading to new bucket: {new_bucket_error}")
        
        # Update task status based on result
        if task_id:
            try:
                if result and result.get('status') != 'failed':
                    processed_key = result.get('files', {}).get('processed_s3_key', '')
                    s3_bucket_env = os.environ.get('S3_BUCKET') or ''
                    s3_output_path = f"s3://{s3_bucket_env}/{processed_key}" if processed_key and s3_bucket_env else processed_key
                    # Use new bucket path instead of old S3 path
                    final_s3_path = new_bucket_path if new_bucket_path else s3_output_path
                    
                    update_task_status(
                        task_id, 
                        "completed", 
                        s3_output_path=final_s3_path,
                        completed_at=datetime.now(timezone.utc)
                    )
                    print(f"✅ Updated task status to 'completed' in RDS")
                else:
                    error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                    update_task_status(task_id, "failed", error_message=error_msg)
                    print(f"❌ Updated task status to 'failed' in RDS: {error_msg}")
            except Exception as db_error:
                print(f"❌ Failed to update final task status: {db_error}")
        
        # Log completion event
        try:
            log_orchestration_event(job_id, "task_completed", {
                "task_id": task_id,
                "product_id": product_id,  # NEW: Include product_id in logging
                "status": "success" if (result and result.get('status') != 'failed') else "failed",
                "result_length": len(str(result)) if result else 0,
                "timestamp": datetime.now().isoformat()
            })
            print(f"✅ Logged completion event to DynamoDB")
        except Exception as db_error:
            print(f"❌ Failed to log completion event: {db_error}")
        
        # Trigger citation-scraper lambda with extracted citations
        if result and result.get('status') != 'failed' and result.get('extraction', {}).get('links_found'):
            try:
                citations = result.get('extraction', {}).get('links_found', [])
                if citations:
                    # Extract and validate URLs from the links list
                    citation_urls = []
                    for link in citations:
                        url = link.get('url', '')
                        if url:
                            # Ensure URL has proper protocol
                            if url.startswith('//'):
                                url = 'https:' + url
                            elif url.startswith('/'):
                                # Skip relative URLs as they're not useful for citation scraping
                                continue
                            elif not url.startswith(('http://', 'https://')):
                                # Skip URLs without protocol
                                continue
                            
                            # Validate URL format
                            if url.startswith(('http://', 'https://')) and len(url) > 10:
                                citation_urls.append(url)
                    
                    if citation_urls:
                        # Remove duplicates while preserving order
                        unique_citation_urls = []
                        seen_urls = set()
                        for url in citation_urls:
                            if url not in seen_urls:
                                unique_citation_urls.append(url)
                                seen_urls.add(url)
                        
                        citation_invocation_success = invoke_citation_scraper_lambda(
                            unique_citation_urls, job_id, query_id, product_id, query, task_id
                        )
                        if citation_invocation_success:
                            logger.info(f"Successfully triggered citation-scraper lambda with {len(unique_citation_urls)} citations")
                            print(f"🔗 Citation-scraper lambda triggered with {len(unique_citation_urls)} URLs")
                            
                            # Log citation-scraper trigger event
                            try:
                                log_orchestration_event(job_id, "citation_scraper_triggered", {
                                    "task_id": task_id,
                                    "product_id": product_id,
                                    "citations_count": len(unique_citation_urls),
                                    "citations": unique_citation_urls[:5],  # Log first 5 citations for reference
                                    "pipeline": "chatgpt_extraction",
                                    "timestamp": datetime.now().isoformat()
                                })
                            except Exception as log_error:
                                print(f"⚠️ Failed to log citation-scraper trigger event: {log_error}")
                        else:
                            logger.warning("Failed to trigger citation-scraper lambda")
                            print(f"⚠️ Failed to trigger citation-scraper lambda")
                    else:
                        logger.info("No valid citation URLs found, skipping citation-scraper invocation")
                        print(f"ℹ️ No valid citation URLs found, skipping citation-scraper invocation")
                else:
                    logger.info("No citations found, skipping citation-scraper invocation")
                    print(f"ℹ️ No citations found, skipping citation-scraper invocation")
            except Exception as citation_error:
                logger.error(f"Error triggering citation-scraper lambda: {citation_error}")
                print(f"❌ Error triggering citation-scraper lambda: {citation_error}")
        
        if result and result.get('status') != 'failed':
            return {
                'statusCode': 200,
                'body': json.dumps(result, default=str),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps(result or {'error': 'Unknown error'}),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
            
    except Exception as e:
        print(f"❌ Lambda handler error: {e}")
        
        # Try to update task status to failed
        if task_id:
            try:
                update_task_status(task_id, "failed", error_message=str(e))
                print(f"✅ Updated task status to 'failed' due to error")
            except Exception as db_error:
                print(f"❌ Failed to update error status: {db_error}")
        
        # Log error event
        try:
            if 'job_id' in locals():
                log_orchestration_event(job_id, "task_error", {
                    "error": str(e),
                    "task_id": task_id,
                    "product_id": product_id if 'product_id' in locals() else None,  # NEW: Include product_id in error logging
                    "timestamp": datetime.now().isoformat()
                })
                print(f"✅ Logged error event to DynamoDB")
        except Exception as db_error:
            print(f"❌ Failed to log error event: {db_error}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }





import asyncio
import os
import base64
from datetime import datetime, timezone
import argparse
import sys
import tempfile
import uuid
import shutil
import atexit
import time
import json
import boto3
from botocore.exceptions import ClientError
import logging
import psycopg2
import random
import re
import urllib.parse

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === ADDED: INTELLIGENT ANSWER BLOCK START EXTRACTION ===
def extract_real_aimode_answer(markdown_text):
    """Extract the real AI Mode answer by identifying start patterns"""
    # Patterns to mark start of actual answer block
    start_patterns = [
        r'Kicking off \d+ searches',
        r'Several sources discuss',
        r"Here's a breakdown",
        r"Putting it all together",
        r"^\d+\.\s+\w+",            # Numbered list (e.g. "1. Specialized ...")
        r"Top \d+ bike models",
        r"Best bikes",
        r"Fastest bike",
        r"According to",
        r"Based on",
    ]
    
    for pattern in start_patterns:
        match = re.search(pattern, markdown_text, flags=re.MULTILINE)
        if match:
            return markdown_text[match.start():]
    
    # Fallback: first substantial paragraph
    paragraphs = [p for p in markdown_text.split('\n\n') if len(p.strip()) > 20]
    if paragraphs:
        return paragraphs[0]
    
    return markdown_text

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
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
    from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
    from crawl4ai.content_filter_strategy import PruningContentFilter
    print("crawl4ai imported successfully")
    CRAWL4AI_AVAILABLE = True
except Exception as e:
    print(f"crawl4ai import failed: {e}")
    print("Continuing without crawl4ai - will use basic HTML extraction")
    CRAWL4AI_AVAILABLE = False

# Import Playwright
try:
    from playwright.async_api import async_playwright
    print("playwright imported successfully")
    PLAYWRIGHT_AVAILABLE = True
except Exception as e:
    print(f"playwright import failed: {e}")
    PLAYWRIGHT_AVAILABLE = False

# Bright Data Configuration
def get_brightdata_auth():
    """Get Bright Data credentials from AWS Secrets Manager"""
    secret_name = "AIMODE-BRIGHTDATA"
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
            if isinstance(secret_json, dict) and 'AIMODE-BRIGHTDATA' in secret_json:
                return secret_json['AIMODE-BRIGHTDATA']
            else:
                return secret_string
        except json.JSONDecodeError:
            # If it's not JSON, return as-is
            return secret_string
            
    except ClientError as e:
        print(f"Error fetching Bright Data credentials from Secrets Manager: {e}")
        raise e

# Get credentials and build WebSocket URL with US country and English language parameters
try:
    AUTH = get_brightdata_auth()
    # Enhanced session management with unique session ID
    SESSION_ID = str(uuid.uuid4())
    SBR_WS_CDP = f'wss://{AUTH}@brd.superproxy.io:9222?country=US&language=en&session_id={SESSION_ID}&proxy_type=residential'
    print("✅ Bright Data credentials loaded with US country, English language, and unique session")
except Exception as e:
    print(f"❌ Failed to load Bright Data credentials: {e}")
    SBR_WS_CDP = None

class GoogleAIModeExtractor:
    def __init__(self, headless=False, verbose=True, use_brightdata=True):
        self.headless = headless
        self.verbose = verbose
        self.use_brightdata = use_brightdata
        self.downloads_path = tempfile.mkdtemp(prefix='google_ai_responses_')
        self.session_id = str(uuid.uuid4()) # Unique session for this instance
        print(f"📂 Downloads will be saved to: {self.downloads_path}")
        print(f"🔑 Session ID: {self.session_id}")
        
    def log(self, message):
        if self.verbose:
            print(message)

    # Keep all existing helper methods (anti-detection, human behavior, etc.)
    async def _add_human_behavior(self, page):
        """Add human-like behavior to avoid detection"""
        # Inject anti-detection scripts
        await page.add_init_script("""
            // Override webdriver detection
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Add realistic browser plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer'},
                    {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai'},
                    {name: 'Native Client', filename: 'internal-nacl-plugin'}
                ],
            });
            
            // Mock chrome runtime
            window.chrome = {
                runtime: {},
                app: {
                    isInstalled: false,
                },
            };
            
            // Add realistic screen properties
            Object.defineProperty(screen, 'colorDepth', {
                get: () => 24,
            });
            
            // Override permissions API
            Object.defineProperty(navigator, 'permissions', {
                get: () => ({
                    query: () => Promise.resolve({state: 'granted'}),
                }),
            });
            
            // Add realistic languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en'],
            });
            
            // Mock battery API
            Object.defineProperty(navigator, 'getBattery', {
                get: () => () => Promise.resolve({
                    charging: true,
                    chargingTime: 0,
                    dischargingTime: Infinity,
                    level: 1,
                }),
            });
        """)

    async def _human_like_typing(self, page, element, text):
        """Type with human-like delays and patterns"""
        await element.click()
        await asyncio.sleep(random.uniform(0.5, 1.5))  # Pause before typing
        
        # Random backspace occasionally (typo simulation)
        if random.random() < 0.1:  # 10% chance of typo
            wrong_char = random.choice('qwertyuiop')
            await page.keyboard.type(wrong_char)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            await page.keyboard.press('Backspace')
            await asyncio.sleep(random.uniform(0.2, 0.4))
        
        for char in text:
            await page.keyboard.type(char)
            # Random delay between keystrokes (50-200ms)
            if char == ' ':
                await asyncio.sleep(random.uniform(0.1, 0.3))  # Longer pause for spaces
            else:
                await asyncio.sleep(random.uniform(0.05, 0.2))
        
        await asyncio.sleep(random.uniform(0.8, 1.5))  # Pause after typing

    async def _human_like_navigation(self, page):
        """Add human-like mouse movements and scrolling"""
        # Random mouse movement
        await page.mouse.move(
            random.randint(100, 800),
            random.randint(100, 600)
        )
        await asyncio.sleep(random.uniform(0.5, 1.0))
        
        # Random scroll
        scroll_amount = random.randint(100, 300)
        await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        # Sometimes scroll back up a bit
        if random.random() < 0.3:  # 30% chance
            await page.evaluate(f"window.scrollBy(0, -{scroll_amount // 2})")
            await asyncio.sleep(random.uniform(0.5, 1.0))

    async def _check_for_captcha(self, page):
        """Check if CAPTCHA appeared and handle it"""
        try:
            # Common CAPTCHA selectors
            captcha_selectors = [
                '#captcha',
                '.g-recaptcha',
                '[src*="captcha"]',
                'iframe[src*="recaptcha"]',
                'div:has-text("I\'m not a robot")',
                'div:has-text("unusual traffic")',
                'div:has-text("verify you\'re human")',
                '.captcha',
                '[id*="captcha"]',
                '[class*="captcha"]'
            ]
            
            for selector in captcha_selectors:
                try:
                    captcha_element = await page.query_selector(selector)
                    if captcha_element:
                        self.log("🚨 CAPTCHA detected!")
                        # Take screenshot of CAPTCHA
                        captcha_screenshot = os.path.join(self.downloads_path, f"captcha_detected_{int(time.time())}.png")
                        await page.screenshot(path=captcha_screenshot, full_page=True)
                        self.log(f"📸 CAPTCHA screenshot saved: {captcha_screenshot}")
                        return True
                except:
                    continue
            
            # Check page content for CAPTCHA text
            page_text = await page.evaluate("() => document.body.innerText.toLowerCase()")
            captcha_texts = [
                'unusual traffic', 'captcha', 'verify you\'re human',
                'not a robot', 'suspicious activity', 'automated queries'
            ]
            
            if any(text in page_text for text in captcha_texts):
                self.log("🚨 CAPTCHA detected by text content!")
                captcha_screenshot = os.path.join(self.downloads_path, f"captcha_text_detected_{int(time.time())}.png")
                await page.screenshot(path=captcha_screenshot, full_page=True)
                return True
            
            return False
            
        except Exception as e:
            self.log(f"Error checking for CAPTCHA: {e}")
            return False

    async def handle_cookie_consent(self, page):
        """Handle cookie consent banner"""
        try:
            print("Checking for cookie consent...")
            cookie_selectors = [
                'button:has-text("Accept all")',
                'button:has-text("Accept")',
                'button:has-text("I agree")',
                '[data-testid="cookie-accept"]',
                'button:has-text("OK")',
                '#L2AGLb',  # Google's "I agree" button
                'button[aria-label="Accept all"]'
            ]
            
            for selector in cookie_selectors:
                try:
                    cookie_button = await page.wait_for_selector(selector, timeout=3000)
                    if cookie_button:
                        print("Accepting cookies...")
                        # Human-like click
                        await cookie_button.scroll_into_view_if_needed()
                        await asyncio.sleep(random.uniform(0.5, 1.0))
                        await cookie_button.click()
                        await asyncio.sleep(random.uniform(2, 4))
                        return True
                except:
                    continue
            
            return False
        except Exception as e:
            print(f"Cookie handling error: {e}")
            return False

    # 🆕 NEW: AI MODE BUTTON FALLBACK LOGIC
    async def _find_and_click_ai_mode_button(self, page):
        """Find and click the AI Mode button at the top of the search results page"""
        self.log("🔍 Looking for AI Mode button at the top of search results...")
        
        # Add human-like behavior before clicking
        await self._human_like_navigation(page)
        await asyncio.sleep(random.uniform(1, 2))
        
        # Enhanced selectors for AI Mode button (top navigation area)
        ai_mode_selectors = [
            'button:has-text("AI Mode")',
            'div[role="button"]:has-text("AI Mode")',
            'a:has-text("AI Mode")',
            'span:has-text("AI Mode")',
            'button[aria-label*="AI Mode"]',
            'div[aria-label*="AI Mode"]',
            'button[title*="AI Mode"]',
            'div[title*="AI Mode"]',
            '[data-testid*="ai-mode"]',
            '[data-testid*="aimode"]',
            # More specific Google selectors
            '.gb_A:has-text("AI Mode")',  # Google navigation
            '.hdtb-mitem:has-text("AI Mode")',  # Google search tabs
            '.MQsxWb:has-text("AI Mode")',  # Google search navigation
            # Generic text-based search in visible elements
            '*:visible:has-text("AI Mode")',
        ]
        
        # Try direct selectors first
        for selector in ai_mode_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    element_text = await element.text_content()
                    if element_text and 'ai mode' in element_text.lower():
                        self.log(f"✅ Found AI Mode button with selector: {selector}, text: '{element_text}'")
                        
                        # Human-like interaction
                        await element.scroll_into_view_if_needed()
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        # Move mouse to element first
                        box = await element.bounding_box()
                        if box:
                            await page.mouse.move(
                                box['x'] + box['width'] / 2,
                                box['y'] + box['height'] / 2
                            )
                        await asyncio.sleep(random.uniform(0.3, 0.7))
                        
                        # Try multiple click methods with human timing
                        click_success = await self._try_multiple_click_methods_human(page, element)
                        
                        if click_success:
                            self.log("✅ AI Mode button clicked successfully!")
                            await asyncio.sleep(random.uniform(3, 6))
                            return True
                        
            except Exception as e:
                self.log(f"AI Mode selector {selector} failed: {e}")
                continue
        
        # 🆕 FALLBACK: Try to find by searching all clickable elements
        self.log("🔍 Fallback: Searching all clickable elements for AI Mode...")
        try:
            # Get all potentially clickable elements
            clickable_elements = await page.query_selector_all('button, div[role="button"], a, span[role="button"], [tabindex], .gb_A, .hdtb-mitem, .MQsxWb')
            
            for element in clickable_elements:
                try:
                    element_text = await element.text_content()
                    if element_text and 'ai mode' in element_text.lower().strip():
                        self.log(f"✅ Found AI Mode in clickable element: '{element_text.strip()}'")
                        
                        # Check if element is visible and interactable
                        is_visible = await element.is_visible()
                        if not is_visible:
                            continue
                            
                        # Human-like interaction
                        await element.scroll_into_view_if_needed()
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        click_success = await self._try_multiple_click_methods_human(page, element)
                        if click_success:
                            self.log("✅ AI Mode button clicked via fallback search!")
                            await asyncio.sleep(random.uniform(3, 6))
                            return True
                            
                except Exception as e:
                    continue
                    
        except Exception as e:
            self.log(f"Fallback search failed: {e}")
        
        self.log("❌ Could not find AI Mode button")
        return False

    # 🔧 ENHANCED: Main extraction flow with AI Mode button fallback
    async def extract_ai_response_with_playwright_crawl4ai_flow(self, query, max_wait_time=120):
        """
        ENHANCED FLOW WITH AI MODE BUTTON FALLBACK:
        Primary: Show More → Dive Deeper → AI Mode page
        Fallback: Direct AI Mode button click → AI Mode page
        """
        if not PLAYWRIGHT_AVAILABLE:
            self.log("❌ Playwright not available")
            return await self.extract_ai_response_fallback(query, max_wait_time)
        
        if not CRAWL4AI_AVAILABLE:
            self.log("❌ Crawl4AI not available")
            return await self.extract_ai_response_fallback(query, max_wait_time)
        
        if not SBR_WS_CDP:
            self.log("❌ Bright Data credentials not available")
            return await self.extract_ai_response_fallback(query, max_wait_time)
        
        results = {
            'query': query,
            'success': False,
            'ai_response': '',
            'files': {},
            'metadata': {},
            'error': None,
            'approach_used': 'playwright_brightdata_crawl4ai_with_aimode_fallback',
            'debug_screenshots': [],
            'captcha_detected': False,
            'retry_count': 0,
            'navigation_method': None  # Track which method was used
        }
        
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            async with async_playwright() as playwright:
                browser = None
                try:
                    self.log(f"🚀 Starting ENHANCED AI Mode extraction with AI Mode button fallback (Attempt {retry_count + 1}) for: '{query}'")
                    
                    # STEP 1: Playwright launches
                    self.log('🚀 Step 1: Launching Playwright with Bright Data connection...')
                    session_cdp = f"{SBR_WS_CDP}&session_id={self.session_id}_{retry_count}"
                    browser = await playwright.chromium.connect_over_cdp(session_cdp)
                    
                    context = await browser.new_context(
                        viewport={'width': 1366, 'height': 768},
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        locale='en-US',
                        timezone_id='America/New_York',
                        permissions=['geolocation'],
                        geolocation={'latitude': 40.7128, 'longitude': -74.0060},
                    )
                    
                    page = await context.new_page()
                    await page.set_extra_http_headers({
                        'Accept-Language': 'en-US,en;q=0.9'
                    })
                    
                    # Add human behavior scripts
                    await self._add_human_behavior(page)
                    
                    # STEP 2: Navigate to Google search
                    self.log('🌐 Step 2: Navigate to Google search...')
                    await page.goto('https://www.google.com/search?q=&hl=en&gl=US',
                                    wait_until='networkidle', timeout=60000)
                    
                    # Check for CAPTCHA immediately
                    if await self._check_for_captcha(page):
                        results['captcha_detected'] = True
                        raise Exception("CAPTCHA detected on initial page load")
                    
                    # Add human-like behavior before interacting
                    await self._human_like_navigation(page)
                    await asyncio.sleep(random.uniform(3, 6))
                    
                    # Handle cookie consent with human behavior
                    await self.handle_cookie_consent(page)
                    await asyncio.sleep(random.uniform(2, 4))
                    
                    # Take screenshot of search page
                    homepage_screenshot = os.path.join(self.downloads_path, f"01_search_page_attempt_{retry_count + 1}.png")
                    await page.screenshot(path=homepage_screenshot, full_page=True)
                    results['debug_screenshots'].append(homepage_screenshot)
                    self.log(f"📸 Search page screenshot: {homepage_screenshot}")
                    
                    # STEP 3: Enter search query
                    self.log("📝 Step 3: Enter search query...")
                    input_selectors = [
                        'input[name="q"]',
                        'textarea[name="q"]',
                        '#APjFqb',
                        '.gLFyf',
                        'input[title="Search"]',
                        'textarea[title="Search"]',
                        'input[type="text"]'
                    ]
                    
                    input_element = None
                    for selector in input_selectors:
                        try:
                            input_element = await page.wait_for_selector(selector, timeout=10000)
                            if input_element:
                                self.log(f"✅ Found search input using selector: {selector}")
                                break
                        except:
                            continue
                    
                    if input_element:
                        # Human-like typing
                        self.log(f"💬 Human-like typing of query: '{query}'")
                        await input_element.wait_for_element_state('stable', timeout=10000)
                        await input_element.click()
                        await page.keyboard.press('Control+a')
                        await asyncio.sleep(random.uniform(0.3, 0.7))
                        await self._human_like_typing(page, input_element, query)
                        
                        # Take screenshot after typing query
                        query_typed_screenshot = os.path.join(self.downloads_path, f"02_query_typed_attempt_{retry_count + 1}.png")
                        await page.screenshot(path=query_typed_screenshot, full_page=True)
                        results['debug_screenshots'].append(query_typed_screenshot)
                        
                        # Human-like search submission
                        await asyncio.sleep(random.uniform(1, 3))
                        self.log("🔤 Human-like search submission...")
                        await page.keyboard.press('Enter')
                        await asyncio.sleep(random.uniform(3, 5))
                    
                    # Check for CAPTCHA after search
                    if await self._check_for_captcha(page):
                        results['captcha_detected'] = True
                        raise Exception("CAPTCHA detected after search")
                    
                    # STEP 4: Wait for search results to load
                    self.log("⏳ Step 4: Waiting for search results...")
                    try:
                        await page.wait_for_selector('.g, [data-ved], .yuRUbf', timeout=30000)
                        self.log("✅ Search results loaded")
                    except:
                        self.log("⚠️ Search results may not have loaded completely")
                    
                    await asyncio.sleep(random.uniform(5, 8))
                    
                    # Take screenshot of search results
                    search_results_screenshot = os.path.join(self.downloads_path, f"03_search_results_attempt_{retry_count + 1}.png")
                    await page.screenshot(path=search_results_screenshot, full_page=True)
                    results['debug_screenshots'].append(search_results_screenshot)
                    
                    # 🔧 ENHANCED: TRY PRIMARY METHOD FIRST (Show More → Dive Deeper)
                    primary_success = False
                    
                    try:
                        self.log("🎯 TRYING PRIMARY METHOD: Show More → Dive Deeper in AI Mode")
                        
                        # STEP 5: Look for and click Show More
                        self.log("🔍 Step 5: Looking for 'Show more' button...")
                        await self._human_like_navigation(page)
                        show_more_found = await self._find_and_click_show_more(page)
                        
                        if show_more_found:
                            self.log("✅ Show more clicked successfully")
                            await asyncio.sleep(random.uniform(3, 5))
                            
                            # Take screenshot after show more
                            show_more_screenshot = os.path.join(self.downloads_path, f"04_after_show_more_attempt_{retry_count + 1}.png")
                            await page.screenshot(path=show_more_screenshot, full_page=True)
                            results['debug_screenshots'].append(show_more_screenshot)
                            
                            # STEP 6: Look for and click Dive Deeper in AI Mode
                            self.log("🔍 Step 6: Looking for 'Dive deeper in AI Mode' button...")
                            dive_deeper_found = await self._find_and_click_dive_deeper(page)
                            
                            if dive_deeper_found:
                                self.log("✅ Dive deeper in AI Mode clicked successfully")
                                results['navigation_method'] = 'show_more_dive_deeper'
                                primary_success = True
                                
                                # Take screenshot after clicking dive deeper
                                dive_deeper_screenshot = os.path.join(self.downloads_path, f"05_after_dive_deeper_attempt_{retry_count + 1}.png")
                                await page.screenshot(path=dive_deeper_screenshot, full_page=True)
                                results['debug_screenshots'].append(dive_deeper_screenshot)
                            else:
                                self.log("❌ Could not find 'Dive deeper in AI Mode' button")
                        else:
                            self.log("❌ Could not find 'Show more' button")
                            
                    except Exception as primary_error:
                        self.log(f"❌ Primary method failed: {primary_error}")
                    
                    # 🆕 FALLBACK METHOD: Direct AI Mode Button Click
                    if not primary_success:
                        self.log("🔄 PRIMARY METHOD FAILED - TRYING FALLBACK: Direct AI Mode Button Click")
                        
                        try:
                            # STEP 5 FALLBACK: Click AI Mode button directly
                            self.log("🔍 Step 5 FALLBACK: Looking for AI Mode button at top of page...")
                            ai_mode_found = await self._find_and_click_ai_mode_button(page)
                            
                            if ai_mode_found:
                                self.log("✅ AI Mode button clicked successfully (FALLBACK METHOD)")
                                results['navigation_method'] = 'direct_ai_mode_button'
                                
                                # Take screenshot after clicking AI Mode button
                                ai_mode_button_screenshot = os.path.join(self.downloads_path, f"05_after_ai_mode_button_attempt_{retry_count + 1}.png")
                                await page.screenshot(path=ai_mode_button_screenshot, full_page=True)
                                results['debug_screenshots'].append(ai_mode_button_screenshot)
                            else:
                                raise Exception("Could not find AI Mode button (both primary and fallback methods failed)")
                                
                        except Exception as fallback_error:
                            self.log(f"❌ Fallback method also failed: {fallback_error}")
                            raise Exception(f"Both primary and fallback navigation methods failed. Primary: Show More→Dive Deeper failed. Fallback: Direct AI Mode button failed.")
                    
                    # 🔧 STEP 7: CRITICAL FIX - Wait for AI Mode page navigation and loading
                    self.log("⏳ Step 7: FIXED - Waiting for AI Mode page to load completely...")
                    
                    # Wait for URL change indicating AI Mode navigation
                    await self._wait_for_ai_mode_navigation(page, max_wait_time=30)
                    
                    # Wait for AI Mode content to load and complete processing
                    ai_completed = await self._wait_for_ai_completion_playwright(page, max_wait_time)
                    
                    if not ai_completed:
                        self.log("⚠️ AI Mode processing may not have completed, but continuing...")
                    
                    # Take final screenshot of AI Mode page
                    final_screenshot = os.path.join(self.downloads_path, f"06_final_ai_mode_page_attempt_{retry_count + 1}.png")
                    await page.screenshot(path=final_screenshot, full_page=True)
                    results['debug_screenshots'].append(final_screenshot)
                    
                    # STEP 8: Extract HTML content from AI Mode page
                    self.log('📄 Step 8: Extracting HTML content from AI Mode page...')
                    final_html = await page.content()
                    
                    if not final_html or len(final_html) < 1000:
                        raise Exception("Insufficient HTML content extracted from AI Mode page")
                    
                    # Verify we're on AI Mode page by checking content
                    page_text = await page.evaluate("() => document.body.innerText.toLowerCase()")
                    ai_mode_indicators = [
                        'ai mode', 'generated by ai', 'ai response',
                        'sources and references', 'based on', 'according to'
                    ]
                    
                    if not any(indicator in page_text for indicator in ai_mode_indicators):
                        self.log("⚠️ Warning: Page content doesn't appear to be from AI Mode")
                    else:
                        self.log("✅ Confirmed: Extracting from AI Mode page")
                    
                    self.log(f"✅ Extracted HTML content ({len(final_html)} characters)")
                    
                    # STEP 9: 🔧 FIXED - Process with Crawl4AI using file-based approach
                    self.log('🔧 Step 9: FIXED - Processing HTML with Crawl4AI using file-based approach...')
                    
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    markdown_result = await self._process_html_with_crawl4ai_file_based(
                        final_html, query, page.url, timestamp
                    )
                    
                    if not markdown_result:
                        raise Exception("Crawl4AI failed to process AI Mode HTML")
                    
                    # STEP 10: Save results with enhanced output and intelligent filtering
                    success = await self._save_enhanced_crawl4ai_results_intelligent(
                        query, final_html, markdown_result, results, timestamp, page.url
                    )
                    
                    if success:
                        results['success'] = True
                        results['retry_count'] = retry_count
                        self.log(f"🎉 Successfully completed ENHANCED AI Mode extraction with {results['navigation_method']} method!")
                        return results
                    else:
                        raise Exception("Failed to save enhanced results")
                    
                except Exception as e:
                    self.log(f"❌ Attempt {retry_count + 1} failed: {str(e)}")
                    if "CAPTCHA" in str(e) and retry_count < max_retries:
                        self.log(f"🔄 CAPTCHA detected, retrying with new session (attempt {retry_count + 2})...")
                        retry_count += 1
                        wait_time = random.uniform(30, 60) * (retry_count + 1)
                        self.log(f"⏳ Waiting {wait_time:.1f} seconds before retry...")
                        await asyncio.sleep(wait_time)
                        self.session_id = str(uuid.uuid4())
                        continue
                    else:
                        results['error'] = str(e)
                        results['retry_count'] = retry_count
                        return results
                        
                finally:
                    try:
                        if browser:
                            await browser.close()
                        self.log("🔌 Browser connection closed")
                    except:
                        pass
        
        # If we exhausted all retries
        results['error'] = f"Failed after {max_retries + 1} attempts due to CAPTCHA"
        results['retry_count'] = retry_count
        return results

    # 🔧 NEW: Wait for AI Mode navigation (URL change)
    async def _wait_for_ai_mode_navigation(self, page, max_wait_time=30):
        """Wait for page to navigate to AI Mode (URL change or content change)"""
        self.log("⏳ Waiting for AI Mode page navigation...")
        
        wait_time = 0
        check_interval = 1
        
        while wait_time < max_wait_time:
            try:
                current_url = page.url
                page_text = await page.evaluate("() => document.body.innerText.toLowerCase()")
                
                # Check for AI Mode URL patterns
                ai_mode_url_indicators = [
                    'udm=6', 'ai_mode', 'gemini', 'aimode'
                ]
                
                # Check for AI Mode content indicators  
                ai_mode_content_indicators = [
                    'ai mode response', 'generated by ai',
                    'thinking', 'analyzing', 'processing'
                ]
                
                url_has_ai_mode = any(indicator in current_url.lower() for indicator in ai_mode_url_indicators)
                content_has_ai_mode = any(indicator in page_text for indicator in ai_mode_content_indicators)
                
                if url_has_ai_mode or content_has_ai_mode:
                    self.log(f"✅ AI Mode navigation detected (URL: {url_has_ai_mode}, Content: {content_has_ai_mode})")
                    await asyncio.sleep(2)  # Give it a moment to stabilize
                    return True
                
                self.log(f"⏳ Waiting for AI Mode navigation... {wait_time}s")
                await asyncio.sleep(check_interval)
                wait_time += check_interval
                
            except Exception as e:
                self.log(f"Error during AI Mode navigation check: {e}")
                break
        
        self.log("⚠️ AI Mode navigation timeout reached")
        return False

    # 🔧 FIXED: File-based Crawl4AI processing (following brightdata.py approach)
    async def _process_html_with_crawl4ai_file_based(self, html_content, query, page_url, timestamp):
        """Process HTML with Crawl4AI using file-based approach like brightdata.py.
        This ensures Crawl4AI processes the exact HTML extracted by Playwright.
        """
        if not CRAWL4AI_AVAILABLE:
            return None
        
        try:
            self.log("🔧 FIXED - Using file-based Crawl4AI processing like brightdata.py...")
            
            # Create complete HTML document with proper structure
            complete_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Google AI Mode Response - {query}</title>
    <base href="https://www.google.com/">
</head>
<body>
    <div class="ai-mode-content">
        {html_content}
    </div>
</body>
</html>"""
            
            # 🔧 KEY FIX: Save HTML to file and use file:// URL (like brightdata.py)
            temp_html_path = os.path.join(ephemeral_base, f"temp_ai_mode_response_{timestamp}.html")
            with open(temp_html_path, 'w', encoding='utf-8') as f:
                f.write(complete_html)
            
            self.log(f"💾 Saved AI Mode HTML to temp file: {temp_html_path}")
            self.log(f"📊 HTML file size: {os.path.getsize(temp_html_path)} bytes")
            
            # Use minimal filtering to preserve maximum content
            content_filter = PruningContentFilter(
                threshold=0.15,  # Very light filtering to preserve content
                threshold_type="fixed",
                min_word_threshold=5  # Lower threshold for more content
            )
            
            # Configure markdown generator with optimal settings
            md_generator = DefaultMarkdownGenerator(
                content_filter=content_filter,
                options={
                    "ignore_links": False,  # ✅ PRESERVE ALL CLICKABLE LINKS
                    "escape_html": False,
                    "body_width": 0,  # No text wrapping
                    "skip_internal_links": False,
                    "include_sup_sub": True,
                }
            )
            
            # Configure browser for Crawl4AI
            browser_config = BrowserConfig(
                headless=True,
                java_script_enabled=True
            )
            
            # Configure crawler with cache bypass to ensure fresh processing
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                markdown_generator=md_generator,
            )
            
            self.log("🌐 Processing AI Mode HTML file with Crawl4AI...")
            
            # 🔧 CRITICAL FIX: Use file:// URL like brightdata.py
            file_url = f"file://{os.path.abspath(temp_html_path)}"
            self.log(f"🔗 Processing file URL: {file_url}")
            
            # Process with Crawl4AI
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(
                    url=file_url,
                    config=crawler_config
                )
                
                if not (result and result.success and result.markdown):
                    error_msg = getattr(result, 'error_message', 'Unknown error') if result else 'No result returned'
                    self.log(f"❌ Crawl4AI processing failed: {error_msg}")
                    return None
                
                # Extract raw markdown from Crawl4AI
                raw_md = (
                    result.markdown.raw_markdown 
                    if hasattr(result.markdown, 'raw_markdown') 
                    else str(result.markdown)
                ).strip()
                
                self.log(f"✅ Crawl4AI extracted markdown from file ({len(raw_md)} characters)")
                
                # Extract links for sources section
                links_list = []
                if result.links:
                    internal_links = result.links.get("internal", [])
                    external_links = result.links.get("external", [])
                    
                    # Combine all links and filter for external sources only
                    all_links = internal_links + external_links
                    for link in all_links:
                        if isinstance(link, dict):
                            href = link.get('href', '')
                            text = link.get('text', '')
                            # Only include external links that aren't Google internal
                            if (href and text and 
                                'google.com' not in href.lower() and 
                                len(text.strip()) > 2):
                                links_list.append({'text': text.strip(), 'url': href})
                
                self.log(f"🔗 Found {len(links_list)} external links")
                
                # Clean up temporary file
                try:
                    if os.path.exists(temp_html_path):
                        os.remove(temp_html_path)
                        self.log("🗑️ Cleaned up temporary HTML file")
                except Exception as cleanup_error:
                    self.log(f"⚠️ Failed to cleanup temp file: {cleanup_error}")
                
                return {
                    'raw_markdown': raw_md,
                    'links_found': links_list,
                    'crawl4ai_result': result
                }
                
        except Exception as e:
            self.log(f"❌ File-based Crawl4AI extraction error: {str(e)}")
            # Clean up temp file on error
            try:
                temp_html_path = os.path.join(ephemeral_base, f"temp_ai_mode_response_{timestamp}.html")
                if os.path.exists(temp_html_path):
                    os.remove(temp_html_path)
            except:
                pass
            return None

    # === MODIFIED: SAVE FUNCTION WITH INTELLIGENT CONTENT EXTRACTION AND LINK FIXES ===
    async def _save_enhanced_crawl4ai_results_intelligent(self, query, final_html, markdown_data, results, timestamp, page_url):
        """Save enhanced Crawl4AI results with intelligent content extraction and improved link handling"""
        
        try:
            self.log("💾 Saving enhanced Crawl4AI results with intelligent extraction...")
            
            # Extract raw markdown from the result
            raw_markdown = markdown_data.get('raw_markdown', '')
            links_found = markdown_data.get('links_found', [])
            
            # --- USE THE SMART ANSWER EXTRACTOR BEFORE SAVING ---
            cleaned_markdown = extract_real_aimode_answer(raw_markdown)
            
            # --- IMPROVED SIDEBAR LINK EXTRACTION ---
            sidebar_sources = []
            for link in links_found:
                href = link.get('url', '').strip()
                text = link.get('text', '').strip()
                
                if not href or href.startswith('#') or href.startswith('javascript:'):
                    continue
                
                # Replace bad/empty link text with domain name or "Link"
                if not text or text in ('•', '.', '-', '–', '—') or len(text) < 2:
                    try:
                        domain = urllib.parse.urlparse(href).netloc
                        text = domain if domain else "Link"
                    except:
                        text = "Link"
                
                text = re.sub(r'\s+', ' ', text).strip()
                if len(text) > 100:
                    text = text[:97] + "..."
                
                sidebar_sources.append((text, href))
            
            # Remove duplicate links
            seen = set()
            unique_sources = []
            for title, url in sidebar_sources:
                if url not in seen:
                    seen.add(url)
                    unique_sources.append((title, url))
            
            sources_markdown = '\n'.join([f"- [{title}]({url})" for title, url in unique_sources[:20]])
            
            enhanced_markdown = f"""# Google AI Mode Response

**Query:** {query}  
**Timestamp:** {timestamp}  
**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
**Navigation Method:** {results.get('navigation_method', 'unknown')}

---

## AI Mode Response Content

{cleaned_markdown}

---

## Sources

{sources_markdown}

---

## Metadata

- **Extraction Method:** Enhanced AI Mode Navigation + File-based Crawl4AI + Intelligent Content Filtering
- **Navigation Strategy:** {results.get('navigation_method', 'unknown')}
- **Response Length:** {len(cleaned_markdown)} characters
- **Links Found:** {len(unique_sources)}
- **Status:** Success
- **Source:** {page_url}
- **Intelligent Filtering:** ✅ Applied

---

*Generated by Google AI Mode Automation System (Enhanced Version with AI Mode Button Fallback)*

"""
            
            # Generate safe filenames
            safe_query = "".join(c for c in query if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
            nav_method = results.get('navigation_method', 'unknown')
            file_prefix = f"{timestamp}_{nav_method}_ai_mode_{safe_query.replace(' ', '_')}"
            
            # Save files
            markdown_path = os.path.join(self.downloads_path, f"{file_prefix}_response.md")
            html_path = os.path.join(self.downloads_path, f"{file_prefix}_response.html")
            raw_md_path = os.path.join(self.downloads_path, f"{file_prefix}_raw.md")
            
            with open(markdown_path, 'w', encoding='utf-8') as f:
                f.write(enhanced_markdown)
            
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(final_html)
            
            with open(raw_md_path, 'w', encoding='utf-8') as f:
                f.write(raw_markdown)
            
            # Save links as JSON for reference
            if unique_sources:
                links_path = os.path.join(self.downloads_path, f"{file_prefix}_links.json")
                with open(links_path, "w", encoding="utf-8") as f:
                    json.dump([{"title": title, "url": url} for title, url in unique_sources], f, indent=2, ensure_ascii=False)
                results['files']['links'] = links_path
            
            results['files']['markdown'] = markdown_path
            results['files']['html'] = html_path
            results['files']['raw_markdown'] = raw_md_path
            results['ai_response'] = cleaned_markdown
            
            results['metadata'] = {
                'query': query,
                'timestamp': timestamp,
                'html_length': len(final_html),
                'structured_markdown_length': len(enhanced_markdown),
                'raw_markdown_length': len(raw_markdown),
                'cleaned_markdown_length': len(cleaned_markdown),
                'external_sources_found': len(unique_sources),
                'url': page_url,
                'approach': 'enhanced_ai_mode_navigation_with_fallback',
                'navigation_method': results.get('navigation_method', 'unknown'),
                'crawl4ai_used': True,
                'playwright_used': True,
                'brightdata_used': True,
                'file_based_processing': True,
                'intelligent_filtering': True,
                'chatgpt_style_formatting': True,
                'session_id': self.session_id,
                'fallback_logic': True
            }
            
            self.log(f"✅ Enhanced markdown saved: {markdown_path}")
            self.log(f"✅ HTML saved: {html_path}")
            self.log(f"✅ Raw markdown saved: {raw_md_path}")
            if unique_sources:
                self.log(f"✅ Links JSON saved: {results['files']['links']}")
            self.log(f"📊 Original response: {len(raw_markdown)} chars → Cleaned: {len(cleaned_markdown)} chars")
            self.log(f"🔗 Links extracted: {len(unique_sources)}")
            self.log(f"🧠 Intelligent filtering applied: ✅")
            self.log(f"🔄 Navigation method used: {results.get('navigation_method', 'unknown')}")
            
            return True
            
        except Exception as e:
            self.log(f"❌ Error saving enhanced results: {str(e)}")
            return False

    # Keep all existing helper methods for Show More and Dive Deeper
    async def _find_and_click_show_more(self, page):
        """Find and click the 'Show more' button with human-like behavior"""
        self.log("🔍 Scanning page for 'Show more' button with human behavior...")
        
        # Add some human browsing behavior first
        await self._human_like_navigation(page)
        await asyncio.sleep(random.uniform(1, 2))
        
        # Enhanced selectors for "Show more" button
        show_more_selectors = [
            'button:has-text("Show more")',
            'div[role="button"]:has-text("Show more")',
            'a:has-text("Show more")',
            'span:has-text("Show more")',
            'button[aria-label*="Show more"]',
            'div[aria-label*="Show more"]',
            'button[title*="Show more"]',
            'div[title*="Show more"]',
            '[data-ved*="show"]',
            '[data-testid*="show"]',
            '[data-id*="show"]'
        ]
        
        # Try direct selectors first
        for selector in show_more_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    element_text = await element.text_content()
                    if element_text and 'show more' in element_text.lower():
                        self.log(f"✅ Found show more button with selector: {selector}, text: '{element_text}'")
                        
                        # Human-like interaction
                        await element.scroll_into_view_if_needed()
                        await asyncio.sleep(random.uniform(1, 2))
                        
                        # Move mouse to element first
                        box = await element.bounding_box()
                        if box:
                            await page.mouse.move(
                                box['x'] + box['width'] / 2,
                                box['y'] + box['height'] / 2
                            )
                        await asyncio.sleep(random.uniform(0.3, 0.7))
                        
                        # Try multiple click methods with human timing
                        click_success = await self._try_multiple_click_methods_human(page, element)
                        
                        if click_success:
                            await asyncio.sleep(random.uniform(2, 4))
                            return True
                        
            except Exception as e:
                self.log(f"Selector {selector} failed: {e}")
                continue
        
        return False

    async def _find_and_click_dive_deeper(self, page):
        """Find and click the 'Dive deeper in AI Mode' button with human behavior"""
        self.log("🔍 Scanning page for 'Dive deeper in AI Mode' button with human behavior...")
        
        # Add human browsing behavior
        await self._human_like_navigation(page)
        await asyncio.sleep(random.uniform(1, 2))
        
        # Enhanced selectors for "Dive deeper" button
        dive_deeper_selectors = [
            'button:has-text("Dive deeper in AI Mode")',
            'div[role="button"]:has-text("Dive deeper in AI Mode")',
            'button:has-text("Dive deeper")',
            'div[role="button"]:has-text("Dive deeper")',
            'a:has-text("Dive deeper in AI Mode")',
            'a:has-text("Dive deeper")',
            'button[aria-label*="Dive deeper"]',
            'div[aria-label*="Dive deeper"]',
            'button[title*="Dive deeper"]',
            'div[title*="Dive deeper"]',
            '[data-ved*="dive"]',
            '[data-testid*="dive"]',
            '[data-id*="dive"]'
        ]
        
        # Try direct selectors first
        for selector in dive_deeper_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    element_text = await element.text_content()
                    self.log(f"✅ Found dive deeper button with selector: {selector}, text: '{element_text}'")
                    
                    # Human-like interaction
                    await element.scroll_into_view_if_needed()
                    await asyncio.sleep(random.uniform(1, 2))
                    
                    # Move mouse to element
                    box = await element.bounding_box()
                    if box:
                        await page.mouse.move(
                            box['x'] + box['width'] / 2,
                            box['y'] + box['height'] / 2
                        )
                    await asyncio.sleep(random.uniform(0.3, 0.7))
                    
                    click_success = await self._try_multiple_click_methods_human(page, element)
                    
                    if click_success:
                        await asyncio.sleep(random.uniform(3, 6))
                        return True
                        
            except Exception as e:
                self.log(f"Selector {selector} failed: {e}")
                continue
        
        return False

    async def _try_multiple_click_methods_human(self, page, element):
        """Try multiple click methods with human-like timing"""
        click_methods = [
            lambda: self._human_click(page, element),
            lambda: self._delayed_click(element),
            lambda: page.evaluate('(element) => element.click()', element),
            lambda: self._click_by_coordinates_human(page, element),
            lambda: self._focus_and_enter(page, element)
        ]
        
        for i, method in enumerate(click_methods, 1):
            try:
                self.log(f"Trying human-like click method {i}...")
                await method()
                await asyncio.sleep(random.uniform(0.5, 1.5))
                return True
            except Exception as e:
                self.log(f"Click method {i} failed: {str(e)[:100]}")
                await asyncio.sleep(random.uniform(0.3, 0.8))
                continue
        
        return False

    async def _human_click(self, page, element):
        """Most human-like click method"""
        box = await element.bounding_box()
        if box:
            target_x = box['x'] + box['width'] / 2 + random.uniform(-5, 5)
            target_y = box['y'] + box['height'] / 2 + random.uniform(-3, 3)
            await page.mouse.move(target_x, target_y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            await page.mouse.down()
            await asyncio.sleep(random.uniform(0.05, 0.15))
            await page.mouse.up()

    async def _delayed_click(self, element):
        """Standard click with human delay"""
        await asyncio.sleep(random.uniform(0.2, 0.5))
        await element.click()

    async def _click_by_coordinates_human(self, page, element):
        """Click by coordinates with human variation"""
        box = await element.bounding_box()
        if box:
            x = box['x'] + box['width'] / 2 + random.uniform(-2, 2)
            y = box['y'] + box['height'] / 2 + random.uniform(-2, 2)
            await page.mouse.click(x, y)

    async def _focus_and_enter(self, page, element):
        """Focus and press Enter with human timing"""
        await element.focus()
        await asyncio.sleep(random.uniform(0.3, 0.7))
        await page.keyboard.press('Enter')

    async def _wait_for_ai_completion_playwright(self, page, max_wait_time):
        """Wait for AI processing to complete with human-like monitoring"""
        self.log("⏳ Monitoring AI Mode processing completion with human behavior...")
        
        wait_time = 0
        check_interval = random.uniform(2, 4)
        max_wait_seconds = max_wait_time
        
        while wait_time < max_wait_seconds:
            try:
                if random.random() < 0.3:
                    await self._human_like_navigation(page)
                
                page_text = await page.evaluate("() => document.body.innerText.toLowerCase()")
                
                completion_indicators = [
                    'here are', 'here is', 'based on', 'according to',
                    'putting it all together', 'in summary', 'conclusion',
                    'comparison', 'features', 
                    'sources and references'
                ]
                
                processing_indicators = [
                    'thinking', 'kicking off', 'looking at', 'processing',
                    'analyzing', 'generating', 'loading'
                ]
                
                has_completion = any(indicator in page_text for indicator in completion_indicators)
                is_processing = any(indicator in page_text for indicator in processing_indicators)
                
                self.log(f"⏰ Wait {wait_time:.1f}s - Processing: {is_processing}, Complete: {has_completion}")
                
                if has_completion and len(page_text) > 2000:
                    self.log("🎉 AI Mode processing complete!")
                    await asyncio.sleep(random.uniform(3, 6))
                    return True
                
                next_check = random.uniform(2, 5)
                await asyncio.sleep(next_check)
                wait_time += next_check
                
            except Exception as e:
                self.log(f"Error during AI completion check: {e}")
                break
        
        self.log("⏰ Max wait time reached for AI Mode completion")
        return False

    # Main extraction method (updated to use enhanced flow with fallback)
    async def extract_ai_response(self, query, max_wait_time=120):
        """Main extraction method using ENHANCED AI Mode navigation with AI Mode button fallback"""
        
        if self.use_brightdata and PLAYWRIGHT_AVAILABLE and CRAWL4AI_AVAILABLE and SBR_WS_CDP:
            self.log("🎯 Using ENHANCED AI Mode Navigation Pipeline with AI Mode Button Fallback")
            return await self.extract_ai_response_with_playwright_crawl4ai_flow(query, max_wait_time)
        else:
            self.log("🔄 Using fallback approach")
            return await self.extract_ai_response_fallback(query, max_wait_time)

    # Fallback method (keep existing logic)
    async def extract_ai_response_fallback(self, query, max_wait_time=120):
        """Fallback method if dependencies are not available"""
        return {
            'query': query,
            'success': False,
            'ai_response': '',
            'files': {},
            'metadata': {},
            'error': "Required dependencies not available (Playwright, Crawl4AI, or Bright Data)",
            'approach_used': 'fallback'
        }

# Keep all existing database and utility functions unchanged
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
            port=secret['DB_PORT'],
            connect_timeout=30,
            application_name='lambda-crawler'
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

def create_llm_task(job_id, query_id, llm_model_name="ai-mode", product_id=None, product_name=None):
    logger.info(f"Creating LLM task for job_id: {job_id}, query_id: {query_id}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        task_id = str(uuid.uuid4())
        # Use query_id directly as integer (matches ChatGPT worker pattern)
        
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
                product_name
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                task_id,
                job_id,
                query_id,
                llm_model_name,
                "created",
                datetime.now(timezone.utc),
                product_id,
                product_name
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Inserted LLM task to DB -- task_id: {task_id}")
        return task_id
        
    except Exception as e:
        logger.error(f"Error creating LLM task: {e}")
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
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Updated task {task_id} status to {status}")
        
    except Exception as e:
        logger.error(f"Error updating task status: {e}")
        raise

def log_orchestration_event(job_id, event_name, details=None):
    """Log an orchestration event to DynamoDB."""
    table_name = os.environ.get('ORCHESTRATION_LOGS_TABLE', 'OrchestrationLogs')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    timestamp = datetime.now(timezone.utc).isoformat()
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
        logger.info(f"Logged orchestration event: {event_name}")
    except Exception as e:
        logger.error(f"Error logging orchestration event: {e}")

def upload_to_s3(content, bucket_name, s3_key, content_type='text/plain'):
    """Generic S3 upload function (old format)"""
    try:
        s3_client = boto3.client('s3')
        
        if isinstance(content, str):
            content = content.encode('utf-8')
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=content,
            ContentType=content_type
        )
        
        print(f"✅ Uploaded to S3: s3://{bucket_name}/{s3_key}")
        return s3_key
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"❌ S3 Error ({error_code}): {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected S3 upload error: {e}")
        return None

def upload_to_new_bucket_structure(content, job_id, product_id, query_id, content_type='application/json', file_extension='json'):
    """Upload content to new bucket structure: job_id/product_id/aim_query_{query_id}.json"""
    try:
        new_bucket = 'bodhium-temp'
        s3_client = boto3.client('s3')
        
        # Create new S3 key following the required structure with query_id
        s3_key = f"{job_id}/{product_id}/aim_query_{query_id}.{file_extension}"
        
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

async def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(description='Google AI Mode Extractor - ENHANCED with AI Mode Button Fallback')
    parser.add_argument('query', nargs='?', help='Search query for AI Mode')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--max-wait', type=int, default=120, help='Maximum wait time (seconds)')
    parser.add_argument('--quiet', action='store_true', help='Suppress verbose output')
    parser.add_argument('--no-brightdata', action='store_true', help='Disable Bright Data')

    args = parser.parse_args()

    if args.query:
        query = args.query
    else:
        query = input("Enter your query for Google AI Mode: ").strip()

    if not query:
        print("❌ No query provided. Exiting.")
        return

    extractor = GoogleAIModeExtractor(
        headless=args.headless,
        verbose=not args.quiet,
        use_brightdata=not args.no_brightdata
    )

    results = await extractor.extract_ai_response(query, args.max_wait)

    print(f"\n{'='*80}")
    print(f"ENHANCED AI MODE GOOGLE EXTRACTOR RESULTS (WITH AI MODE BUTTON FALLBACK)")
    print(f"{'='*80}")
    print(f"Query: {results['query']}")
    print(f"Success: {results['success']}")
    
    if results['success']:
        print(f"Approach Used: {results['approach_used']}")
        print(f"Navigation Method: {results.get('navigation_method', 'unknown')}")
        print(f"\n📊 Metadata:")
        for key, value in results['metadata'].items():
            print(f"  {key}: {value}")
        
        print(f"\n📁 Files saved:")
        for file_type, path in results['files'].items():
            print(f"  {file_type}: {path}")
            
        print(f"\n🎯 Enhanced AI Mode Features:")
        print(f"  ✅ PRIMARY: Show More → Dive Deeper navigation")
        print(f"  🆕 FALLBACK: Direct AI Mode button click")
        print(f"  ✅ Proper navigation to AI Mode page")
        print(f"  ✅ Waits for AI Mode URL/content change") 
        print(f"  ✅ Waits for AI processing to complete")
        print(f"  ✅ Extracts from actual AI Mode page (not search results)")
        print(f"  ✅ FILE-BASED Crawl4AI processing like brightdata.py")
        print(f"  ✅ HTML saved to temp file, processed with file:// URL")
        print(f"  ✅ INTELLIGENT content filtering removes unwanted content")
        print(f"  ✅ IMPROVED link extraction with proper text handling")
        print(f"  ✅ ChatGPT-style formatting with sources")
        print(f"  ✅ Anti-CAPTCHA measures with human behavior")
        print(f"  ✅ Debugging screenshots for each step")
        print(f"  🆕 DUAL NAVIGATION STRATEGY for maximum success rate")
        
        if results.get('debug_screenshots'):
            print(f"\n📸 Debug Screenshots:")
            for screenshot in results['debug_screenshots']:
                print(f"  - {screenshot}")
        
        if results.get('ai_response'):
            print(f"\n📄 Preview of cleaned AI Mode content:")
            print(f"{results['ai_response'][:500]}...")
    else:
        print(f"❌ Error: {results['error']}")
        if results.get('debug_screenshots'):
            print(f"\n📸 Debug Screenshots available:")
            for screenshot in results['debug_screenshots']:
                print(f"  - {screenshot}")

    print(f"\n🔧 KEY ENHANCEMENTS IMPLEMENTED:")
    print(f"  1. ✅ Intelligent answer extraction (removes UI/navigation noise)")
    print(f"  2. ✅ Improved link text handling (replaces empty text with domain)")
    print(f"  3. ✅ File-based HTML processing (like brightdata.py)")
    print(f"  4. ✅ Smart content pattern recognition")
    print(f"  5. ✅ Enhanced markdown visibility and formatting")
    print(f"  6. ✅ Better source link organization")
    print(f"  🆕 7. DUAL NAVIGATION STRATEGY:")
    print(f"     PRIMARY: Show More → Dive Deeper in AI Mode")
    print(f"     FALLBACK: Direct AI Mode button click")
    print(f"\n🎯 Navigation Flow:")
    print(f"  1. 🌐 Navigate to Google search")
    print(f"  2. 📝 Enter query and search")
    print(f"  3. 🎯 PRIMARY: Try Show More → Dive Deeper")
    print(f"  4. 🔄 FALLBACK: If primary fails, click AI Mode button directly")
    print(f"  5. ⏳ Wait for AI Mode page to load")
    print(f"  6. ⏳ Wait for AI processing to complete")
    print(f"  7. 💾 Save HTML to temporary file")
    print(f"  8. 📄 Process file with Crawl4AI using file:// URL")
    print(f"  9. 🧠 Apply intelligent filtering to extract real AI content")
    print(f"  10. 🔗 Improve link text handling for better visibility")
    print(f"  11. 📝 Format as ChatGPT-style markdown")
    print(f"\n{'='*80}")

def lambda_handler(event, context):
    """AWS Lambda handler"""
    task_id = None
    job_id = None
    
    try:
        print("🎯 Lambda function started (ENHANCED AI Mode Navigation with AI Mode Button Fallback)")
        
        query = event.get('query', 'What is artificial intelligence?')
        job_id = event.get('job_id', str(uuid.uuid4()))
        query_id = event.get('query_id', 1)  # Default to integer like ChatGPT worker
        product_id = event.get('product_id', str(uuid.uuid4()))  # NEW: Extract product_id
        
        print(f"📊 Job ID: {job_id}, Query ID: {query_id}, Product ID: {product_id}")
        
        # Get product name from database
        product_name = get_product_name_from_db(product_id) if product_id else 'Unknown Product'
        
        # Database operations
        try:
            task_id = create_llm_task(job_id, query_id, "GOOGLE_AI_MODE", product_id, product_name)
            update_task_status(task_id, "running")
        except Exception as db_error:
            print(f"⚠️ Database operations failed: {db_error}")
            task_id = None
        
        try:
            log_orchestration_event(job_id, "task_started", {
                "query": query,
                "task_id": task_id,
                "product_id": product_id,  # NEW: Include product_id in logging
                "pipeline": "enhanced_ai_mode_navigation_with_aimode_button_fallback",
                "timestamp": datetime.now().isoformat()
            })
        except Exception as db_error:
            print(f"⚠️ Failed to log orchestration event: {db_error}")
        
        extractor = GoogleAIModeExtractor(
            headless=True,
            verbose=True,
            use_brightdata=True
        )
        
        result = asyncio.run(extractor.extract_ai_response(query))
        
        # NEW: Upload to new bucket structure
        if result and result.get('success'):
            try:
                # Create comprehensive result for new bucket
                aim_result = {
                    "job_id": job_id,
                    "product_id": product_id,
                    "query_id": query_id,
                    "query": query,
                    "timestamp": datetime.now().isoformat(),
                    "model": "GOOGLE_AI_MODE",
                    "content": result.get('ai_response', ''),
                    "metadata": result.get('metadata', {}),
                    "navigation_method": result.get('navigation_method', 'unknown'),
                    "status": "success"
                }
                
                new_bucket_path = upload_to_new_bucket_structure(
                    aim_result, job_id, product_id, query_id, 'application/json', 'json'
                )
                if new_bucket_path:
                    print(f"✅ Uploaded to new bucket structure: {new_bucket_path}")
                    result['new_bucket_path'] = new_bucket_path
                else:
                    print(f"❌ Failed to upload to new bucket structure")
                    
            except Exception as new_bucket_error:
                print(f"❌ Error uploading to new bucket: {new_bucket_error}")
        
        # S3 upload (old format)
        if result and result.get('success'):
            S3_BUCKET = os.environ.get('S3_BUCKET')
            S3_PATH = os.environ.get('S3_PATH', 'google-ai-mode')
            
            if S3_BUCKET:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                query_folder = re.sub(r'[^a-zA-Z0-9\s]', '', query).replace(' ', '_')
                nav_method = result.get('navigation_method', 'unknown')
                folder_name = f"{query_folder}_{nav_method}_{timestamp}"
                
                s3_keys = {}
                for file_type, file_path in result.get('files', {}).items():
                    if os.path.exists(file_path):
                        s3_key = f"{S3_PATH}/{folder_name}/{os.path.basename(file_path)}"
                        
                        try:
                            with open(file_path, 'rb') as f:
                                content = f.read()
                            
                            if file_type == 'html':
                                content_type = 'text/html'
                            elif file_type in ['markdown', 'raw_markdown']:
                                content_type = 'text/markdown'
                            elif file_type == 'links':
                                content_type = 'application/json'
                            else:
                                content_type = 'text/plain'
                            
                            uploaded_key = upload_to_s3(content, S3_BUCKET, s3_key, content_type)
                            
                            if uploaded_key:
                                s3_keys[file_type] = f"s3://{S3_BUCKET}/{uploaded_key}"
                                
                        except Exception as upload_error:
                            print(f"❌ Error uploading {file_type}: {upload_error}")
                
                if s3_keys:
                    result['s3_files'] = s3_keys
            
            print(f"🎯 Enhanced AI Mode navigation extraction complete!")
            print(f"🔄 Navigation method used: {result.get('navigation_method', 'unknown')}")
            print(f"🧠 Key Enhancement: Intelligent content filtering removes unwanted UI elements")
            print(f"🔗 Key Enhancement: Improved link text handling for better visibility")
            print(f"🆕 Key Enhancement: Dual navigation strategy (Primary + AI Mode button fallback)")
            
        # Update task status
        if task_id:
            try:
                if result and result.get('success'):
                    s3_output_path = result.get('s3_files', {}).get('markdown', '')
                    # Use new bucket path instead of old S3 path
                    final_s3_path = new_bucket_path if new_bucket_path else s3_output_path
                    
                    update_task_status(
                        task_id,
                        "completed",
                        s3_output_path=final_s3_path,
                        completed_at=datetime.now(timezone.utc)
                    )
                else:
                    error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                    update_task_status(task_id, "failed", error_message=error_msg)
            except Exception as db_error:
                print(f"⚠️ Failed to update task status: {db_error}")
        
        if result and result.get('success'):
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
        if task_id:
            try:
                update_task_status(task_id, "failed", error_message=str(e))
            except:
                pass
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("🎯 Google AI Mode Extractor - ENHANCED WITH AI MODE BUTTON FALLBACK")
        print("\n🆕 DUAL NAVIGATION STRATEGY:")
        print(" PRIMARY METHOD: Show More → Dive Deeper in AI Mode")
        print(" FALLBACK METHOD: Direct AI Mode button click")
        print("\n🧠 Key Enhancements:")
        print(" 1. ✅ Intelligent answer extraction (removes UI/navigation noise)")
        print(" 2. ✅ Smart pattern recognition for actual AI content")
        print(" 3. ✅ Improved link text handling (replaces empty text with domain)")
        print(" 4. ✅ File-based HTML processing (like brightdata.py)")
        print(" 5. ✅ Enhanced markdown visibility and formatting")
        print(" 6. ✅ Better source link organization")
        print(" 🆕 7. DUAL NAVIGATION STRATEGY for maximum success rate")
        print("\n🔧 Technical Fixes:")
        print(" 8. ✅ File-based HTML processing ensures accuracy")
        print(" 9. ✅ HTML saved to temp file before Crawl4AI processing")
        print(" 10. ✅ Uses file:// URL instead of raw:// content")
        print(" 11. ✅ Waits for AI Mode page navigation (URL/content change)")
        print(" 12. ✅ Waits for AI processing to complete before extraction")
        print(" 13. ✅ Verifies extraction is from AI Mode page (not search results)")
        print(" 14. ✅ Debug screenshots for each navigation step")
        print("\n🚀 Enhanced Flow:")
        print(" 1. 🌐 Navigate to Google search")
        print(" 2. 📝 Enter query and search")
        print(" 3. 🎯 PRIMARY: Try Show More → Dive Deeper in AI Mode")
        print(" 4. 🔄 FALLBACK: If primary fails, click AI Mode button directly")
        print(" 5. ⏳ Wait for AI Mode page to load")
        print(" 6. ⏳ Wait for AI processing to complete")
        print(" 7. 💾 Save HTML to temporary file")
        print(" 8. 📄 Process file with Crawl4AI using file:// URL")
        print(" 9. 🧠 Apply intelligent filtering to extract real AI content")
        print(" 10. 🔗 Improve link text handling for better visibility")
        print(" 11. 📝 Format as ChatGPT-style markdown")
        print("\n✨ Features:")
        print(" • Anti-CAPTCHA with human-like behavior")
        print(" • Dual navigation strategy (Primary + Fallback)")
        print(" • Intelligent content extraction (removes unwanted UI elements)")
        print(" • External source links with improved text handling")
        print(" • Professional markdown formatting")
        print(" • Debugging screenshots")
        print(" • File-based Crawl4AI processing (ensures accuracy)")
        print(" • Smart pattern recognition for AI responses")
        print(" • Navigation method tracking and reporting")
        print("\nUsage:")
        print(" python script.py 'What is quantum computing?'")
        print()
    
    asyncio.run(main())
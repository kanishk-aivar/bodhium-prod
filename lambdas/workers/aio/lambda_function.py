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
from typing import List

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# === IMPROVED: INTELLIGENT ANSWER BLOCK START EXTRACTION ===

def clean_ai_overview_content(content):
    """Clean AI Overview content to remove problematic image and link patterns"""
    if not content:
        return content
    
    lines = content.split('\n')
    cleaned_lines = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Skip image syntax lines completely
        if line.strip().startswith('![](') and line.strip().endswith(')'):
            i += 1
            continue
        
        # Skip empty image syntax lines
        if line.strip() == '![]()':
            i += 1
            continue
        
        # Handle bullet point links with empty text
        if line.strip().startswith('* [') and '](' in line and line.strip().endswith(')'):
            # Extract URL from the link
            url_match = re.search(r'\]\((.*?)\)', line)
            if url_match:
                url = url_match.group(1)
                # Look for the next line which might contain the title
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    # If next line looks like a title (not empty, not image, not link)
                    if (next_line and 
                        not next_line.startswith('![](') and 
                        not next_line.startswith('* [') and 
                        not next_line.startswith('[') and
                        len(next_line) > 5):
                        # Create proper link with title
                        proper_link = f"* [{next_line}]({url})"
                        cleaned_lines.append(proper_link)
                        i += 2  # Skip both link and title lines
                        continue
                    else:
                        # Use domain name as link text
                        try:
                            domain = urllib.parse.urlparse(url).netloc
                            link_text = domain if domain else "Link"
                        except:
                            link_text = "Link"
                        proper_link = f"* [{link_text}]({url})"
                        cleaned_lines.append(proper_link)
                        i += 1
                        continue
        
        # Keep all other lines as they are
        cleaned_lines.append(line)
        i += 1
    
    return '\n'.join(cleaned_lines)

def extract_ai_overview_content(markdown_text):
    """Extract the AI Overview content by identifying patterns - generalized for any query"""
    
    # First, check if there's an error message about AI Overview not being available
    error_pattern = r'AI Overview is not available for this search|Can\'t generate an AI overview right now'
    error_match = re.search(error_pattern, markdown_text)
    
    # If there's an error message but actual content follows, remove the error message
    if error_match:
        # Check if there's actual content after the error message
        if "**AI Overview**" in markdown_text[error_match.end():]:
            markdown_text = markdown_text[error_match.end():]
    
    # PRIMARY TRUNCATION: "AI responses may include mistakes. Learn more" should be the FIRST truncation point
    primary_truncation_pattern = r'AI responses may include mistakes\. Learn more'
    primary_match = re.search(primary_truncation_pattern, markdown_text, re.MULTILINE | re.DOTALL)
    if primary_match:
        markdown_text = markdown_text[:primary_match.start()].strip()
    
    # AGGRESSIVE TRUNCATION: Look for other specific patterns that indicate the end of AI Overview content
    aggressive_truncation_patterns = [
        r'Thank you Your feedback helps Google improve\. See our Privacy Policy\.',
        r'Report a problemClose Show more Explain this',
        r'Access this menu with Ctrl\+Shift\+X',
        r'Restricted Mode.*?Autocomplete',
        r'People also ask Which conditioner is no 1\?',
        r'Dive deeper in AI Mode',
        r'Positive feedback Negative feedback Thank you',
        r'Your feedback helps Google improve\. See our Privacy Policy\.'
    ]
    
    # Try aggressive truncation for remaining patterns
    for pattern in aggressive_truncation_patterns:
        match = re.search(pattern, markdown_text, re.MULTILINE | re.DOTALL)
        if match:
            markdown_text = markdown_text[:match.start()].strip()
            break
    
    # Look for AI Overview section with various patterns
    ai_overview_patterns = [
        r'(?:# AI [Oo]verview|AI [Oo]verview)',
        r'\*\*AI Overview\*\*',
        r'## AI Overview'
    ]
    
    start_idx = -1
    for pattern in ai_overview_patterns:
        match = re.search(pattern, markdown_text)
        if match:
            start_idx = match.start()
            break
    
    if start_idx >= 0:
        # Look for end markers after AI Overview
        end_markers = [
            r'AI responses may include mistakes\. Learn more',  # PRIMARY truncation point - must be first
            r'AI responses may include mistakes',  # Fallback truncation point
            r'Dive deeper in AI Mode',
            r'Positive feedback',
            r'Negative feedback',
            r'From sources across the web',
            r'Related searches',
            r'People also ask',
            r'---\n\*This content was extracted',
            r'# Search Results',
            r'# Filters and topics',
            r'How it Works',
            r'Show more',
            r'^\s*\$\n#{1,3}\s',  # Any header after empty line
            r'Types of Machine Learning',  # Section break for machine learning content
            r'Thank you Your feedback helps Google improve',  # Additional truncation point
            r'Report a problemClose Show more',  # Additional truncation point
            r'Access this menu with Ctrl\+Shift\+X',  # Additional truncation point
            r'Restricted Mode.*?Autocomplete',  # Footer content
            r'People also ask Which conditioner is no 1\?'  # Specific content break
        ]
        
        end_idx = len(markdown_text)
        for marker in end_markers:
            match = re.search(marker, markdown_text[start_idx:], re.MULTILINE | re.DOTALL)
            if match:
                current_end = start_idx + match.start()
                if current_end < end_idx:
                    end_idx = current_end
        
        # Extract the AI Overview content
        ai_overview_content = markdown_text[start_idx:end_idx].strip()
        
        # IMPORTANT: Content is truncated at "AI responses may include mistakes" 
        # to exclude non-AI overview content like "People also ask" sections
        
        # If content is too short, it might not be the actual overview
        if len(ai_overview_content) < 100:
            # Try to find content after the header
            lines = markdown_text[start_idx:].split('\n')
            content_lines = []
            for i, line in enumerate(lines):
                if i > 0 and len(line.strip()) > 0:  # Skip the header line
                    content_lines.append(line)
                if i > 50:  # Look at first 50 lines max
                    break
            
            if content_lines:
                ai_overview_content = "\n".join([lines[0]] + content_lines)
        
        # Clean the content to remove problematic image and link patterns
        ai_overview_content = clean_ai_overview_content(ai_overview_content)
        
        # DYNAMIC SENTENCE REMOVAL: Remove specific unwanted sentences
        unwanted_sentences = [
            "AI overview right now. Try again later.",
            "AI overview right now. Try again later",
            "AI overview right now. Try again later",
            "AI overview right now. Try again later"
        ]
        
        for sentence in unwanted_sentences:
            ai_overview_content = ai_overview_content.replace(sentence, "")
        
        # FINAL CLEANUP: Remove any remaining problematic content patterns
        final_cleanup_patterns = [
            r'Thank you Your feedback helps Google improve.*$',
            r'Report a problem.*$',
            r'Restricted Mode.*$',
            r'People also ask.*$',
            r'Dive deeper in AI Mode.*$',
            r'Positive feedback.*$',
            r'Negative feedback.*$'
        ]
        
        for pattern in final_cleanup_patterns:
            ai_overview_content = re.sub(pattern, '', ai_overview_content, flags=re.MULTILINE | re.DOTALL)
        
        # Remove any trailing whitespace and empty lines
        ai_overview_content = re.sub(r'\n\s*\n\s*\n', '\n\n', ai_overview_content)
        ai_overview_content = ai_overview_content.strip()
        
        return ai_overview_content
    
    # Fallback: look for content that appears to be an overview
    # This is a generalized approach that looks for paragraphs that might be an overview
    lines = markdown_text.split('\n')
    
    # Look for potential overview sections by identifying patterns in the text
    for i, line in enumerate(lines):
            # Check for common content patterns that indicate an overview
            if any(pattern in line.lower() for pattern in [
                "machine learning", "artificial intelligence", "several excellent cars", 
                "top skincare brands", "enables computers", "type of artificial"
            ]):
                # Found potential overview content
                start_idx = i
                # Collect lines until we hit an end marker or section break
                content_lines = [lines[start_idx]]
                for j in range(start_idx + 1, min(start_idx + 50, len(lines))):
                    # PRIMARY truncation point - stop at "AI responses may include mistakes. Learn more"
                    if "AI responses may include mistakes. Learn more" in lines[j]:
                        break
                    # Fallback truncation point - stop at "AI responses may include mistakes"
                    if "AI responses may include mistakes" in lines[j]:
                        break
                    if any(marker in lines[j] for marker in ["Dive deeper", "Feedback", "People also", "How it Works", "Thank you Your feedback", "Report a problem", "Restricted Mode"]):
                        break
                    if re.match(r'^#{1,3}\s', lines[j]):  # New header indicates section break
                        break
                    if lines[j].strip():
                        content_lines.append(lines[j])
            
            if len(content_lines) > 2 and sum(len(line) for line in content_lines) > 100:
                content = "# AI Overview\n\n" + "\n".join(content_lines)
                # Clean the content
                content = clean_ai_overview_content(content)
                
                # DYNAMIC SENTENCE REMOVAL: Remove specific unwanted sentences (fallback)
                unwanted_sentences = [
                    "AI overview right now. Try again later.",
                    "AI overview right now. Try again later",
                    "AI overview right now. Try again later",
                    "AI overview right now. Try again later"
                ]
                
                for sentence in unwanted_sentences:
                    content = content.replace(sentence, "")
                
                # Apply final cleanup to fallback content too
                final_cleanup_patterns = [
                    r'Thank you Your feedback helps Google improve.*$',
                    r'Report a problem.*$',
                    r'Restricted Mode.*$',
                    r'People also ask.*$',
                    r'Dive deeper in AI Mode.*$',
                    r'Positive feedback.*$',
                    r'Negative feedback.*$'
                ]
                
                for pattern in final_cleanup_patterns:
                    content = re.sub(pattern, '', content, flags=re.MULTILINE | re.DOTALL)
                
                content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
                content = content.strip()
                
                return content
    
    # If no clear AI Overview found, return a placeholder
    return "# AI Overview\n\nNo AI Overview content found."

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
    
    # FIXED: Correct import path for content filter
    try:
        from crawl4ai.content_filter_strategy import PruningContentFilter
    except ImportError:
        # Try alternative import path if the first one fails
        from crawl4ai.content_filtering_strategy import PruningContentFilter
    
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

class GoogleAIOverviewExtractor:
    def __init__(self, headless=False, verbose=True, use_brightdata=True):
        self.headless = headless
        self.verbose = verbose
        self.use_brightdata = use_brightdata
        self.downloads_path = tempfile.mkdtemp(prefix='google_ai_overview_')
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

    async def _find_and_click_interactive_elements(self, page, target_texts=None, max_attempts=3):
        """
        Generalized method to find and click interactive elements like "Show more" or "Show all"
        
        Args:
            page: Playwright page object
            target_texts: List of text strings to look for (default: ["show more", "show all"])
            max_attempts: Maximum number of attempts to find and click elements
        """
        if target_texts is None:
            target_texts = ["show more", "show all"]
            
        self.log(f"🔍 Looking for interactive elements with text: {target_texts}...")
        
        # Add human-like behavior before clicking
        await self._human_like_navigation(page)
        await asyncio.sleep(random.uniform(1, 2))
        
        # Take screenshot before attempting to find elements
        before_screenshot = os.path.join(self.downloads_path, f"before_interactive_elements_{int(time.time())}.png")
        await page.screenshot(path=before_screenshot, full_page=True)
        self.log(f"📸 Screenshot before looking for elements: {before_screenshot}")
        
        # Enhanced selectors for interactive elements
        interactive_selectors = [
            'button',
            'div[role="button"]',
            'a[role="button"]',
            'span[role="button"]',
            '[tabindex="0"]',
            '.niO4u',
            '.kHtcsd',
            '.clOx1e',
            '.sjVJQd',
            '.VDgVie',
            '.SlP8xc',
            '[data-ved]',
            '[data-hveid]',
            # Dictionary-specific selectors
            'div.xpdopen span.qLLird',
            'div.xpdclose span.qLLird',
            # AI Overview specific selectors
            'div.UDZeY button',
            'div.V3FYCf button',
            'div.iDjcJe button',
            # Show more/all specific selectors
            'button:has-text("Show more")',
            'button:has-text("Show all")',
            'div:has-text("Show more")',
            'div:has-text("Show all")',
            'span:has-text("Show more")',
            'span:has-text("Show all")'
        ]
        
        for attempt in range(max_attempts):
            self.log(f"Attempt {attempt + 1} to find interactive elements...")
            
            # Try direct selectors first
            for selector in interactive_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        try:
                            element_text = await element.text_content()
                            if element_text and any(target_text.lower() in element_text.lower() for target_text in target_texts):
                                self.log(f"✅ Found interactive element with selector: {selector}, text: '{element_text.strip()}'")
                                
                                # Check if element is visible
                                is_visible = await element.is_visible()
                                if not is_visible:
                                    self.log(f"Element not visible, skipping: {element_text.strip()}")
                                    continue
                                    
                                # Human-like interaction
                                await element.scroll_into_view_if_needed()
                                await asyncio.sleep(random.uniform(1, 2))
                                
                                # Take screenshot with element highlighted
                                try:
                                    await page.evaluate("""(element) => {
                                        const originalBackground = element.style.backgroundColor;
                                        const originalBorder = element.style.border;
                                        element.style.backgroundColor = 'rgba(255, 0, 0, 0.3)';
                                        element.style.border = '2px solid red';
                                        return {originalBackground, originalBorder};
                                    }""", element)
                                    
                                    highlight_screenshot = os.path.join(self.downloads_path, f"element_highlight_{int(time.time())}.png")
                                    await page.screenshot(path=highlight_screenshot, full_page=True)
                                    self.log(f"📸 Element highlighted screenshot: {highlight_screenshot}")
                                    
                                    # Restore original styles
                                    await page.evaluate("""(element) => {
                                        element.style.backgroundColor = '';
                                        element.style.border = '';
                                    }""", element)
                                except Exception as e:
                                    self.log(f"Error highlighting element: {e}")
                                
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
                                    self.log(f"✅ '{element_text.strip()}' clicked successfully!")
                                    await asyncio.sleep(random.uniform(2, 4))
                                    
                                    # Take screenshot after clicking
                                    after_screenshot = os.path.join(self.downloads_path, f"after_click_{int(time.time())}.png")
                                    await page.screenshot(path=after_screenshot, full_page=True)
                                    self.log(f"📸 Screenshot after clicking: {after_screenshot}")
                                    
                                    return True
                        except Exception as e:
                            self.log(f"Error processing element: {e}")
                            continue
                except Exception as e:
                    self.log(f"Selector {selector} failed: {e}")
                    continue
            
            # Try JavaScript approach to find and click elements
            self.log("🔍 Trying JavaScript approach to find interactive elements...")
            try:
                # Find elements by text content using JavaScript
                button_found = await page.evaluate("""
                    (targetTexts) => {
                        const elements = Array.from(document.querySelectorAll('button, [role="button"], a, span, div'));
                        for (const element of elements) {
                            const text = element.textContent.toLowerCase();
                            if (targetTexts.some(target => text.includes(target))) {
                                // Check if element is visible
                                const rect = element.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0) {
                                    console.log("Found element with text:", element.textContent);
                                    element.scrollIntoView({behavior: 'smooth', block: 'center'});
                                    setTimeout(() => {
                                        element.click();
                                    }, 500);
                                    return true;
                                }
                            }
                        }
                        return false;
                    }
                """, target_texts)
                
                if button_found:
                    self.log(f"✅ Found and clicked interactive element via JavaScript!")
                    await asyncio.sleep(random.uniform(2, 4))
                    
                    # Take screenshot after JavaScript click
                    js_after_screenshot = os.path.join(self.downloads_path, f"after_js_click_{int(time.time())}.png")
                    await page.screenshot(path=js_after_screenshot, full_page=True)
                    self.log(f"📸 Screenshot after JavaScript click: {js_after_screenshot}")
                    
                    return True
            except Exception as e:
                self.log(f"JavaScript approach failed: {e}")
            
            # If we haven't found anything yet, scroll down a bit and try again
            if attempt < max_attempts - 1:
                self.log("Scrolling down to look for more elements...")
                await page.evaluate("window.scrollBy(0, 300)")
                await asyncio.sleep(random.uniform(1, 2))
        
        self.log(f"⚠️ Could not find interactive elements with text: {target_texts}, continuing with available content")
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

    async def extract_ai_overview_with_playwright(self, query, max_wait_time=60):
        """
        Extract Google AI Overview using Playwright with Bright Data
        """
        if not PLAYWRIGHT_AVAILABLE:
            self.log("❌ Playwright not available")
            return await self.extract_ai_overview_fallback(query, max_wait_time)
        
        if not SBR_WS_CDP and self.use_brightdata:
            self.log("❌ Bright Data credentials not available")
            return await self.extract_ai_overview_fallback(query, max_wait_time)
        
        results = {
            'query': query,
            'success': False,
            'ai_overview': '',
            'files': {},
            'metadata': {},
            'error': None,
            'approach_used': 'playwright_brightdata',
            'debug_screenshots': [],
            'captcha_detected': False,
            'retry_count': 0
        }
        
        max_retries = 2
        retry_count = 0
        
        while retry_count <= max_retries:
            browser = None
            context = None
            page = None
            
            try:
                async with async_playwright() as playwright:
                    self.log(f"🚀 Starting AI Overview extraction (Attempt {retry_count + 1}) for: '{query}'")
                    
                    # STEP 1: Playwright launches
                    self.log('🚀 Step 1: Launching Playwright with Bright Data connection...')
                    
                    if self.use_brightdata and SBR_WS_CDP:
                        session_cdp = f"{SBR_WS_CDP}&session_id={self.session_id}_{retry_count}"
                        browser = await playwright.chromium.connect_over_cdp(session_cdp)
                    else:
                        browser = await playwright.chromium.launch(headless=self.headless)
                    
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
                    
                    # STEP 5: Check for AI Overview section
                    self.log("🔍 Step 5: Looking for AI Overview section...")
                    
                    # Check if AI Overview is present - generalized approach
                    ai_overview_present = await page.evaluate("""
                        () => {
                            const pageText = document.body.innerText.toLowerCase();
                            
                            // Check for text indicators
                            const textIndicators = ['ai overview', 'ai mode'];
                            if (textIndicators.some(indicator => pageText.includes(indicator))) {
                                return true;
                            }
                            
                            // Check for structural indicators
                            const structuralIndicators = [
                                '[aria-label="AI Overview"]',
                                'div.UDZeY',
                                'div[data-attrid*="ai_overview"]',
                                'div[data-hveid*="AI"]',
                                'svg + div.iDjcJe',
                                'div.V3FYCf',
                                'div.xpdopen',
                                'div.xpdclose'
                            ];
                            
                            for (const selector of structuralIndicators) {
                                if (document.querySelector(selector)) {
                                    return true;
                                }
                            }
                            
                            return false;
                        }
                    """)
                    
                    if ai_overview_present:
                        self.log("✅ AI Overview section found!")
                        
                        # Try to click "Show more" if available to expand AI Overview
                        await self._find_and_click_interactive_elements(page, ["show more"])
                        
                        # Wait for content to stabilize after possible expansion
                        await asyncio.sleep(random.uniform(3, 5))
                        
                        # Take screenshot after possible expansion
                        expanded_screenshot = os.path.join(self.downloads_path, f"04_expanded_ai_overview_{retry_count + 1}.png")
                        await page.screenshot(path=expanded_screenshot, full_page=True)
                        results['debug_screenshots'].append(expanded_screenshot)
                        
                        # Try to click "Show all" for citations
                        await self._find_and_click_interactive_elements(page, ["show all"])
                        
                        # Wait for citations to expand
                        await asyncio.sleep(random.uniform(3, 5))
                        
                        # Take screenshot after citations expansion
                        citations_screenshot = os.path.join(self.downloads_path, f"05_expanded_citations_{retry_count + 1}.png")
                        await page.screenshot(path=citations_screenshot, full_page=True)
                        results['debug_screenshots'].append(citations_screenshot)
                        
                        # Try to click any other "Show more" buttons that might have appeared
                        await self._find_and_click_interactive_elements(page, ["show more"])
                        await asyncio.sleep(random.uniform(2, 4))
                        
                        # Final screenshot after all expansions
                        final_screenshot = os.path.join(self.downloads_path, f"06_final_expanded_{retry_count + 1}.png")
                        await page.screenshot(path=final_screenshot, full_page=True)
                        results['debug_screenshots'].append(final_screenshot)
                        
                    else:
                        self.log("⚠️ No AI Overview section found, but continuing with available content")
                    
                    # STEP 6: Extract HTML content
                    self.log('📄 Step 6: Extracting HTML content...')
                    final_html = await page.content()
                    
                    if not final_html or len(final_html) < 1000:
                        raise Exception("Insufficient HTML content extracted")
                    
                    self.log(f"✅ Extracted HTML content ({len(final_html)} characters)")
                    
                    # STEP 7: Process with Crawl4AI if available
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    page_url = page.url
                    
                    # IMPORTANT: Get all necessary data before closing the browser
                    if CRAWL4AI_AVAILABLE:
                        self.log('🔧 Step 7: Processing HTML with Crawl4AI...')
                        markdown_result = await self._process_html_with_crawl4ai(
                            final_html, query, page_url, timestamp
                        )
                        
                        if not markdown_result:
                            self.log('⚠️ Crawl4AI processing failed, using basic HTML extraction')
                            markdown_result = {
                                'raw_markdown': f"# Search Results for {query}\n\n{await page.evaluate('() => document.body.innerText')}",
                                'links_found': []
                            }
                    else:
                        self.log('⚠️ Crawl4AI not available, using basic HTML extraction')
                        markdown_result = {
                            'raw_markdown': f"# Search Results for {query}\n\n{await page.evaluate('() => document.body.innerText')}",
                            'links_found': []
                        }
                    
                    # IMPORTANT: Close browser AFTER extracting all necessary data
                    await browser.close()
                    browser = None
                    
                    # STEP 8: Save results
                    success = await self._save_results(
                        query, final_html, markdown_result, results, timestamp, page_url
                    )
                    
                    if success:
                        results['success'] = True
                        results['retry_count'] = retry_count
                        self.log(f"🎉 Successfully completed AI Overview extraction!")
                        return results
                    else:
                        raise Exception("Failed to save results")
                    
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
                # Ensure browser is closed properly
                if browser:
                    try:
                        await browser.close()
                        self.log("🔌 Browser connection closed")
                    except Exception as close_error:
                        self.log(f"⚠️ Error closing browser: {close_error}")
        
        # If we exhausted all retries
        results['error'] = f"Failed after {max_retries + 1} attempts due to CAPTCHA"
        results['retry_count'] = retry_count
        return results

    async def _process_html_with_crawl4ai(self, html_content, query, page_url, timestamp):
        """Process HTML with Crawl4AI using file-based approach"""
        if not CRAWL4AI_AVAILABLE:
            return None
        
        try:
            self.log("🔧 Using file-based Crawl4AI processing...")
            
            # Create complete HTML document with proper structure
            complete_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Google AI Overview Response - {query}</title>
    <base href="https://www.google.com/">
</head>
<body>
    <div class="ai-overview-content">
        {html_content}
    </div>
</body>
</html>"""
            
            # Save HTML to file and use file:// URL
            temp_html_path = os.path.join(ephemeral_base, f"temp_ai_overview_response_{timestamp}.html")
            with open(temp_html_path, 'w', encoding='utf-8') as f:
                f.write(complete_html)
            
            self.log(f"💾 Saved AI Overview HTML to temp file: {temp_html_path}")
            self.log(f"📊 HTML file size: {os.path.getsize(temp_html_path)} bytes")
            
            # Use minimal filtering to preserve maximum content
            try:
                # Try with the correct import path first
                content_filter = PruningContentFilter(
                    threshold=0.15,  # Very light filtering to preserve content
                    threshold_type="fixed",
                    min_word_threshold=5  # Lower threshold for more content
                )
            except Exception as filter_error:
                self.log(f"⚠️ Error creating content filter: {filter_error}")
                # Use default filter if custom one fails
                content_filter = None
            
            # Configure markdown generator with optimal settings
            md_generator = DefaultMarkdownGenerator(
                content_filter=content_filter,
                options={
                    "ignore_links": False,  # Preserve all clickable links
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
            
            self.log("🌐 Processing AI Overview HTML file with Crawl4AI...")
            
            # Use file:// URL
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
                
                # Extract sidebar links specifically (not all links)
                sidebar_links = []
                if result.links:
                    # Look for links that appear to be from the sidebar/citations
                    all_links = result.links.get("internal", []) + result.links.get("external", [])
                    
                    for link in all_links:
                        if isinstance(link, dict):
                            href = link.get('href', '')
                            text = link.get('text', '')
                            
                            # Only include links that look like external citations
                            if (href and text and 
                                len(text.strip()) > 5 and  # Longer text suggests actual content
                                not any(unwanted in text.lower() for unwanted in ['preview', 'aim_query', 'show_more', 'dive_deeper', 'ai_mode', 'parsed.md', '.json', 'crawl4ai']) and
                                not text.lower() in ['ai mode', 'shopping', 'images', 'short videos', 'forums', 'videos', 'news', 'web', 'books', 'past hour', 'past 24 hours', 'past week', 'past month', 'past year', 'verbatim', 'reconstructing', 'usa', 'moisturizing', 'reviews', 'curly hair']):
                                
                                # Extract domain name for better display
                                try:
                                    domain = urllib.parse.urlparse(href).netloc
                                    if domain and domain != 'google.com':
                                        sidebar_links.append({'text': text.strip(), 'url': href, 'domain': domain})
                                except:
                                    sidebar_links.append({'text': text.strip(), 'url': href})

                links_list = sidebar_links
                
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
                temp_html_path = os.path.join(ephemeral_base, f"temp_ai_overview_response_{timestamp}.html")
                if os.path.exists(temp_html_path):
                    os.remove(temp_html_path)
            except:
                pass
            return None

    async def _save_results(self, query, final_html, markdown_data, results, timestamp, page_url):
        """Save results with AI Overview extraction using enhanced approach"""
        
        try:
            self.log("💾 Saving AI Overview results with enhanced approach...")
            
            # Extract raw markdown from the result
            raw_markdown = markdown_data.get('raw_markdown', '')
            links_found = markdown_data.get('links_found', [])
            
            # Clean the raw markdown FIRST to remove problematic patterns
            cleaned_raw_markdown = clean_ai_overview_content(raw_markdown)
            
            # Extract AI Overview content from the CLEANED markdown
            ai_overview_content = extract_ai_overview_content(cleaned_raw_markdown)
            
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
            
            # --- FILTER OUT UNWANTED SOURCES ---
            filtered_sources = []
            for title, url in sidebar_sources:
                # Skip Google search filter options
                if title.lower() in [
                    'ai mode', 'shopping', 'images', 'short videos', 'forums', 'videos', 
                    'news', 'web', 'books', 'past hour', 'past 24 hours', 'past week', 
                    'past month', 'past year', 'verbatim', 'reconstructing', 'usa', 
                    'moisturizing', 'reviews', 'curly hair'
                ]:
                    continue
                
                # Skip unwanted entries by title patterns
                if any(unwanted in title.lower() for unwanted in [
                    'preview', 'aim_query', 'show_more', 'dive_deeper', 'ai_mode',
                    'parsed.md', '.json', 'crawl4ai'
                ]):
                    continue
                    
                # Skip empty or invalid titles
                if not title or title.strip() in ['•', '.', '-', '–', '—']:
                    continue
                    
                filtered_sources.append((title, url))
            
            # Remove duplicate links
            seen = set()
            unique_sources = []
            for title, url in filtered_sources:
                if url not in seen:
                    seen.add(url)
                    unique_sources.append((title, url))
            
            sources_markdown = '\n'.join([f"- [{title}]({url})" for title, url in unique_sources[:20]])
            
            # Create enhanced markdown with proper formatting
            enhanced_markdown = f"""# Google AI Overview Response

**Query:** {query}  
**Timestamp:** {timestamp}  
**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

---

## AI Overview Content

{ai_overview_content}

---

## Sources

{sources_markdown}

---

## Metadata

- **Extraction Method:** File-based Crawl4AI Processing
- **Response Length:** {len(ai_overview_content)} characters
- **Links Found:** {len(unique_sources)}
- **Status:** Success
- **Source:** {page_url}
- **File-based Processing:** ✅ Applied

---

*Generated by Google AI Overview Automation System (Enhanced Version)*

"""
            
            # Generate safe filenames
            safe_query = "".join(c for c in query if c.isalnum() or c in (' ', '-', '_')).rstrip()[:50]
            file_prefix = f"{timestamp}_ai_overview_{safe_query.replace(' ', '_')}"
            
            # Save files
            markdown_path = os.path.join(self.downloads_path, f"{file_prefix}_parsed.md")
            html_path = os.path.join(self.downloads_path, f"{file_prefix}_raw.html")
            raw_md_path = os.path.join(self.downloads_path, f"{file_prefix}_raw.md")
            
            # Save enhanced markdown content
            with open(markdown_path, 'w', encoding='utf-8') as f:
                f.write(enhanced_markdown)
            
            # Save raw HTML
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(final_html)
            
            # Save cleaned raw markdown (without problematic patterns)
            with open(raw_md_path, 'w', encoding='utf-8') as f:
                f.write(cleaned_raw_markdown)
            
            # Save links as JSON for reference
            if unique_sources:
                links_path = os.path.join(self.downloads_path, f"{file_prefix}_links.json")
                with open(links_path, "w", encoding="utf-8") as f:
                    json.dump([{"title": title, "url": url} for title, url in unique_sources], f, indent=2, ensure_ascii=False)
                results['files']['links'] = links_path
            
            # Prepare citations.json data for S3 upload only
            if unique_sources:
                citations_data = {
                    "urls": [url for title, url in unique_sources],
                    "mode": "ai_overview",
                    "query": query
                }
                results['citations_data'] = citations_data
            
            results['files']['markdown'] = markdown_path
            results['files']['html'] = html_path
            results['files']['raw_markdown'] = raw_md_path
            results['ai_overview'] = ai_overview_content
            
            results['metadata'] = {
                'query': query,
                'timestamp': timestamp,
                'html_length': len(final_html),
                'structured_markdown_length': len(enhanced_markdown),
                'raw_markdown_length': len(raw_markdown),
                'ai_overview_length': len(ai_overview_content),
                'external_sources_found': len(unique_sources),
                'url': page_url,
                'approach': 'file_based_crawl4ai_processing',
                'crawl4ai_used': CRAWL4AI_AVAILABLE,
                'playwright_used': True,
                'brightdata_used': self.use_brightdata,
                'file_based_processing': True,
                'enhanced_link_handling': True,
                'session_id': self.session_id
            }
            
            self.log(f"✅ Enhanced AI Overview markdown saved: {markdown_path}")
            self.log(f"✅ HTML saved: {html_path}")
            self.log(f"✅ Raw markdown saved: {raw_md_path}")
            if unique_sources:
                self.log(f"✅ Links JSON saved: {results['files']['links']}")
            self.log(f"📊 AI Overview content: {len(ai_overview_content)} chars")
            self.log(f"🔗 Links extracted: {len(unique_sources)}")
            self.log(f"🧠 Enhanced link handling applied: ✅")
            
            return True
            
        except Exception as e:
            self.log(f"❌ Error saving results: {str(e)}")
            return False

    async def extract_ai_overview_fallback(self, query, max_wait_time=60):
        """Fallback method if dependencies are not available"""
        return {
            'query': query,
            'success': False,
            'ai_overview': '',
            'files': {},
            'metadata': {},
            'error': "Required dependencies not available (Playwright or Crawl4AI)",
            'approach_used': 'fallback'
        }

    async def extract_ai_overview(self, query, max_wait_time=60):
        """Main extraction method for AI Overview"""
        
        if PLAYWRIGHT_AVAILABLE:
            self.log("🎯 Using Playwright for AI Overview extraction")
            return await self.extract_ai_overview_with_playwright(query, max_wait_time)
        else:
            self.log("🔄 Using fallback approach")
            return await self.extract_ai_overview_fallback(query, max_wait_time)

# Retry logic helper function
def should_retry_error(error_message):
    """Determine if an error should trigger a retry"""
    if not error_message:
        return False
    
    error_lower = str(error_message).lower()
    
    # Retryable errors
    retryable_patterns = [
        'timeout',
        'connection',
        'network',
        'captcha',
        'rate limit',
        'temporary',
        'unavailable',
        'service error',
        'internal server error',
        'bad gateway',
        'gateway timeout',
        'brightdata',
        'proxy',
        'browser',
        'playwright'
    ]
    
    # Non-retryable errors (should fail immediately)
    non_retryable_patterns = [
        'authentication',
        'unauthorized',
        'forbidden',
        'not found',
        'invalid query',
        'malformed',
        'syntax error',
        'permission denied'
    ]
    
    # Check for non-retryable patterns first
    for pattern in non_retryable_patterns:
        if pattern in error_lower:
            return False
    
    # Check for retryable patterns
    for pattern in retryable_patterns:
        if pattern in error_lower:
            return True
    
    # Default to retry for unknown errors
    return True

# FIXED: Corrected database functions to handle UUID properly
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

def create_llm_task(job_id, query_id, llm_model_name="google-ai-overview", product_id=None, product_name=None, session_id=None, task_id=None):
    logger.info(f"Creating LLM task for job_id: {job_id}, query_id: {query_id}, session_id: {session_id}")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
                datetime.now(timezone.utc),
                product_id,
                product_name,
                session_id
            )
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Inserted LLM task to DB -- task_id: {task_id}, session_id: {session_id}")
        return task_id
        
    except Exception as e:
        logger.error(f"Error creating LLM task: {e}")
        print(f"❌ Failed to create LLM task in RDS: {e}")
        return None

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
        logger.error(f"Error updating task status: {e}")
        print(f"❌ Failed to update task status in RDS: {e}")
        # Match crawlforai.py pattern - raise the exception
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
        logger.info(f"Logged orchestration event: {event_name} for job_id: {job_id}")
        print(f"✅ Logged orchestration event to DynamoDB")
    except Exception as e:
        logger.error(f"Error logging orchestration event: {e}")
        print(f"❌ Failed to log orchestration event: {e}")

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
        s3_key = f"{brand_name_safe}/{job_id}/{product_name_safe}/{query_text_safe}/aio/response.{file_extension}"
        
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
            'mode': 'aio',  # Use specific mode for AI Overview
            'query': user_query if user_query else 'na',
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

async def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(description='Google AI Overview Extractor')
    parser.add_argument('query', nargs='?', help='Search query for AI Overview')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode')
    parser.add_argument('--max-wait', type=int, default=60, help='Maximum wait time (seconds)')
    parser.add_argument('--quiet', action='store_true', help='Suppress verbose output')
    parser.add_argument('--no-brightdata', action='store_true', help='Disable Bright Data')

    args = parser.parse_args()

    if args.query:
        query = args.query
    else:
        query = input("Enter your query for Google AI Overview: ").strip()

    if not query:
        print("❌ No query provided. Exiting.")
        return

    extractor = GoogleAIOverviewExtractor(
        headless=args.headless,
        verbose=not args.quiet,
        use_brightdata=not args.no_brightdata
    )

    results = await extractor.extract_ai_overview(query, args.max_wait)

    print(f"\n{'='*80}")
    print(f"GOOGLE AI OVERVIEW EXTRACTOR RESULTS")
    print(f"{'='*80}")
    print(f"Query: {results['query']}")
    print(f"Success: {results['success']}")
    
    if results['success']:
        print(f"Approach Used: {results['approach_used']}")
        print(f"\n📊 Metadata:")
        for key, value in results['metadata'].items():
            print(f"  {key}: {value}")
        
        print(f"\n📁 Files saved:")
        for file_type, path in results['files'].items():
            print(f"  {file_type}: {path}")
            
        print(f"\n🎯 AI Overview Features:")
        print(f"  ✅ Extracts Google's AI Overview content")
        print(f"  ✅ Handles 'Show more' expansion")
        print(f"  ✅ Saves raw HTML and processed markdown")
        print(f"  ✅ Extracts external links and sources")
        print(f"  ✅ Anti-CAPTCHA measures with human behavior")
        print(f"  ✅ Debugging screenshots for each step")
        
        if results.get('debug_screenshots'):
            print(f"\n📸 Debug Screenshots:")
            for screenshot in results['debug_screenshots']:
                print(f"  - {screenshot}")
        
        if results.get('ai_overview'):
            print(f"\n📄 Preview of AI Overview content:")
            preview = results['ai_overview'][:500] + "..." if len(results['ai_overview']) > 500 else results['ai_overview']
            print(f"{preview}")
    else:
        print(f"❌ Error: {results['error']}")
        if results.get('debug_screenshots'):
            print(f"\n📸 Debug Screenshots available:")
            for screenshot in results['debug_screenshots']:
                print(f"  - {screenshot}")

    print(f"\n{'='*80}")

def lambda_handler(event, context):
    """AWS Lambda handler for AI Overview extraction"""
    task_id = None
    job_id = None
    new_bucket_path = None
    max_retries = 10
    retry_count = 0
    
    try:
        print("🎯 Lambda function started (Google AI Overview Extractor)")
        
        query = event.get('query', 'What is artificial intelligence?')
        job_id = event.get('job_id', str(uuid.uuid4()))
        query_id = event.get('query_id', 1)  # Default to integer like crawlforai.py
        product_id = event.get('product_id', str(uuid.uuid4()))  # NEW: Extract product_id
        session_id = event.get('session_id')  # NEW: Extract session_id
        provided_task_id = event.get('task_id')  # NEW: Extract provided task_id
        retry_count = event.get('retry_count', 0)  # NEW: Extract retry count
        
        print(f"📊 Job ID: {job_id}, Query ID: {query_id}, Product ID: {product_id}, Session ID: {session_id}")
        
        # Get product name from database
        product_name = get_product_name_from_db(product_id) if product_id else 'Unknown Product'
        
        # Log orchestration event first (doesn't depend on database)
        try:
            log_orchestration_event(job_id, "task_started", {
                "query": query,
                "task_id": None,  # Will be updated later if task creation succeeds
                "product_id": product_id,  # NEW: Include product_id in logging
                "session_id": session_id,  # NEW: Include session_id in logging
                "pipeline": "google_ai_overview_extraction",
                "timestamp": datetime.now().isoformat()
            })
        except Exception as log_error:
            print(f"⚠️ Failed to log orchestration event: {log_error}")
        
        # Database operations
        try:
            task_id = create_llm_task(job_id, query_id, "GOOGLE_AI_OVERVIEW", product_id, product_name, session_id, provided_task_id)
            if task_id:
                # Set status based on retry count
                if retry_count > 0:
                    status = f"retrying...({retry_count})"
                    print(f"🔄 Retry attempt {retry_count}/{max_retries}")
                else:
                    status = "running"
                
                update_task_status(task_id, status)
                # Update orchestration event with task_id
                log_orchestration_event(job_id, "task_created", {
                    "query": query,
                    "task_id": task_id,
                    "product_id": product_id,  # NEW: Include product_id in logging
                    "session_id": session_id,  # NEW: Include session_id in logging
                    "retry_count": retry_count,  # NEW: Include retry count in logging
                    "pipeline": "google_ai_overview_extraction",
                    "timestamp": datetime.now().isoformat()
                })
        except Exception as db_error:
            print(f"⚠️ Database operations failed: {db_error}")
            task_id = None
        
        extractor = GoogleAIOverviewExtractor(
            headless=True,
            verbose=True,
            use_brightdata=True
        )
        
        result = asyncio.run(extractor.extract_ai_overview(query))
        
        # NEW: Upload to new bucket structure (like crawlforai.py)
        if result and result.get('success'):
            try:
                # Create comprehensive result for new bucket
                aio_result = {
                    "job_id": job_id,
                    "product_id": product_id,
                    "query_id": query_id,
                    "query": query,
                    "timestamp": datetime.now().isoformat(),
                    "model": "GOOGLE_AI_OVERVIEW",
                    "content": result.get('ai_overview', ''),
                    "metadata": result.get('metadata', {}),
                    "status": "success"
                }
                
                new_bucket_path = upload_to_new_bucket_structure(
                    aio_result, job_id, product_id, query_id, 'application/json', 'json'
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
            S3_PATH = os.environ.get('S3_PATH', 'google-ai-overview')
            
            if S3_BUCKET:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                query_folder = re.sub(r'[^a-zA-Z0-9\s]', '', query).replace(' ', '_')
                folder_name = f"{query_folder}_{timestamp}"
                
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
                            elif file_type in ['links', 'citations']:
                                content_type = 'application/json'
                            elif file_type == 'screenshot':
                                content_type = 'image/png'
                            else:
                                content_type = 'text/plain'
                            
                            uploaded_key = upload_to_s3(content, S3_BUCKET, s3_key, content_type)
                            
                            if uploaded_key:
                                s3_keys[file_type] = f"s3://{S3_BUCKET}/{uploaded_key}"
                                
                        except Exception as upload_error:
                            print(f"❌ Error uploading {file_type}: {upload_error}")
                
                # Upload citations.json to S3
                if result.get('citations_data'):
                    try:
                        citations_content = json.dumps(result['citations_data'], indent=2, ensure_ascii=False).encode('utf-8')
                        citations_s3_key = f"{S3_PATH}/{folder_name}/citations.json"
                        uploaded_citations_key = upload_to_s3(citations_content, S3_BUCKET, citations_s3_key, 'application/json')
                        
                        if uploaded_citations_key:
                            s3_keys['citations'] = f"s3://{S3_BUCKET}/{uploaded_citations_key}"
                            print(f"✅ Citations uploaded to S3: s3://{S3_BUCKET}/{uploaded_citations_key}")
                    except Exception as citations_error:
                        print(f"❌ Error uploading citations: {citations_error}")
                
                if s3_keys:
                    result['s3_files'] = s3_keys
            
            print(f"🎯 Google AI Overview extraction complete!")
            print(f"📊 Key Enhancement: New bucket structure upload to bodhium-temp bucket")
            print(f"📁 New S3 path: {new_bucket_path}" if new_bucket_path else "")
            
        # Update task status
        if task_id:
            try:
                if result and result.get('success'):
                    s3_output_path = result.get('s3_files', {}).get('markdown', '')
                    # Use new bucket path instead of old S3 path (like crawlforai.py)
                    final_s3_path = new_bucket_path if new_bucket_path else s3_output_path
                    
                    update_task_status(
                        task_id,
                        "completed",
                        s3_output_path=final_s3_path,
                        completed_at=datetime.now(timezone.utc)
                    )
                else:
                    error_msg = result.get('error', 'Unknown error') if result else 'No result returned'
                    
                    # Check if we should retry
                    if retry_count < max_retries and should_retry_error(error_msg):
                        print(f"🔄 Task failed, will retry. Attempt {retry_count + 1}/{max_retries}")
                        update_task_status(task_id, f"retrying...({retry_count + 1})")
                        
                        # Trigger retry by invoking this lambda again with incremented retry count
                        retry_event = event.copy()
                        retry_event['retry_count'] = retry_count + 1
                        
                        try:
                            lambda_client = boto3.client('lambda')
                            lambda_client.invoke(
                                FunctionName=context.function_name,
                                InvocationType='Event',  # Asynchronous
                                Payload=json.dumps(retry_event)
                            )
                            print(f"✅ Retry invocation triggered for attempt {retry_count + 1}")
                            
                            return {
                                'statusCode': 202,
                                'body': json.dumps({
                                    'message': f'Task failed, retry {retry_count + 1}/{max_retries} triggered',
                                    'task_id': task_id,
                                    'retry_count': retry_count + 1
                                })
                            }
                        except Exception as retry_error:
                            print(f"❌ Failed to trigger retry: {retry_error}")
                            update_task_status(task_id, "failed", error_message=f"Retry failed: {retry_error}")
                    else:
                        # Max retries reached or non-retryable error
                        if retry_count >= max_retries:
                            error_msg = f"Max retries ({max_retries}) reached. Last error: {error_msg}"
                        update_task_status(task_id, "failed", error_message=error_msg)
            except Exception as db_error:
                print(f"⚠️ Failed to update task status: {db_error}")
        
        # Log completion event
        try:
            log_orchestration_event(job_id, "task_completed", {
                "query": query,
                "task_id": task_id,
                "product_id": product_id,  # NEW: Include product_id in logging
                "success": result.get('success', False) if result else False,
                "pipeline": "google_ai_overview_extraction",
                "timestamp": datetime.now().isoformat()
            })
        except Exception as log_error:
            print(f"⚠️ Failed to log completion event: {log_error}")
        
        # Trigger citation-scraper lambda with extracted citations
        if result and result.get('success') and result.get('metadata', {}).get('external_sources_found', 0) > 0:
            try:
                # Extract citations from the result metadata
                external_sources = result.get('metadata', {}).get('external_sources_found', 0)
                if external_sources > 0:
                    # For AI Overview, we need to extract URLs from the links file if available
                    citations = []
                    links_file = result.get('files', {}).get('links')
                    if links_file and os.path.exists(links_file):
                        try:
                            with open(links_file, 'r', encoding='utf-8') as f:
                                links_data = json.load(f)
                                for link in links_data:
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
                                            citations.append(url)
                                        
                        except Exception as link_error:
                            print(f"⚠️ Error reading links file: {link_error}")
                    
                    if citations:
                        # Remove duplicates while preserving order
                        unique_citations = []
                        seen_urls = set()
                        for url in citations:
                            if url not in seen_urls:
                                unique_citations.append(url)
                                seen_urls.add(url)
                        
                        citation_invocation_success = invoke_citation_scraper_lambda(
                            unique_citations, job_id, query_id, product_id, query, task_id
                        )
                        if citation_invocation_success:
                            logger.info(f"Successfully triggered citation-scraper lambda with {len(unique_citations)} citations")
                            print(f"🔗 Citation-scraper lambda triggered with {len(unique_citations)} URLs")
                            
                            # Log citation-scraper trigger event
                            try:
                                log_orchestration_event(job_id, "citation_scraper_triggered", {
                                    "task_id": task_id,
                                    "product_id": product_id,
                                    "citations_count": len(unique_citations),
                                    "citations": unique_citations[:5],  # Log first 5 citations for reference
                                    "pipeline": "google_ai_overview_extraction",
                                    "timestamp": datetime.now().isoformat()
                                })
                            except Exception as log_error:
                                print(f"⚠️ Failed to log citation-scraper trigger event: {log_error}")
                        else:
                            logger.warning("Failed to trigger citation-scraper lambda")
                            print(f"⚠️ Failed to trigger citation-scraper lambda")
                    else:
                        logger.info("No valid citations found, skipping citation-scraper invocation")
                        print(f"ℹ️ No valid citations found, skipping citation-scraper invocation")
                else:
                    logger.info("No external sources found, skipping citation-scraper invocation")
                    print(f"ℹ️ No external sources found, skipping citation-scraper invocation")
            except Exception as citation_error:
                logger.error(f"Error triggering citation-scraper lambda: {citation_error}")
                print(f"❌ Error triggering citation-scraper lambda: {citation_error}")
        
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
                # Check if we should retry on exception
                if retry_count < max_retries and should_retry_error(str(e)):
                    print(f"🔄 Exception occurred, will retry. Attempt {retry_count + 1}/{max_retries}")
                    update_task_status(task_id, f"retrying...({retry_count + 1})")
                    
                    # Trigger retry by invoking this lambda again with incremented retry count
                    retry_event = event.copy()
                    retry_event['retry_count'] = retry_count + 1
                    
                    try:
                        lambda_client = boto3.client('lambda')
                        lambda_client.invoke(
                            FunctionName=context.function_name,
                            InvocationType='Event',  # Asynchronous
                            Payload=json.dumps(retry_event)
                        )
                        print(f"✅ Retry invocation triggered for attempt {retry_count + 1}")
                        
                        return {
                            'statusCode': 202,
                            'body': json.dumps({
                                'message': f'Exception occurred, retry {retry_count + 1}/{max_retries} triggered',
                                'task_id': task_id,
                                'retry_count': retry_count + 1,
                                'error': str(e)
                            })
                        }
                    except Exception as retry_error:
                        print(f"❌ Failed to trigger retry: {retry_error}")
                        error_msg = f"Max retries ({max_retries}) reached. Last error: {str(e)}" if retry_count >= max_retries else f"Retry failed: {retry_error}"
                        update_task_status(task_id, "failed", error_message=error_msg)
                else:
                    # Max retries reached or non-retryable error
                    error_msg = f"Max retries ({max_retries}) reached. Last error: {str(e)}" if retry_count >= max_retries else str(e)
                    update_task_status(task_id, "failed", error_message=error_msg)
            except:
                pass
        
        # Log failure event
        if job_id:
            try:
                log_orchestration_event(job_id, "task_failed", {
                    "error": str(e),
                    "task_id": task_id,
                    "product_id": event.get('product_id'),  # Include product_id in failure logging
                    "pipeline": "google_ai_overview_extraction",
                    "timestamp": datetime.now().isoformat()
                })
            except:
                pass
                
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e), 'status': 'failed', 'timestamp': datetime.now().isoformat()}),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("🎯 Google AI Overview Extractor")
        print("\n🧠 Key Features:")
        print(" 1. ✅ Extracts Google's AI Overview content")
        print(" 2. ✅ Handles 'Show more' expansion")
        print(" 3. ✅ Saves raw HTML and processed markdown")
        print(" 4. ✅ Extracts external links and sources")
        print(" 5. ✅ Anti-CAPTCHA measures with human behavior")
        print(" 6. ✅ Debugging screenshots for each step")
        print("\n🚀 Flow:")
        print(" 1. 🌐 Navigate to Google search")
        print(" 2. 📝 Enter query and search")
        print(" 3. 🔍 Find AI Overview section")
        print(" 4. 🖱️ Click 'Show more' if available")
        print(" 5. 📸 Take screenshots")
        print(" 6. 📄 Extract HTML content")
        print(" 7. 🔍 Process with Crawl4AI")
        print(" 8. 🧠 Extract AI Overview content")
        print(" 9. 💾 Save results (HTML, markdown, parsed content)")
        print("\nUsage:")
        print(" python script.py 'best skincare brands in india'")
        print()
    
    asyncio.run(main())
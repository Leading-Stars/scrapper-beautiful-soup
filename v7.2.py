import os
import asyncio
import re
import json
import random
import requests
import psutil
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
import tracemalloc
import aiohttp
from itertools import islice

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
SEND_API_URL = "http://82.112.254.77:8000/queries/results"
DEFAULT_PARAMS = {
    "country": "usa_blockdata",
    "machine_id": "2"
}
CHUNK_SIZE = 50  # Increased from 20
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]
MAX_CONCURRENT_PAGES = 10  # Adjust based on system resources
EMAIL_EXTRACTION_TIMEOUT = 20
SCROLL_DELAY = 0.8  # Reduced from 1.5s
RETRY_LIMIT = 3
BATCH_SIZE = 50

SOCIAL_PATTERNS = [
    r"(?:facebook\.com|fb\.com)",
    r"(?:instagram\.com|instagr\.am)",
    r"(?:twitter\.com|x\.com)",
    r"linkedin\.com",
    r"youtube\.com|youtu\.be",
    r"tiktok\.com|tik\.to|tiktok\.tv",
    r"pinterest\.com",
    r"reddit\.com",
    r"whatsapp\.com",
]

SELECTORS = {
    "name": [
        ".qBF1Pd.fontHeadlineSmall",   # Best selector
        ".DUwDvf",                      # Fallback name
        ".hfpxzc[aria-label]",          # From link itself
    ],
    "rating": [
        ".e4rVHe.fontBodyMedium",      # e.g., "4.6 stars 180 reviews"
        ".ZkP5Je+span.e4rVHe",
        ".AJB7ye .e4rVHe",
        ".rsqaWe"
    ],
    "address": [
        ".W4Efsd span:nth-of-type(2)",
        ".W4Efsd span:-soup-contains('·')",
        ".Io6YTe.fontBodyMedium",
        ".section-info-text > span:first-child"
    ],
    "phone": [
        ".UsdlK",                       # Most modern Google Maps phone class
        ".W4Efsd span:nth-of-type(2):contains('(')",
        ".W4Efsd span:contains('Phone:')",
        "button[data-item-id='phone:tel'] > div.fontBodyMedium",
        "[data-section-id='pn0']"
    ],
    "website": [
        "a[href]:has(span:-soup-contains('Visit'))",
        ".etWJQ a[href]",
        "a[jslog*='action:pane.website']", 
        "[data-section-id='apn']",
        ".bIAO7b > a"
    ],
    "email": {
        "text_patterns": [r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"]
    },
    "social_links": {
        "patterns": SOCIAL_PATTERNS,
        "url_pattern": r"https?://[^\s\"'>]+"
    }
}

async def get_hrefs_with_retry(page, max_retries=3):
    for attempt in range(max_retries):
        try:
            # Try primary selector
            hrefs = await page.evaluate('''() => {
                const links = document.querySelectorAll('a.hfpxzc');
                return [...new Set([...links].map(l => l.href).filter(h => h.includes('/maps/place/')))];
            }''')
            
            if hrefs and len(hrefs) > 0:
                return hrefs
                
            # Fallback selector if first one fails
            hrefs = await page.evaluate('''() => {
                const links = document.querySelectorAll('a[href*="/maps/place/"]');
                return [...new Set([...links].map(l => l.href))];
            }''')
            
            if hrefs and len(hrefs) > 0:
                return hrefs
                
            return []
                
        except Exception as e:
            print(f"⚠️ Error extracting hrefs (attempt {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            continue
            
    return []

# --- Utility Functions ---
def is_valid_address(text):
    return bool(re.search(r'\d+|St\.? |Ave\.? |Blvd\.? |Rd\.? |Lane\.?', text))

def is_rating_string(text):
    return bool(re.fullmatch(r'\d+(\.\d+)?$$(\d+)$$', text))

def parse_rating_and_reviews(rating_block):
    if not rating_block:
        return None, None
    full_match = re.search(
        r'(\d+(?:\.\d+)?)(?:\sstars\s|\sreviews?\s)(\d+)', 
        rating_block
    )
    if full_match:
        return full_match.group(1), full_match.group(2)
    short_match = re.search(
        r'(\d+(?:\.\d+)?)[^\d]*(\d+)', 
        rating_block
    )
    if short_match:
        return short_match.group(1), short_match.group(2)
    return None, None

def extract_phone(full_text):
    us_phone_patterns = [
        r'\+1\s\d{3}-\d{3}-\d{4}',     # +1 555-123-4567
        r'$$\d{3}$$\s\d{3}-\d{4}',    # (555) 123-4567
        r'\d{3}-\d{3}-\d{4}'          # 555-123-4567
    ]
    for pattern in us_phone_patterns:
        match = re.search(pattern, full_text)
        if match:
            return match.group(0)
    return None

def get_first_text(soup, selectors, filter_invalid=True):
    invalid_keywords = {"photos", "write", "add", "videos", "menu", "share", "edit", "more", "visit"}
    for selector in selectors:
        try:
            elements = soup.select(selector)
            for el in elements:
                if el.name in ['script', 'style']:
                    continue
                text = el.get_text(strip=True)
                if text:
                    if filter_invalid:
                        if any(kw.lower() in text.lower() for kw in invalid_keywords):
                            continue
                        if is_rating_string(text):
                            continue
                    return text
        except Exception as e:
            print(f"⚠️ Error using selector '{selector}': {str(e)}")
    return None

def extract_social_links(full_text, patterns, url_pattern):
    urls = re.findall(url_pattern, full_text)
    matched = []
    for url in urls:
        for pattern in patterns:
            if re.search(pattern, url, re.IGNORECASE):
                matched.append(url)
                break
    return list(set(matched))

async def extract_email_from_website(url, session=None):
    try:
        # First try with requests for speed
        if session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=EMAIL_EXTRACTION_TIMEOUT)) as response:
                if response.status == 200:
                    text = await response.text()
                    for pattern in SELECTORS["email"]["text_patterns"]:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            return match.group(0)
        
        # Fallback to Playwright if requests fail
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            html = await page.content()
            await browser.close()
            soup = BeautifulSoup(html, "html.parser")
            full_text = soup.get_text(strip=True)
            
            for pattern in SELECTORS["email"]["text_patterns"]:
                match = re.search(pattern, full_text)
                if match:
                    return match.group(0)
    except Exception as e:
        print(f"🚨 Email extraction error: {str(e)}")
    return None

# --- Scrape Business Page Details ---
@retry(stop=stop_after_attempt(RETRY_LIMIT), wait=wait_exponential(multiplier=1, max=10))
async def scrape_place_details(html: str) -> dict:
    try:
        soup = BeautifulSoup(html, 'html.parser')
        full_text = soup.get_text(" ", strip=True)
        
        # Efficient name extraction with fallbacks
        name = None
        for selector in SELECTORS["name"]:
            name_element = soup.select_one(selector)
            if name_element:
                name = name_element.text.strip()
                if name:
                    break
        
        # Rating extraction
        rating_block = None
        rating_span = soup.select_one('span[role="img"]')
        if rating_span and rating_span.has_attr("aria-label"):
            rating_block = rating_span["aria-label"]
        elif soup.select_one('.DUwDvf'):
            rating_block = soup.select_one('.DUwDvf').next_sibling
            
        address = None
        for selector in SELECTORS["address"]:
            el = soup.select_one(selector)
            if el:
                text = el.text.strip()
                if text and is_valid_address(text):
                    address = text
                    break
        
        # Phone extraction
        phone = None
        for selector in SELECTORS["phone"]:
            el = soup.select_one(selector)
            if el:
                text = el.text.strip()
                potential_phone = extract_phone(text)
                if potential_phone:
                    phone = potential_phone
                    break
        
        # Website extraction
        website = None
        for selector in SELECTORS["website"]:
            el = soup.select_one(selector)
            if el and el.has_attr("href"):
                href = el["href"]
                if href.startswith(("http://", "https://")):
                    website = href
                    break
        
        # Social links extraction
        social_links = extract_social_links(
            full_text,
            SELECTORS["social_links"]["patterns"],
            SELECTORS["social_links"]["url_pattern"]
        )
        
        # Return results
        return {
            "name": name,
            "rating": rating_block.split()[0] if rating_block else None,
            "review_count": rating_block.split()[-1].replace('(', '').replace(')', '') if rating_block and len(rating_block.split()) > 1 else None,
            "address": address,
            "phone": phone,
            "website": website,
            "email": None,  # Will be filled later
            "social_links": social_links
        }
    except Exception as e:
        print(f"🚨 Error parsing details: {str(e)}")
        return {}

# --- Optimized Scrolling Function ---
async def optimized_scrolling(page):
    last_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.keyboard.press('PageDown')
        await asyncio.sleep(SCROLL_DELAY)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

# --- Concurrent Detail Scraping ---
async def scrape_detail_page(context, href, semaphore):
    async with semaphore:
        try:
            page = await context.new_page()
            await page.goto(href, timeout=60000)
            html = await page.content()
            await page.close()

            details = await scrape_place_details(html)
            if details.get("name"):
                details["source_url"] = href
                return details
        except Exception as e:
            print(f"🚨 Detail page error: {str(e)}")
            return None


# --- Main Scraper Function ---
async def scrape_google_maps_page(query_data, browser, email_session):
    industry = query_data.get("industry")
    lat = query_data.get("latitude")
    lon = query_data.get("longitude")
    zoom_level = query_data.get("zoom_level")
    query_id = query_data.get("id")
    results = []
    
    try:
        print(f"🔍 [{industry}] Starting scrape...")
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        page = await context.new_page()
        
        url = f"https://www.google.com/maps/search/{query_data.get('industry').replace(' ', '+')}/@{lat},{lon},{zoom_level}z?hl=en"
        print(url)
        await page.goto(url, timeout=120000)
        await optimized_scrolling(page)
        
        # Collect hrefs efficiently
        # Replace the old href extraction code with:
        hrefs = await get_hrefs_with_retry(page)
        print(f"🔗 [{industry}] Found {len(hrefs)} businesses.")
        
        # Concurrent detail scraping
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
        tasks = [scrape_detail_page(context, href, semaphore) for href in hrefs]
        details_list = await asyncio.gather(*tasks)
        
        # Extract emails concurrently
        valid_results = [d for d in details_list if d and d.get("name")]
        if valid_results:
            email_semaphore = asyncio.Semaphore(5)
            
            async def get_email_with_semaphore(result):
                async with email_semaphore:
                    if result.get("website") and not result.get("email"):
                        result["email"] = await extract_email_from_website(result["website"], email_session)
                    return result
            
            email_tasks = [get_email_with_semaphore(r) for r in valid_results]
            results = await asyncio.gather(email_tasks)
        
        await context.close()
        print(f"✅ [{industry}] Completed. Total records: {len(results)}")
        return {"id": query_id, "results": results}
        
    except Exception as e:
        print(f"🚨 Error in {industry}: {str(e)}")
        if 'context' in locals():
            await context.close()
        return {"id": query_id, "results": []}

# --- Send Scraped Data to API ---
def format_result_for_api(business, query_id, industry, source_url=""):
    return {
        "id": query_id,
        "title": business.get("name"),
        "category": industry,
        "address": business.get("address"),
        "phone": business.get("phone"),
        "website": business.get("website"),
        "email": business.get("email"),
        "star_rating": float(business.get("rating")) if business.get("rating") else None,
        "review_count": int(business.get("review_count")) if business.get("review_count") else None,
        "source_url": source_url or business.get("source_url", ""),
        "scraped_at": datetime.utcnow().isoformat() + "Z"
    }

def send_to_api(data):
    payload = {
        "country": DEFAULT_PARAMS["country"],
        "machine_id": DEFAULT_PARAMS["machine_id"],
        "status": "completed",
        "queries": data
    }
    try:
        response = requests.post(SEND_API_URL, json=payload, timeout=60)
        print(f"📤 Sent {len(data)} records. Status: {response.status_code}")
        try:
            print(json.dumps(response.json(), indent=2))
        except:
            print("Received non-JSON response.")
        return response.status_code == 200
    except Exception as e:
        print(f"📡 Error sending data: {str(e)}")
        return False

# --- Memory Usage Tracker ---
def print_memory_usage(message=""):
    mem = psutil.virtual_memory()
    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss / 1024 ** 2
    print(f"{message} | 🧠 RSS: {rss:.2f} MB | 💾 Available: {mem.available / 1024 ** 2:.2f} MB")

# --- Main Runner Loop ---
async def run_scrape_job():
    print("\n🔄 Starting optimized scrape job...")
    try:
        async with async_playwright() as p:
            # Launch browser once and reuse it
            browser = await p.chromium.launch(headless=True)
            
            # Create session pool for email extraction
            connector = aiohttp.TCPConnector(limit_per_host=5, ssl=False)
            session = aiohttp.ClientSession(connector=connector)
            
            while True:
                try:
                    # Fetch queries
                    response = requests.get(API_URL, timeout=30)
                    queries = response.json().get("queries", [])
                    
                    # Process queries concurrently
                    tasks = [scrape_google_maps_page(q, browser, session) for q in queries if valid_query(q)]
                    results = await asyncio.gather(*tasks)
                    
                    # Format results
                    all_results = []
                    for result_batch in results:
                        for business in result_batch.get("results", []):
                            all_results.append(format_result_for_api(
                                business, 
                                result_batch["id"], 
                                business.get("category")
                            ))
                    
                    # Send in batches
                    for i in range(0, len(all_results), BATCH_SIZE):
                        chunk = all_results[i:i+BATCH_SIZE]
                        send_to_api(chunk)
                        
                    await asyncio.sleep(10)  # Shorter wait between cycles
                    
                except Exception as e:
                    print(f"🚨 Cycle error: {str(e)}")
                    await asyncio.sleep(60)
                    
            await session.close()
            await browser.close()
            
    except Exception as e:
        print(f"🚨 Fatal error: {str(e)}")

def valid_query(query):
    required_fields = ["id", "industry", "latitude", "longitude", "zoom_level"]
    
    return all(field in query for field in required_fields)

# --- Start Task ---
if __name__ == "__main__":
    print_memory_usage("🚀 Initial memory")
    asyncio.run(run_scrape_job())
import os
import asyncio
import re
import aiohttp
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import psutil
import tracemalloc
from datetime import datetime
import json

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
SEND_API_URL = "http://82.112.254.77:8000/queries/results"
DEFAULT_PARAMS = {
    "country": "usa_blockdata",
    "machine_id": "2"
}
CHUNK_SIZE = 20  # Send every 20 records
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]
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

semaphore = asyncio.Semaphore(5)

# --- Utility Functions ---
def is_valid_address(text):
    return bool(re.search(r'\d+|St\.? |Ave\.? |Blvd\.? |Rd\.? |Lane\.?', text))

def is_rating_string(text):
    return bool(re.fullmatch(r'\d+(\.\d+)?$$(\d+)$$', text))

def parse_rating_and_reviews(rating_block):
    if not rating_block:
        return None, None
    full_match = re.search(
        r'(\d+(?:\.\d+)?).stars\s(\d+).(?:R|r)eviews', 
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
            pass
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

async def extract_email_from_website(url, session):
    try:
        async with session.get(url, ssl=False, timeout=10) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, "html.parser")
                full_text = soup.get_text(" ", strip=True)
                for pattern in SELECTORS["email"]["text_patterns"]:
                    match = re.search(pattern, full_text)
                    if match:
                        return match.group(0)
    except Exception as e:
        print(f"🚨 Error fetching website {url}: {str(e)}")
    return None

# --- Scrape Business Page Details ---
async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(" ", strip=True)
    
    name = get_first_text(soup, SELECTORS["name"])
    rating_block = None
    rating_span = soup.select_one('span[role="img"]')
    if rating_span and rating_span.has_attr("aria-label"):
        rating_block = rating_span["aria-label"]
    address = get_first_text(soup, SELECTORS["address"])
    if address and is_rating_string(address):
        rating_block = address
    rating, review_count = parse_rating_and_reviews(rating_block)
    if address and is_rating_string(address):
        address = None
    elif address and not is_valid_address(address):
        address = None
    phone = extract_phone(full_text)
    website = None
    for selector in SELECTORS["website"]:
        el = soup.select_one(selector)
        if el and el.has_attr("href"):
            href = el["href"]
            if href.startswith(("http://", "https://")):
                website = href
                break
    email = None
    for pattern in SELECTORS["email"]["text_patterns"]:
        match = re.search(pattern, full_text)
        if match:
            email = match.group(0)
            break
    
    social_links = extract_social_links(
        full_text,
        SELECTORS["social_links"]["patterns"],
        SELECTORS["social_links"]["url_pattern"]
    )

    return {
        "name": name,
        "rating": rating,
        "review_count": review_count,
        "address": address,
        "phone": phone,
        "website": website,
        "email": email,
        "social_links": social_links
    }

# --- Scrape Map Search Results ---
async def scrape_google_maps_page(query_data, context, session):
    industry = query_data.get("industry")
    lat = query_data.get("latitude")
    lon = query_data.get("longitude")
    zoom_level = query_data.get("zoom_level")
    query_id = query_data.get("id")

    results = []

    try:
        page = await context.new_page()
        query = industry.replace(" ", "+")
        url = f"https://www.google.com/maps/search/ {query}/@{lat},{lon},{zoom_level}z?hl=en"
        print(f"🔍 Navigating to: {url}")
        await page.goto(url, timeout=120000)

        try:
            await page.wait_for_selector('.Nv2PK', timeout=60000)
        except Exception:
            print(f"❌ [{industry}] No business cards found.")
            await page.close()
            return {"id": query_id, "results": []}

        prev_count = 0
        while True:
            cards = await page.query_selector_all('.Nv2PK')
            curr_count = len(cards)
            if curr_count == prev_count:
                break
            prev_count = curr_count
            await page.keyboard.press('PageDown')
            await asyncio.sleep(0.5)

        hrefs = set()
        cards = await page.query_selector_all('.Nv2PK')
        for card in cards:
            link_el = await card.query_selector('a.hfpxzc')
            if link_el:
                href = await link_el.get_attribute('href')
                if href and '/maps/place/' in href:
                    hrefs.add(href)

        hrefs = list(hrefs)
        print(f"🔗 [{industry}] Found {len(hrefs)} businesses.")

        tasks = [scrape_detail(link, context, query_id, industry, session) for link in hrefs]
        batch_results = await asyncio.gather(*tasks)
        results = [r for r in batch_results if r]

        await page.close()

    except Exception as e:
        print(f"🚨 Critical error scraping '{industry}': {str(e)}")

    return {"id": query_id, "results": results}

async def scrape_detail(link, context, query_id, industry, session):
    async with semaphore:
        try:
            page = await context.new_page()
            await page.goto(link, timeout=120000)
            html = await page.content()
            await page.close()
            details = await scrape_place_details(html)
            if details.get("name"):
                return format_result_for_api(details, query_id, industry, link)
        except Exception as e:
            print(f"🚨 Failed to scrape detail page: {str(e)}")
        return None

# --- Format Result ---
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
        "source_url": source_url or business.get("source_url", ""),
        "scraped_at": datetime.utcnow().isoformat() + "Z"
    }

# --- Send Scraped Data to API ---
async def send_to_api(data):
    payload = {
        "country": DEFAULT_PARAMS["country"],
        "machine_id": DEFAULT_PARAMS["machine_id"],
        "status": "completed",
        "queries": data
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(SEND_API_URL, json=payload, ssl=False, timeout=60) as response:
                print(f"📤 Sent {len(data)} records. Status: {response.status}")
                try:
                    res_json = await response.json()
                    print(json.dumps(res_json, indent=2))
                except:
                    print("Received non-JSON response.")
                return response.status == 200
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
    print("\n🔄 Starting scheduled scrape job...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    print("📥 Fetching queries from API...")
                    async with session.get(API_URL, ssl=False, timeout=30) as resp:
                        if resp.status != 200:
                            print("💤 API returned non-200 status. Retrying...")
                            await asyncio.sleep(60)
                            continue
                        data = await resp.json()
                        queries = data.get("queries", [])
                        if not queries:
                            print("💤 No queries returned from API. Waiting before retry...")
                            await asyncio.sleep(60)
                            continue
                        all_results = []
                        for query in queries:
                            if not isinstance(query, dict):
                                continue
                            required_fields = ["id", "industry", "latitude", "longitude", "zoom_level"]
                            if not all(field in query for field in required_fields):
                                print(f"⚠️ Skipping incomplete query: {query}")
                                continue
                            result_batch = await scrape_google_maps_page(query, context, session)
                            query_id = result_batch.get("id")
                            batch_results = result_batch.get("results", [])
                            all_results.extend(batch_results)

                            # Chunk and send
                            while len(all_results) >= CHUNK_SIZE:
                                chunk = all_results[:CHUNK_SIZE]
                                success = await send_to_api(chunk)
                                if success:
                                    all_results = all_results[CHUNK_SIZE:]
                                else:
                                    await asyncio.sleep(10)

                        if all_results:
                            await send_to_api(all_results)

                except Exception as e:
                    print(f"🚨 Error fetching queries: {str(e)}")
                    await asyncio.sleep(60)

# --- Start Task ---
if __name__ == "__main__":
    print_memory_usage("🚀 Initial memory")
    asyncio.run(run_scrape_job())
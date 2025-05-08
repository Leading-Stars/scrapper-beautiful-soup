import os
import asyncio
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import requests
import csv
import psutil

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
GOOGLE_MAPS_TEMPLATE = "https://www.google.com/maps/search/{query}/@{lat},{lon},{zoom_level}z"

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
        ".hfpxzc[aria-label]"           # From link itself
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


def print_memory_usage(message=""):
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    print(f"{message} | 🧠 Memory: {mem_info.rss / 1024 ** 2:.2f} MB")

def extract_social_links(full_text, patterns, url_pattern):
    # First extract all possible URLs from text
    urls = re.findall(url_pattern, full_text)
    
    # Then filter only those matching social patterns
    matched = []
    for url in urls:
        for pattern in patterns:
            if re.search(pattern, url, re.IGNORECASE):
                matched.append(url)
                break
    
    return list(set(matched))
# --- Utility Functions ---

def is_valid_address(text):
    return bool(re.search(r'\d+|St\.? |Ave\.? |Blvd\.? |Rd\.? |Lane\.?', text))

def is_valid_website(url):
    return url.startswith(("http://", "https://")) and not url.startswith("mailto:")

def get_first_text(soup, selectors, filter_invalid=True):
    """
    Try multiple CSS selectors until one returns valid text.
    Skips invalid content like scripts or UI strings.
    """
    # Keywords to skip (common Google Maps UI text)
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
                        # Skip if text contains rating pattern or common UI words
                        if any(kw.lower() in text.lower() for kw in invalid_keywords):
                            continue
                        if re.search(r'\d+\.\d+$$(\d+)$$|\d+$$(\d+)$$', text):
                            continue
                    return text
        except Exception as e:
            print(f"⚠️ Error using selector '{selector}': {str(e)}")
    return None
def extract_email(full_text, patterns):
    for pattern in patterns:
        match = re.search(pattern, full_text)
        if match:
            return match.group(0)
    return None
async def extract_email_from_website(url):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(url, timeout=60000)
            html = await page.content()
            soup = BeautifulSoup(html, "html.parser")
            full_text = soup.get_text(strip=True)

            for pattern in SELECTORS["email"]["text_patterns"]:
                match = re.search(pattern, full_text)
                if match:
                    await browser.close()
                    return match.group(0)
            await browser.close()
    except Exception as e:
        print(f"🚨 Error fetching website {url}: {str(e)}")
    return None

def parse_rating_and_reviews(rating_block):
    """
    Tries to extract rating & review count from:
        - '4.8 stars 36 Reviews' (FULL)
        - '4.8(36)' (SHORT)
    Returns:
        tuple: (rating, review_count)
    """
    if not rating_block:
        return None, None

    # First try full pattern: "4.8 stars 36 Reviews"
    full_match = re.search(
        r'(\d+(?:\.\d+)?)\sstars\s(\d+)\s(?:R|r)eviews', 
        rating_block
    )
    if full_match:
        return full_match.group(1), full_match.group(2)

    # If full match fails, try short pattern: "4.8(36)"
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

# --- Scrape Business Page Details ---
async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(" ", strip=True)

    name = None
    for selector in SELECTORS["name"]:
        el = soup.select_one(selector)
        if el:
            name = el.get_text(strip=True)
            break

    # --- RATING & REVIEW COUNT ---
    rating_block = None
    rating_span = soup.select_one('span[role="img"].ZkP5Je')
    if rating_span and rating_span.has_attr("aria-label"):
        rating_block = rating_span["aria-label"]

    rating, review_count = parse_rating_and_reviews(rating_block)

    # --- ADDRESS ---
    address = get_first_text(soup, SELECTORS["address"])

    # Discard if it looks like a rating string (like "4.0(30)")
    if address and re.fullmatch(r'\d+(\.\d+)?$$(\d+)$$', address):
        address = None


    

    # Only save if it has real address-like content
    if address and not is_valid_address(address):
        address = None

    # --- PHONE ---
    phone = extract_phone(full_text)

    # --- WEBSITE ---
    website = None
    for selector in SELECTORS["website"]:
        el = soup.select_one(selector)
        if el and el.has_attr("href"):
            href = el["href"]
            if is_valid_website(href):
                website = href
                break

    # --- EMAIL ---
    email = extract_email(full_text, SELECTORS["email"]["text_patterns"])
    
    # Try email from website if not found on Google Maps
    if not email and website:
        email = await extract_email_from_website(website)

    # --- SOCIAL LINKS ---
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
        "social_links": list(set(social_links))
    }

# --- Scrape Map Search Results ---
async def scrape_google_maps_page(industry, lat, lon, zoom_level):
    results = []

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()

            query = industry.replace(" ", "+")
            url = GOOGLE_MAPS_TEMPLATE.format(query=query, lat=lat, lon=lon, zoom_level=zoom_level)
            print(f"🔍 Navigating to: {url}")
            await page.goto(url, timeout=120000)

            # Wait for map pins
            try:
                await page.wait_for_selector('.Nv2PK', timeout=60000)
            except Exception:
                print("❌ No business cards found.")
                await context.close()
                await browser.close()
                return {"industry": industry, "results": []}

            # Scroll to load more results
            last_height = await page.evaluate("document.body.scrollHeight")
            while True:
                await page.keyboard.press('PageDown')
                await asyncio.sleep(1.5)
                new_height = await page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height

            # Extract all business links
            cards = await page.query_selector_all('.Nv2PK')
            hrefs = set()
            for card in cards:
                link_el = await card.query_selector('a.hfpxzc')
                if link_el:
                    href = await link_el.get_attribute('href')
                    if href and '/maps/place/' in href:
                        hrefs.add(href)

            hrefs = list(hrefs)
            print(f"🔗 Found {len(hrefs)} businesses.")

            for link in hrefs:
                try:
                    new_page = await context.new_page()
                    await new_page.goto(link, timeout=120000)
                    html = await new_page.content()
                    await new_page.close()

                    details = await scrape_place_details(html)

                    if details.get("name"):
                        results.append(details)

                except Exception as e:
                    print(f"🚨 Failed to scrape detail page: {str(e)}")

            await context.close()
            await browser.close()

    except Exception as e:
        print(f"🚨 Critical error scraping '{industry}': {str(e)}")

    return {"industry": industry, "results": results}

# --- Save to CSV ---
OUTPUT_CSV = "businesses.csv"

def save_to_csv(data, industry):
    file_exists = os.path.isfile(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "industry", "name", "rating", "review_count", "address", "phone", "website", "email", "social_links"
            ])
        writer.writerow([
            industry,
            data.get("name"),
            data.get("rating"),
            data.get("review_count"),
            data.get("address"),
            data.get("phone"),
            data.get("website"),
            data.get("email"),
            "|".join(data.get("social_links", []))
        ])

# --- Main Runner ---
async def run_scrape_job():
    print("\n🔄 Starting scheduled scrape job...")
    try:
        response = requests.get(API_URL, timeout=30)
        data = response.json()
        queries = data.get("queries", [])

        if not queries:
            print("❌ No queries returned from API.")
            return

        tasks = []
        for i, query in enumerate(queries):
            if not isinstance(query, dict):
                print(f"⚠️ Skipping invalid query at index {i}: {query}")
                continue

            industry = query.get("industry")
            lat = query.get("latitude")
            lon = query.get("longitude")
            zoom_level = query.get("zoom_level")

            if not all([industry, lat, lon, zoom_level]):
                print(f"⚠️ Missing required fields in query: {query}")
                continue

            task = scrape_google_maps_page(industry, lat, lon, zoom_level)
            tasks.append(task)

        all_results = await asyncio.gather(*tasks)

        for result in all_results:
            print(f"\n📌 Industry: {result['industry']}")
            for business in result['results']:
                print(business)
                save_to_csv(business, result['industry'])

    except Exception as e:
        print(f"🚨 Error fetching queries: {str(e)}")

def scheduled_task():
    asyncio.run(run_scrape_job())

if __name__ == "__main__":
    scheduled_task()
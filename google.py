import os
import asyncio
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import requests
from datetime import datetime

def save_link_to_file(link):
    with open("found_links.txt", "a", encoding="utf-8") as f:
        f.write(link + "\n")
    print(f"🔗 Saved link: {link}")

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
GOOGLE_MAPS_TEMPLATE = "https://www.google.com/maps/search/{query}/@{lat},{lon},{zoom_level}z?hl=en"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

OUTPUT_HTML_DIR = "debug_html"
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)

SELECTORS = {
    "name": [
        ".qBF1Pd.fontHeadlineSmall",   # Best selector
        ".DUwDvf",
        ".hfpxzc[aria-label]",         # From link itself
    ],
    "rating": [
        ".e4rVHe.fontBodyMedium",      # e.g., "4.6 stars 180 reviews"
        ".ZkP5Je+span.e4rVHe",
        ".AJB7ye .e4rVHe",
        ".rsqaWe"
    ],
    "review_count": [
        ".e4rVHe.fontBodyMedium",      # Same as rating but parsed differently
        ".ZkP5Je+span.e4rVHe",
        ".yFnQ8c > span"               # Alternative review count container
    ],
    "address": [
        ".W4Efsd span:nth-of-type(2)", # Common structure
        ".W4Efsd span:contains('·')",  # Span with separator
        ".Io6YTe.fontBodyMedium",      # Older selector
        ".section-info-text > span:first-child"
    ],
    "phone": [
        ".UsdlK",                      # New reliable phone selector
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
        "domains": ["facebook.com", "instagram.com", "twitter.com", "linkedin.com"],
        "url_pattern": r"https?://[^\s\"'>]+"
    }
}

# --- Utility Functions ---
def get_first_text(soup, selectors, filter_invalid=True):
    """Try multiple selectors until one returns a non-empty string."""
    invalid_keywords = {"photos", "write", "add", "videos", "menu", "share", "edit"}
    
    for selector in selectors:
        elements = soup.select(selector)
        for el in elements:
            text = el.get_text(strip=True)
            if text:
                if filter_invalid and any(kw in text.lower() for kw in invalid_keywords):
                    continue
                return text
    return None


async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(" ", strip=True)

    name = get_first_text(soup, SELECTORS["name"], filter_invalid=True)
    
    rating_block = get_first_text(soup, SELECTORS["rating"])
    rating = None
    review_count = None
    if rating_block:
        rating_match = re.search(r'(\d+(\.\d+)?)', rating_block)
        rating = rating_match.group(1) if rating_match else None
        review_match = re.search(r'$$(\d+)$$', rating_block)
        review_count = review_match.group(1) if review_match else None

    address = get_first_text(soup, SELECTORS["address"])
    
    phone = get_first_text(soup, SELECTORS["phone"])

    website = None
    for selector in SELECTORS["website"]:
        el = soup.select_one(selector)
        if el and el.has_attr("href"):
            website = el["href"]
            break

    email = extract_email(full_text, SELECTORS["email"]["text_patterns"])
    social_links = extract_social_links(
        full_text,
        SELECTORS["social_links"]["domains"],
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
        "social_links": list(set(social_links))  # Deduplicate
    }
def extract_email(full_text, patterns):
    import re
    for pattern in patterns:
        match = re.search(pattern, full_text)
        if match:
            return match.group(0)
    return None


def extract_social_links(full_text, domains, url_pattern):
    import re
    urls = re.findall(url_pattern, full_text)
    return [url for url in urls if any(domain in url.lower() for domain in domains)]


async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(" ", strip=True)

    name = get_first_text(soup, SELECTORS["name"])
    rating = get_first_text(soup, SELECTORS["rating"])

    # Special handling for review count
    review_count = None
    review_text = get_first_text(soup, SELECTORS["review_count"])
    if review_text and "reviews" in review_text:
        review_count = review_text

    address = get_first_text(soup, SELECTORS["address"])
    phone = get_first_text(soup, SELECTORS["phone"])

    website = None
    for selector in SELECTORS["website"]:
        el = soup.select_one(selector)
        if el and el.has_attr("href"):
            website = el["href"]
            break

    email = extract_email(full_text, SELECTORS["email"]["text_patterns"])

    social_links = extract_social_links(
        full_text,
        SELECTORS["social_links"]["domains"],
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
        "social_links": list(set(social_links))  # Remove duplicates
    }


async def scrape_google_maps_page(industry, lat, lon, zoom_level, proxy=None):
    results = []

    try:
        async with async_playwright() as p:
            # Launch Chromium in headless mode
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-setuid-sandbox",
                    "--no-first-run",
                    "--no-sandbox",
                    "--ignore-certificate-errors"
                ]
            )
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()

            query = industry.replace(" ", "+")
            url = GOOGLE_MAPS_TEMPLATE.format(query=query, lat=lat, lon=lon, zoom_level=zoom_level)
            print(f"🔍 Navigating to: {url}")
            await page.goto(url, timeout=120000)

            # Wait for map pins to load
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

            # Extract links from business cards
            hrefs = set()
            cards = await page.query_selector_all('.Nv2PK')

            for card in cards:
                link_el = await card.query_selector('a.hfpxzc')
                if link_el:
                    href = await link_el.get_attribute('href')
                    if href and '/maps/place/' in href:
                        hrefs.add(href)

            hrefs = list(hrefs)
            print(f"🔗 Found {len(hrefs)} businesses.")
            print(f"🔗 Example: {hrefs[:3]}")

            for link in hrefs[:3]:  # Limit to 3 for demo
                try:
                    new_page = await context.new_page()
                    await new_page.goto(link, timeout=120000)
                    await new_page.wait_for_timeout(5000)

                    html = await new_page.content()
                    details = await scrape_place_details(html)

                    if details.get("name"):
                        results.append(details)

                    await new_page.close()

                except Exception as e:
                    print(f"🚨 Failed to scrape detail page: {str(e)}")

            await context.close()
            await browser.close()

    except Exception as e:
        print(f"🚨 Critical error scraping '{industry}': {str(e)}")

    return {"industry": industry, "results": results}


# --- Main Runner & Scheduler ---

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
        async with async_playwright() as p:
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

    except Exception as e:
        print(f"🚨 Error fetching queries: {str(e)}")


def scheduled_task():
    asyncio.run(run_scrape_job())


if __name__ == "__main__":
    scheduled_task()
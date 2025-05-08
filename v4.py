import os
import asyncio
import re
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import requests

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
GOOGLE_MAPS_TEMPLATE = "https://www.google.com/maps/search/{query}/@{lat},{lon},{zoom_level}z"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

SELECTORS = {
    "name": [
        ".qBF1Pd.fontHeadlineSmall",   # Most reliable current name container
        ".DUwDvf",                      # Older but still used
        ".hfpxzc[aria-label]",          # From link's aria-label if all else fails
    ],
    "rating": [
        ".ZkP5Je+span.e4rVHe",
        ".AJB7ye .e4rVHe",
        ".rsqaWe"
    ],
    "address": [
        ".W4Efsd span:nth-of-type(2)", # Common structure
        ".W4Efsd span:-soup-contains('·')",  # Span with separator
        ".Io6YTe.fontBodyMedium",       # Older selector
        ".section-info-text > span:first-child"
    ],
    "phone": [
        ".UsdlK",                      # Modern phone class
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
        "domains": ["facebook.com", "instagram.com", "twitter.com", "linkedin.com", "youtube.com", "tiktok.com"],
        "url_pattern": r"https?://[^\s\"'>]+"
    }
}

# --- Utility Functions ---

def is_valid_address(text):
    return bool(re.search(r'\d+|St\.? |Ave\.? |Blvd\.? |Rd\.? |Lane\.? ', text))

def is_valid_website(url):
    return url.startswith(("http://", "https://")) and not url.startswith(("tel:", "mailto:"))

def extract_email(full_text, patterns):
    for pattern in patterns:
        match = re.search(pattern, full_text)
        if match:
            return match.group(0)
    return None

def extract_social_links(full_text, domains, url_pattern):
    urls = re.findall(url_pattern, full_text)
    result = []
    for url in urls:
        if any(domain in url.lower() for domain in domains):
            result.append(url)
    return list(set(result))

async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    full_text = soup.get_text(strip=True)

    name = None
    for selector in SELECTORS["name"]:
        el = soup.select_one(selector)
        if el:
            name = el.get_text(strip=True)
            break

    # --- RATING & REVIEW COUNT ---
    rating_block = None
    rating_span = soup.select_one('span[role="img"]')
    if rating_span and rating_span.has_attr("aria-label"):
        rating_block = rating_span["aria-label"]

    rating = None
    review_count = None
    if rating_block:
        full_match = re.search(r'(\d+(?:\.\d+)?)\sstars\s(\d+)\s(?:R|r)eviews', rating_block)
        if full_match:
            rating = full_match.group(1)
            review_count = full_match.group(2)
        else:
            short_match = re.search(r'(\d+(?:\.\d+)?)[^\d]*(\d+)', rating_block)
            if short_match:
                rating = short_match.group(1)
                review_count = short_match.group(2)

    # --- ADDRESS ---
    address = None
    for selector in SELECTORS["address"]:
        el = soup.select_one(selector)
        if el:
            text = el.get_text(strip=True)
            if is_valid_address(text):
                address = text
                break

    # --- PHONE ---
    phone = None
    for selector in SELECTORS["phone"]:
        el = soup.select_one(selector)
        if el:
            phone = el.get_text(strip=True)
            if phone:
                break

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

    # --- SOCIAL LINKS ---
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
        "social_links": social_links
    }

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
            hrefs = set()
            cards = await page.query_selector_all('.Nv2PK')
            for card in cards:
                link_el = await card.query_selector('a.hfpxzc')
                if link_el:
                    link = await link_el.get_attribute('href')
                    if link and '/maps/place/' in link:
                        hrefs.add(link)

            hrefs = list(hrefs)
            print(f"🔗 Found {len(hrefs)} businesses.")

            for link in hrefs:
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

# --- Main Runner ---

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
import os
import asyncio
import re
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import psutil
from datetime import datetime
from itertools import cycle
import phonenumbers
import time



# --- Configurable Settings ---
LINKS_FILE = "links.json"
OUTPUT_FILE = "output_results.json"
MAX_CONCURRENT_PAGES = 5  # Increase based on system resources
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
    r"whatsapp\.com"
]

SELECTORS = {
    "name": [".qBF1Pd.fontHeadlineSmall", ".DUwDvf", ".hfpxzc[aria-label]"],
    "rating": [".e4rVHe.fontBodyMedium", ".ZkP5Je+span.e4rVHe", ".AJB7ye .e4rVHe", ".rsqaWe"],
    "address": [".W4Efsd span:nth-of-type(2)", ".W4Efsd span:-soup-contains('·')", ".Io6YTe.fontBodyMedium"],
    "phone": [".UsdlK", ".W4Efsd span:nth-of-type(2):contains('(')", "[data-item-id='phone:tel'] > div.fontBodyMedium"],
    "website": ["a[href]:has(span:-soup-contains('Visit'))", ".etWJQ a[href]", "[data-section-id='apn']"],
    "email": {"text_patterns": [r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"]},
    "social_links": {"patterns": SOCIAL_PATTERNS, "url_pattern": r"https?://[^\s\"'>]+"}
}



# --- Utility Functions ---
def is_valid_address(text):
    return bool(re.search(r'\d+|St\.? |Ave\.? |Blvd\.? |Rd\.? |Lane\.?', text))

def is_rating_string(text):
    return bool(re.fullmatch(r'\d+(\.\d+)?$$(\d+)$$', text))


def extract_phone(full_text):
    for match in phonenumbers.PhoneNumberMatcher(full_text, None):
        return phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
    return None


def parse_rating_and_reviews(rating_block):
    if not rating_block:
        return None, None
    full_match = re.search(r'(\d+(?:\.\d+)?).*stars.*(\d+)', rating_block)
    if full_match:
        return full_match.group(1), full_match.group(2)
    short_match = re.search(r'(\d+(?:\.\d+)?)[^\d]*(\d+)', rating_block)
    if short_match:
        return short_match.group(1), short_match.group(2)
    return None, None

# def extract_phone(full_text):
#     patterns = [r'\+1\s\d{3}-\d{3}-\d{4}', r'$$\d{3}$$\s\d{3}-\d{4}', r'\d{3}-\d{3}-\d{4}']
#     for pattern in patterns:
#         match = re.search(pattern, full_text)
#         if match:
#             return match.group(0)
#     return None

def get_first_text(soup, selectors, filter_invalid=True):
    invalid_keywords = {"photos", "write", "add", "videos", "menu", "share", "edit", "more", "visit"}
    for selector in selectors:
        elements = soup.select(selector)
        for el in elements:
            if el.name in ['script', 'style']:
                continue
            text = el.get_text(strip=True)
            if text:
                if filter_invalid and any(kw.lower() in text.lower() for kw in invalid_keywords):
                    continue
                if is_rating_string(text):
                    continue
                return text
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

async def extract_email_from_website(page):
    try:
        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
        full_text = soup.get_text(strip=True)
        for pattern in SELECTORS["email"]["text_patterns"]:
            match = re.search(pattern, full_text)
            if match:
                return match.group(0)
    except Exception as e:
        print(f"🚨 Error fetching website: {str(e)}")
    return None

# --- Helper: Clean Place URL from Deep Link ---
def clean_place_url(href):
    # Remove any spaces that may have been accidentally included
    href = href.replace(" ", "")

    # Extract place_id from '!1s...' pattern
    match = re.search(r'!1s([^:!]+)(?::[^!]*)?', href)
    if match:
        place_id = match.group(1)
        return f"https://www.google.com/maps/place/?q=place_id :{place_id}"
    
    # If no match, try to extract from '19s' which also sometimes contains place_id
    match = re.search(r'!19s([^:!]+)(?::[^!]*)?', href)
    if match:
        place_id = match.group(1)
        return f"https://www.google.com/maps/place/?q=place_id :{place_id}"

    # As fallback, return original if already valid
    if href.startswith("https://www.google.com/maps/place/?q=place_id :"):
        return href

    return None  # Or raise an error if needed

# --- Scrape Business Page Details ---
async def scrape_place_details(context, link):
    page = await context.new_page()
    try:
        cleaned_link = link.replace(" ", "")#clean_place_url(link)
        print(f"📄 Scraping: {cleaned_link}")
        await page.goto(cleaned_link, timeout=60000)
        await page.wait_for_timeout(2000)  # Allow JS to load

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")
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
        if address and not is_valid_address(address):
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
        if not email and website:
            try:
                await page.goto(website, timeout=30000)
                email = await extract_email_from_website(page)
            except:
                pass

        social_links = extract_social_links(full_text, SOCIAL_PATTERNS, SELECTORS["social_links"]["url_pattern"])

        result = {
            "name": name,
            "rating": rating,
            "review_count": review_count,
            "address": address,
            "phone": phone,
            "website": website,
            "email": email,
            "social_links": social_links,
            "source_url": link
        }

        print(f"✅ Success: {name}")
        return result
    except Exception as e:
        print(f"❌ Failed to scrape {link}: {str(e)}")
        return {"error": str(e), "source_url": link}
    finally:
        await page.close()

# --- Main Scraper Runner ---
async def main():
    print("📥 Loading links...")
    with open(LINKS_FILE, "r") as f:
        links = json.load(f)

    print(f"🔗 Loaded {len(links)} links.")

    start_time = time.time()

    BATCH_SIZE = 5  # Change this number if you want different batch sizes

    all_results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        contexts = [await browser.new_context(user_agent=random.choice(USER_AGENTS)) for _ in range(MAX_CONCURRENT_PAGES)]

        # Use a page pool for context rotation
        page_pool = cycle(contexts)

        # Split links into batches
        for i in range(0, len(links), BATCH_SIZE):
            batch_links = links[i:i + BATCH_SIZE]
            print(f"\n🔄 Processing batch {i // BATCH_SIZE + 1}: {len(batch_links)} links")

            tasks = []
            for link in batch_links:
                context = next(page_pool)
                tasks.append(scrape_place_details(context, link))

            results = await asyncio.gather(*tasks)
            all_results.extend(results)

            print(f"✅ Batch {i // BATCH_SIZE + 1} completed.")

        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"\n📊 Total records scraped: {len(all_results)}")
        print(f"⏱️ Total time taken: {elapsed_time:.2f} seconds")

        with open(OUTPUT_FILE, "w") as f:
            json.dump(all_results, f, indent=2)

        print(f"💾 Results saved to: {OUTPUT_FILE}")

        await browser.close()
# --- Start Task ---
if __name__ == "__main__":
    asyncio.run(main())
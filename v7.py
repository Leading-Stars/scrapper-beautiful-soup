import os
import asyncio
import re
import json
import time
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
import random
import requests
import psutil
import tracemalloc

# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries"
DEFAULT_PARAMS = {
    "country": "usa_blockdata",
    "machine_id": "your-machine-id"  # Replace with your actual machine ID
}
CHUNK_SIZE = 20  # Send every 20 results

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

SELECTORS = {
    "result_card": ".Nv2PK.THOPZb.CEt8oc",
    "result_link": "a.hfpxzc",
    "name": [".qBF1Pd.fontHeadlineSmall", ".DUwDvf"],
    "rating": [".e4rVHe.fontBodyMedium", ".rsqaWe"],
    "address": [".Io6YTe.fontBodyMedium"],
    "phone": ["button[data-item-id='phone:tel'] > div.fontBodyMedium"],
    "website": ["a[jslog*='action:pane.website']"],
}

# --- Utility Functions ---
def get_first_text(soup, selectors):
    for selector in selectors:
        el = soup.select_one(selector)
        if el:
            return el.text.strip()
    return None

def parse_rating(text):
    match = re.search(r'(\d+(?:\.\d+)?).*?(\d+)', text or "")
    if match:
        return float(match.group(1)), int(match.group(2))
    return None, None

# --- Scrape Business Page Details ---
async def scrape_place_details(html: str) -> dict:
    soup = BeautifulSoup(html, 'html.parser')
    name = get_first_text(soup, SELECTORS["name"])
    rating_str = get_first_text(soup, SELECTORS["rating"])
    rating, review_count = parse_rating(rating_str)
    address = get_first_text(soup, SELECTORS["address"])
    phone = get_first_text(soup, SELECTORS["phone"])

    website_el = soup.select_one(SELECTORS["website"])
    website = website_el['href'] if website_el else None

    return {
        "title": name,
        "star_rating": rating,
        "review_count": review_count,
        "address": address,
        "phone": phone,
        "website": website,
        "source_url": "",
        "scraped_at": datetime.utcnow().isoformat() + "Z"
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
            url = f"https://www.google.com/maps/search/ {query}/@{lat},{lon},{zoom_level}z?hl=en"
            print(f"🔍 Navigating to: {url}")
            await page.goto(url, timeout=120000)

            # Wait for map pins
            try:
                await page.wait_for_selector(SELECTORS["result_card"], timeout=60000)
            except Exception:
                print(f"❌ [{industry}] No business cards found.")
                return []

            prev_len = 0
            while True:
                cards = await page.query_selector_all(SELECTORS["result_card"])
                curr_len = len(cards)
                if curr_len == prev_len:
                    break
                prev_len = curr_len
                await page.keyboard.press('PageDown')
                await asyncio.sleep(1.5)

            hrefs = set()
            cards = await page.query_selector_all(SELECTORS["result_card"])
            for card in cards:
                link = await card.query_selector(SELECTORS["result_link"])
                if link:
                    href = await link.get_attribute('href')
                    if href and '/maps/place/' in href:
                        hrefs.add(href)
            hrefs = list(hrefs)
            print(f"🔗 [{industry}] Found {len(hrefs)} businesses.")

            for idx, link in enumerate(hrefs):
                try:
                    new_page = await context.new_page()
                    await new_page.goto(link, timeout=120000)
                    html = await new_page.content()
                    details = await scrape_place_details(html)
                    details["category"] = industry
                    details["source_url"] = link
                    results.append(details)
                    await new_page.close()
                except Exception as e:
                    print(f"🚨 Failed to scrape detail page: {str(e)}")

            await context.close()
            await browser.close()
    except Exception as e:
        print(f"🚨 Critical error scraping '{industry}': {str(e)}")
    return results

# --- Send Scraped Data to API ---
def send_to_api(data, country, machine_id):
    payload = {
        "country": country,
        "machine_id": machine_id,
        "queries": data
    }
    try:
        response = requests.post(API_URL, json=payload, timeout=60)
        print(f"📤 Sent {len(data)} records. Status: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"📡 Error sending data: {str(e)}")
        return False

# --- Get New Queries from API ---
def fetch_new_queries():
    params = DEFAULT_PARAMS.copy()
    try:
        response = requests.get(API_URL, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            queries = data.get("queries", [])
            print(f"📥 Fetched {len(queries)} new queries.")
            return queries
        else:
            print(f"❌ Failed to fetch queries: {response.status_code}")
    except Exception as e:
        print(f"📡 Error fetching queries: {str(e)}")
    return []

# --- Main Runner Loop ---
async def run_scrape_job():
    print("\n🔄 Starting scheduled scrape job...")
    while True:
        queries = fetch_new_queries()
        if not queries:
            print("💤 No more queries. Waiting before retry...")
            time.sleep(60)
            continue

        all_results = []
        for query in queries:
            industry = query.get("industry")
            lat = query.get("latitude")
            lon = query.get("longitude")
            zoom_level = query.get("zoom_level")
            if not all([industry, lat, lon, zoom_level]):
                continue

            print(f"\n📌 Processing: {industry}")
            result_batch = await scrape_google_maps_page(industry, lat, lon, zoom_level)
            all_results.extend(result_batch)

            # Send in chunks
            if len(all_results) >= CHUNK_SIZE:
                success = send_to_api(all_results[:CHUNK_SIZE], DEFAULT_PARAMS["country"], DEFAULT_PARAMS["machine_id"])
                if success:
                    all_results = all_results[CHUNK_SIZE:]

        # Send any remaining
        if all_results:
            send_to_api(all_results, DEFAULT_PARAMS["country"], DEFAULT_PARAMS["machine_id"])

# --- Start Task ---
if __name__ == "__main__":
    asyncio.run(run_scrape_job())
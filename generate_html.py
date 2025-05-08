import os
import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
import requests
import random


# --- Configurable Settings ---
API_URL = "http://82.112.254.77:8000/queries?country=usa_blockdata&machine_id=2"
OUTPUT_HTML_DIR = "minimal_html"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
]

# Resource types to block
BLOCKED_RESOURCE_TYPES = [
    "image",          # Images (.png, .jpg, etc.)
    "stylesheet",     # CSS files
    "font",           # Fonts (.woff, .ttf, etc.)
    "media",          # Audio/video
    "iframe",         # Embedded iframes (e.g., maps)
    "websocket",      # Real-time connections
    "manifest",       # App manifest
    "xhr",            # AJAX/XHR calls
    "fetch",          # fetch() API calls
    "other"           # Other misc. network requests
]


# --- Create Output Folder ---
os.makedirs(OUTPUT_HTML_DIR, exist_ok=True)


async def save_minimal_html(industry: str, lat: float, lon: float, zoom_level: int):
    url = f"https://www.google.com/maps/search/{industry.replace(' ', '+')}/@{lat},{lon},{zoom_level}z?hl=en"
    print(f"🔍 Navigating to: {url}")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
            page = await context.new_page()

            # Route handler to block unwanted resources
            async def route_handler(route, request):
                if request.resource_type in BLOCKED_RESOURCE_TYPES:
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", route_handler)

            # Navigate and wait briefly for minimal DOM
            await page.goto(url, timeout=60000)
            await page.wait_for_timeout(5000)  # Minimal JS rendering time

            # Extract only <body> content
            body_html = await page.eval_on_selector("body", "el => el.outerHTML")

            # Generate safe filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_industry = industry.replace(" ", "_").lower()
            filename = f"{OUTPUT_HTML_DIR}/{safe_industry}_{timestamp}.html"

            # Save minimal HTML
            with open(filename, "w", encoding="utf-8") as f:
                f.write(body_html)

            print(f"💾 Saved minimal body HTML to: {filename}")

            await context.close()
            await browser.close()

    except Exception as e:
        print(f"🚨 Error saving HTML for '{industry}': {str(e)}")


async def main():
    print("📡 Fetching queries from API...")
    try:
        response = requests.get(API_URL, timeout=30)
        data = response.json()
        queries = data.get("queries", [])

        if not queries:
            print("❌ No queries returned from API.")
            return

        print(f"✅ Found {len(queries)} queries. Saving lightweight HTML pages...")

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

            task = save_minimal_html(industry, lat, lon, zoom_level)
            tasks.append(task)

        await asyncio.gather(*tasks)
        print("✅ All lightweight HTML pages saved successfully.")

    except Exception as e:
        print(f"❌ Failed to fetch queries: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
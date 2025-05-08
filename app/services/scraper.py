from .api_client import APIClient
from config import settings

async def process_industry(industry_data: dict):
    api_client = APIClient(settings.API_BASE_URL, settings.MACHINE_ID)
    results = await scrape_google_maps_page(industry_data)
    await api_client.submit_results(industry_data['industry'], results)
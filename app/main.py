from fastapi import FastAPI
from app.routers import data_router
from app.config import settings

app = FastAPI(title="Google Maps Scraper API")
app.include_router(data_router.router)

@app.on_event("startup")
async def startup_event():
    print("🚀 Starting scraper service...")
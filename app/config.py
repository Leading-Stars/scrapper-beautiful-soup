from pydantic import BaseSettings

class Settings(BaseSettings):
    api_endpoint: str = "http://82.112.254.77:8000/queries"
    api_key: str
    machine_id: int = 2
    
    class Config:
        env_file = ".env"

settings = Settings()
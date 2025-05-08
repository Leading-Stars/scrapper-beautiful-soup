from tenacity import retry, stop_after_attempt, wait_fixed
import requests

class APIClient:
    def __init__(self, base_url: str, machine_id: int):
        self.base_url = base_url
        self.machine_id = machine_id

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    async def submit_results(self, industry: str, results: list):
        try:
            response = requests.post(
                f"{self.base_url}/submissions",
                json={
                    "machine_id": self.machine_id,
                    "status": "completed",
                    "industry": industry,
                    "results": results,
                    "count": len(results)
                },
                timeout=15
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Submission failed: {str(e)}")
            return False
import aiohttp
import time
from .base import Exchange

class Aster(Exchange):
    def __init__(self):
        super().__init__("Aster", "https://fapi.asterdex.com")

    async def get_funding_rate(self, symbol: str) -> dict:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": symbol}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")
                data = await response.json()
                # If symbol is provided, data is a dict. If not, it's a list.
                # We assume symbol is provided.
                if isinstance(data, list):
                     # Handle case where list is returned (shouldn't happen with symbol param but good to be safe)
                     for item in data:
                         if item['symbol'] == symbol:
                             data = item
                             break
                
                interval_hours = None
                if data.get("nextFundingTime") and data.get("time"):
                    try:
                        diff = int(data["nextFundingTime"]) - int(data["time"])
                        if diff > 0:
                            interval_hours = diff / 3600_000
                    except Exception:
                        interval_hours = None

                return {
                    "exchange": self.name,
                    "symbol": symbol,
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
                    "nextFundingTime": int(data.get("nextFundingTime", 0)) if data.get("nextFundingTime") else None,
                    "interval_hours": interval_hours
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Aster API error: {response.status}")
                data = await response.json()
                
                results = []
                for item in data:
                    interval_hours = None
                    if item.get("nextFundingTime") and item.get("time"):
                        try:
                            diff = int(item["nextFundingTime"]) - int(item["time"])
                            if diff > 0:
                                interval_hours = diff / 3600_000
                        except Exception:
                            interval_hours = None

                    results.append({
                        "exchange": self.name,
                        "symbol": item["symbol"],
                        "rate": float(item["lastFundingRate"]),
                        "timestamp": int(item["time"]),
                        "nextFundingTime": int(item.get("nextFundingTime", 0)) if item.get("nextFundingTime") else None,
                        "interval_hours": interval_hours
                    })
                return results

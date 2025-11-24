import aiohttp
from .base import Exchange


class Binance(Exchange):
    def __init__(self):
        super().__init__("Binance", "https://fapi.binance.com")
        self.timeout = aiohttp.ClientTimeout(total=10)

    async def get_funding_rate(self, symbol: str) -> dict:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        params = {"symbol": symbol.upper()}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")
                data = await resp.json()
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
                    "symbol": symbol.upper(),
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
                    "nextFundingTime": int(data.get("nextFundingTime", 0)) if data.get("nextFundingTime") else None,
                    "interval_hours": interval_hours
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/fapi/v1/premiumIndex"
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Binance API error: {resp.status}")
                data = await resp.json()
                results = []
                for item in data:
                    results.append(
                        {
                            "exchange": self.name,
                            "symbol": item["symbol"],
                            "rate": float(item["lastFundingRate"]),
                            "timestamp": int(item["time"]),
                            "nextFundingTime": int(item.get("nextFundingTime", 0)) if item.get("nextFundingTime") else None,
                            "interval_hours": ( (int(item.get("nextFundingTime")) - int(item.get("time"))) / 3600_000 ) if item.get("nextFundingTime") and item.get("time") else None
                        }
                    )
                return results

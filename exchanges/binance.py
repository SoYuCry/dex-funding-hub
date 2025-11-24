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
                return {
                    "exchange": self.name,
                    "symbol": symbol.upper(),
                    "rate": float(data["lastFundingRate"]),
                    "timestamp": int(data["time"]),
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
                        }
                    )
                return results

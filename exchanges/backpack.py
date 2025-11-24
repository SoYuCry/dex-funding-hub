import aiohttp
import time
from .base import Exchange


class Backpack(Exchange):
    def __init__(self):
        super().__init__("Backpack", "https://api.backpack.exchange")
        self.timeout = aiohttp.ClientTimeout(total=10)

    async def _get_markets(self):
        url = f"{self.base_url}/api/v1/markets"
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url) as resp:
                if resp.status != 200:
                    raise Exception(f"Backpack API error: {resp.status}")
                return await resp.json()

    async def _fetch_latest_rate(self, symbol: str) -> dict | None:
        """
        Use fundingRates history endpoint to get last interval.
        """
        url = f"{self.base_url}/api/v1/fundingRates"
        params = {"symbol": symbol, "limit": 1}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if isinstance(data, list) and data:
                    return data[0]
                return None

    def _api_symbol(self, symbol: str) -> str:
        base = symbol.upper().replace("USDT", "").replace("USDC", "").replace("_", "").replace("-", "").replace("/", "")
        return f"{base}_USDC_PERP"

    async def get_funding_rate(self, symbol: str) -> dict:
        api_symbol = self._api_symbol(symbol)
        # Try history first
        latest = await self._fetch_latest_rate(api_symbol)
        ts_now = int(time.time() * 1000)

        if latest:
            base_symbol = api_symbol.split("_")[0]
            unified_symbol = f"{base_symbol}USDT"
            return {
                "exchange": self.name,
                "symbol": unified_symbol,
                "rate": float(latest.get("fundingRate", 0.0)),
                "timestamp": int(latest.get("intervalEndTimestamp") or ts_now),
            }

        # Fallback: pull markets info for fundingRate if present
        markets = await self._get_markets()
        for m in markets:
            if m.get("symbol", "").upper() == api_symbol:
                rate = m.get("fundingRate")
                if rate is None:
                    raise Exception(f"No funding rate available for {symbol}")
                base_symbol = m.get("baseSymbol", "").upper()
                unified_symbol = f"{base_symbol}USDT" if base_symbol else api_symbol.replace("_", "")
                return {
                    "exchange": self.name,
                    "symbol": unified_symbol,
                    "rate": float(rate),
                    "timestamp": ts_now,
                }
        raise Exception(f"Symbol {symbol} not found on Backpack")

    async def get_all_funding_rates(self) -> list[dict]:
        markets = await self._get_markets()
        results = []
        ts_now = int(time.time() * 1000)
        perps = [m for m in markets if m.get("marketType") == "PERP"]

        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            for m in perps:
                interval_ms = m.get("fundingInterval")
                symbol_api = m.get("symbol", "").upper()
                base = m.get("baseSymbol", "").upper()
                if not symbol_api or not base:
                    continue
                unified_symbol = f"{base}USDT"
                rate = None
                ts = ts_now

                url = f"{self.base_url}/api/v1/fundingRates"
                params = {"symbol": symbol_api, "limit": 1}
                try:
                    async with session.get(url, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if isinstance(data, list) and data:
                                rate = data[0].get("fundingRate")
                                ts = int(data[0].get("intervalEndTimestamp") or ts_now)
                except Exception:
                    pass

                if rate is None:
                    continue

                results.append(
                    {
                        "exchange": self.name,
                        "symbol": unified_symbol,
                        "rate": float(rate),
                        "timestamp": ts,
                        "fundingInterval": interval_ms,
                    }
                )
        return results

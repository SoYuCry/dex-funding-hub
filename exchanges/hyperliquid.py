import aiohttp
import time
from .base import Exchange


class Hyperliquid(Exchange):
    def __init__(self):
        super().__init__("Hyperliquid", "https://api.hyperliquid.xyz")
        self.timeout = aiohttp.ClientTimeout(total=10)

    def _symbol_to_coin(self, symbol: str) -> str:
        s = symbol.upper()
        if s.endswith("USDT"):
            return s[:-4]
        if s.endswith("USD"):
            return s[:-3]
        return s

    def _coin_to_symbol(self, coin: str) -> str:
        coin = coin.upper()
        if coin.endswith("USDT") or coin.endswith("USD"):
            return coin
        return coin + "USDT"

    async def _fetch_meta_and_ctx(self):
        url = f"{self.base_url}/info"
        payload = {"type": "metaAndAssetCtxs"}
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    raise Exception(f"Hyperliquid API error: {resp.status}")
                data = await resp.json()
                if not isinstance(data, list) or len(data) < 2:
                    raise Exception("Unexpected Hyperliquid response structure")
                meta, ctxs = data[0], data[1]
                universe = meta.get("universe") or []
                return universe, ctxs

    async def get_funding_rate(self, symbol: str) -> dict:
        universe, ctxs = await self._fetch_meta_and_ctx()
        coin = self._symbol_to_coin(symbol)
        for meta, ctx in zip(universe, ctxs):
            if meta.get("name", "").upper() == coin:
                rate = float(ctx.get("funding", 0.0))
                ts = int(time.time() * 1000)
                return {
                    "exchange": self.name,
                    "symbol": self._coin_to_symbol(coin),
                    "rate": rate,
                    "timestamp": ts,
                    "interval_hours": 1,
                }
        raise Exception(f"Symbol {symbol} not found on Hyperliquid")

    async def get_all_funding_rates(self) -> list[dict]:
        universe, ctxs = await self._fetch_meta_and_ctx()
        results = []
        now_ms = int(time.time() * 1000)
        for meta, ctx in zip(universe, ctxs):
            coin = meta.get("name", "").upper()
            if not coin:
                continue
            rate = float(ctx.get("funding", 0.0))
            results.append(
                {
                    "exchange": self.name,
                    "symbol": self._coin_to_symbol(coin),
                    "rate": rate,
                    "timestamp": now_ms,
                    "interval_hours": 1,
                }
            )
        return results

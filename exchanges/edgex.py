import aiohttp
import asyncio
import os
import json
import time
import websockets
import logging
from .base import Exchange

class EdgeX(Exchange):
    def __init__(self):
        super().__init__("EdgeX", "https://pro.edgex.exchange")
        self.contract_map = {} # Cache for symbol -> contractId
        self.contract_interval_map = {}
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.logger = logging.getLogger("EdgeX")
        # Optional manual cookies to bypass Cloudflare if needed (set EDGEX_COOKIES="key1=val; key2=val")
        raw_cookie = os.environ.get("EDGEX_COOKIES")
        self.cookies = {}
        if raw_cookie:
            try:
                for pair in raw_cookie.split(";"):
                    if "=" in pair:
                        k, v = pair.strip().split("=", 1)
                        self.cookies[k.strip()] = v.strip()
            except Exception:
                self.cookies = {}

    async def _get_contract_id(self, symbol: str, session: aiohttp.ClientSession) -> str:
        if symbol in self.contract_map:
            return self.contract_map[symbol]
        
        url = f"{self.base_url}/api/v1/public/meta/getMetaData"
        async with session.get(url) as response:
            if response.status != 200:
                raise Exception(f"EdgeX API error (contracts): {response.status}")
            data = await response.json()
            if data['code'] != 'SUCCESS':
                 raise Exception(f"EdgeX API error (contracts): {data}")

            for contract in data['data']['contractList']:
                # Skip hidden contracts
                if contract.get("enableDisplay") is False:
                    continue
                    
                if contract['contractName'] == symbol:
                    self.contract_map[symbol] = contract['contractId']
                    interval_min = contract.get("fundingRateIntervalMin")
                    if interval_min:
                        self.contract_interval_map[symbol] = float(interval_min) / 60
                    return contract['contractId']
                # Fallback for USDT -> USD mismatch
                if symbol.endswith("USDT") and contract['contractName'] == symbol.replace("USDT", "USD"):
                     self.contract_map[symbol] = contract['contractId']
                     interval_min = contract.get("fundingRateIntervalMin")
                     if interval_min:
                         self.contract_interval_map[symbol] = float(interval_min) / 60
                     return contract['contractId']
            
            raise Exception(f"Contract ID not found for symbol: {symbol}")

    async def _fetch_latest_funding(self, contract_id: str, session: aiohttp.ClientSession, retries: int = 5, backoff_base: float = 1, skip_on_block: bool = False) -> dict | None:
        """
        Call EdgeX funding/getLatestFundingRate with retry/backoff and simple Cloudflare detection.
        """
        url = f"{self.base_url}/api/v1/public/funding/getLatestFundingRate"
        params = {"contractId": contract_id}
        backoff = backoff_base

        for i in range(retries):
            async with session.get(url, params=params) as response:
                # Cloudflare or throttling
                if response.status in (403, 429):
                    if skip_on_block:
                        return None
                    if i < retries - 1:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    raise Exception(f"EdgeX funding blocked ({response.status}) after {retries} tries (set EDGEX_COOKIES if Cloudflare blocks)")

                if response.status != 200:
                    raise Exception(f"EdgeX API error: {response.status}")

                # If Cloudflare returns HTML, content-type will likely be text/html
                content_type = response.headers.get("Content-Type", "")
                if "text/html" in content_type:
                    _ = await response.text()
                    if skip_on_block:
                        return None
                    raise Exception("EdgeX funding returned HTML (likely Cloudflare)")

                data = await response.json(content_type=None)
                if data.get("code") != "SUCCESS":
                    raise Exception(f"EdgeX API error: {data}")
                if not data.get("data"):
                    raise Exception(f"No funding data returned for contract {contract_id}")

                return data["data"][0]

        raise Exception(f"EdgeX API error: exhausted retries for contract {contract_id}")

    async def get_funding_rate(self, symbol: str) -> dict:
        async with aiohttp.ClientSession(headers=self.headers, timeout=self.timeout, cookies=self.cookies) as session:
            contract_id = await self._get_contract_id(symbol, session)
            entry = await self._fetch_latest_funding(contract_id, session)

            timestamp = int(entry.get("fundingTimestamp") or entry.get("fundingTime"))
            interval_hours = self.contract_interval_map.get(symbol)
            return {
                "exchange": self.name,
                "symbol": symbol,
                "rate": float(entry["fundingRate"]),
                "timestamp": timestamp,
                "interval_hours": interval_hours
            }

    async def _fetch_all_funding_http(self) -> list[dict]:
        async with aiohttp.ClientSession(headers=self.headers, timeout=self.timeout, cookies=self.cookies) as session:
            # 1. Get all contracts
            url_meta = f"{self.base_url}/api/v1/public/meta/getMetaData"
            async with session.get(url_meta) as response:
                if response.status != 200:
                    raise Exception(f"EdgeX API error (contracts): {response.status}")
                data = await response.json()
                if data['code'] != 'SUCCESS':
                     raise Exception(f"EdgeX API error (contracts): {data}")
                
                contracts = data['data']['contractList']
            
            # 2. Fetch funding rates for all contracts (in parallel)
            results = []
            
            async def fetch_one(contract):
                try:
                    interval_hours = None
                    if contract.get("fundingRateIntervalMin"):
                        interval_hours = float(contract["fundingRateIntervalMin"]) / 60

                    entry = await self._fetch_latest_funding(
                        contract["contractId"],
                        session,
                        retries=2,
                        backoff_base=0.5,
                        skip_on_block=True
                    )
                    if entry is None:
                        return None
                    symbol = contract["contractName"]
                    # Normalize BTCUSD -> BTCUSDT if needed
                    if symbol.endswith("USD") and not symbol.endswith("USDT"):
                        symbol += "T"
                    return {
                        "exchange": self.name,
                        "symbol": symbol,
                        "rate": float(entry["fundingRate"]),
                        "timestamp": int(entry.get("fundingTimestamp") or entry.get("fundingTime")),
                        "interval_hours": interval_hours
                    }
                except Exception as e:
                    self.logger.warning("Error fetching EdgeX rate for %s: %s", contract["contractName"], e)
                    return None

            # Limit concurrency to avoid rate limits
            semaphore = asyncio.Semaphore(2) # keep low to reduce Cloudflare throttling while improving speed
            async def fetch_with_sem(contract):
                async with semaphore:
                    await asyncio.sleep(0.05) # Small delay between requests
                    return await fetch_one(contract)

            tasks = [fetch_with_sem(c) for c in contracts]
            fetched_results = await asyncio.gather(*tasks)
            
            return [r for r in fetched_results if r is not None]

    async def _fetch_all_funding_ws(self) -> list[dict]:
        # 1. Fetch metadata to filter hidden contracts
        valid_symbols = set()
        async with aiohttp.ClientSession(headers=self.headers, timeout=self.timeout, cookies=self.cookies) as session:
            url_meta = f"{self.base_url}/api/v1/public/meta/getMetaData"
            try:
                async with session.get(url_meta) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('code') == 'SUCCESS':
                            for contract in data['data']['contractList']:
                                if contract.get("enableDisplay") is False:
                                    continue
                                valid_symbols.add(contract['contractName'])
                                # Also add USDT variant if needed
                                if contract['contractName'].endswith("USD"):
                                    valid_symbols.add(contract['contractName'] + "T")
            except Exception as e:
                self.logger.warning(f"Failed to fetch metadata for filtering: {e}")
                # If metadata fetch fails, we might want to fall back to HTTP or proceed without filtering
                # For now, let's proceed but log warning. If valid_symbols is empty, we won't filter.
                pass

        uri = "wss://quote.edgex.exchange/api/v1/public/ws"
        sub_msg = {"type": "subscribe", "channel": "ticker.all.1s"}
        async with websockets.connect(
            uri,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            await ws.send(json.dumps(sub_msg))

            attempts = 0
            while attempts < 5:
                msg_raw = await asyncio.wait_for(ws.recv(), timeout=10)
                data = json.loads(msg_raw)

                if data.get("type") == "ping":
                    await ws.send(json.dumps({"type": "pong", "time": data.get("time")}))
                    continue

                if data.get("type") not in ("payload", "quote-event"):
                    continue

                items = data.get("content", {}).get("data", [])
                if not items:
                    attempts += 1
                    continue

                results = []
                for item in items:
                    name = item.get("contractName") or item.get("symbol") or item.get("contractId")
                    if not name:
                        continue
                    if str(name).startswith("TEMP"):
                        continue
                    
                    # Filter hidden contracts if we successfully fetched metadata
                    if valid_symbols and name not in valid_symbols:
                        # Try normalized name
                        symbol = name
                        if symbol.endswith("USD") and not symbol.endswith("USDT"):
                            symbol += "T"
                        if symbol not in valid_symbols:
                            continue

                    fr = item.get("fundingRate")
                    if fr is None:
                        continue
                    symbol = name
                    if symbol.endswith("USD") and not symbol.endswith("USDT"):
                        symbol += "T"
                    ts = item.get("fundingTime") or item.get("fundingTimestamp") or item.get("time")
                    results.append(
                        {
                            "exchange": self.name,
                            "symbol": symbol,
                            "rate": float(fr),
                            "timestamp": int(ts) if ts is not None else int(time.time() * 1000),
                        }
                    )

                if results:
                    return results

                attempts += 1

        raise Exception("EdgeX WS funding returned no data")

    async def get_all_funding_rates(self) -> list[dict]:
        try:
            return await self._fetch_all_funding_ws()
        except Exception as e_ws:
            self.logger.warning("EdgeX WS funding failed: %s, falling back to HTTP (may be rate limited)", e_ws)
            return await self._fetch_all_funding_http()

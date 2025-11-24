import aiohttp
import time
from .base import Exchange

class Lighter(Exchange):
    def __init__(self):
        super().__init__("Lighter", "https://mainnet.zklighter.elliot.ai")

    async def get_funding_rate(self, symbol: str) -> dict:
        url = f"{self.base_url}/api/v1/funding-rates"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Lighter API error: {response.status}")
                data = await response.json()
                
                # Data structure based on research: 
                # likely a list of objects or a dict with symbol keys.
                # Since exact structure isn't confirmed, we'll assume list of dicts based on common patterns
                # and the search result saying "Get funding rates" (plural).
                # We will iterate to find the symbol.
                
                # Note: Lighter symbols might be different (e.g. BTC-USDT). 
                # We'll try exact match first, then common variations.
                
                target_data = None
                
                # Helper to normalize symbol from API
                def norm(s):
                    if s and not s.endswith("USDT") and not s.endswith("USD"):
                        return s + "USDT"
                    return s

                def pick_target(items):
                    candidates = []
                    lighter_candidates = []
                    for item in items:
                        api_symbol = item.get('symbol')
                        if api_symbol == symbol or norm(api_symbol) == symbol:
                            candidates.append(item)
                            if item.get('exchange') == 'lighter':
                                lighter_candidates.append(item)
                    if lighter_candidates:
                        return lighter_candidates[0]
                    if candidates:
                        return candidates[0]
                    return None

                if isinstance(data, list):
                    target_data = pick_target(data)
                elif isinstance(data, dict) and 'funding_rates' in data:
                    target_data = pick_target(data['funding_rates'])

                if not target_data:
                     raise Exception(f"Symbol {symbol} not found in Lighter funding rates")

                raw_rate = float(target_data.get('rate', 0.0))
                # Lighter API appears to return 8h-style rate; normalize to 1h to match UI display.
                rate_per_hour = raw_rate / 8

                return {
                    "exchange": self.name,
                    "symbol": symbol,
                    "rate": rate_per_hour, # normalized to 1h
                    "timestamp": int(time.time() * 1000), # Use current time if API doesn't provide it
                    "interval_hours": 1,
                }

    async def get_all_funding_rates(self) -> list[dict]:
        url = f"{self.base_url}/api/v1/funding-rates"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Lighter API error: {response.status}")
                data = await response.json()
                
                rates = []
                # Handle {code: ..., funding_rates: [...]} structure
                if isinstance(data, dict) and 'funding_rates' in data:
                    raw_rates = data['funding_rates']
                elif isinstance(data, list):
                    raw_rates = data
                else:
                    # Fallback or empty if structure is unexpected
                    raw_rates = []

                # Prefer the exchange's own funding feed
                lighter_only = [item for item in raw_rates if item.get('exchange') == 'lighter']
                if lighter_only:
                    raw_rates = lighter_only

                for item in raw_rates:
                    # item example: {'market_id': 64, 'exchange': 'binance', 'symbol': 'ETHFI', 'rate': 9.032e-05}
                    symbol = item.get('symbol')
                    if symbol:
                        # Normalize symbol if needed (e.g. append USDT if missing)
                        # Based on observation, symbols might be just "ETHFI"
                        if not symbol.endswith("USDT") and not symbol.endswith("USD"):
                            symbol += "USDT"
                        
                        raw_rate = float(item.get('rate', 0.0))
                        rate_per_hour = raw_rate / 8  # normalize 8h -> 1h

                        rates.append({
                            "exchange": self.name,
                            "symbol": symbol,
                            "rate": rate_per_hour,
                            "timestamp": int(time.time() * 1000),
                            "interval_hours": 1,
                        })
                return rates

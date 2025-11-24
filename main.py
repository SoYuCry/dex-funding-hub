import asyncio
import logging
from exchanges.aster import Aster
from exchanges.edgex import EdgeX
from exchanges.lighter import Lighter
from exchanges.hyperliquid import Hyperliquid
from exchanges.binance import Binance
from utils import setup_logging, format_rate, format_timestamp

setup_logging()
logger = logging.getLogger("FundingMonitor")

async def main():
    exchanges = [
        Aster(),
        EdgeX(),
        Lighter(),
        Hyperliquid(),
        Binance()
    ]
    
    symbols = ["BTCUSDT", "ETHUSDT"] # Example symbols, can be made configurable

    while True:
        logger.info("Fetching funding rates...")
        tasks = []
        for exchange in exchanges:
            for symbol in symbols:
                tasks.append(exchange.get_funding_rate(symbol))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching data: {result}")
            else:
                logger.info(f"[{result['exchange']}] {result['symbol']}: Rate={format_rate(result['rate'])}, Time={format_timestamp(result['timestamp'])}")
        
        await asyncio.sleep(60) # Fetch every minute

if __name__ == "__main__":
    asyncio.run(main())

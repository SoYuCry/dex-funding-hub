from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class Exchange(ABC):
    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url

    @abstractmethod
    async def get_funding_rate(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch the current funding rate for a given symbol.
        Returns a dictionary with keys: 'symbol', 'rate', 'timestamp', 'exchange'.
        """
        pass

    @abstractmethod
    async def get_all_funding_rates(self) -> list[Dict[str, Any]]:
        """
        Fetch funding rates for all available symbols.
        Returns a list of dictionaries with keys: 'symbol', 'rate', 'timestamp', 'exchange'.
        """
        pass

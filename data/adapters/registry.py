from __future__ import annotations

from data.adapters.edgar import EdgarAdapter
from data.adapters.fred import FredAdapter
from data.adapters.yahoo import YahooFinanceAdapter


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters = {
            "fred": FredAdapter(),
            "yahoo_finance": YahooFinanceAdapter(),
            "sec_edgar": EdgarAdapter(),
        }

    def get(self, name: str):
        return self._adapters[name]

    def list(self) -> dict[str, object]:
        return dict(self._adapters)

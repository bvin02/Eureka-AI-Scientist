from data.adapters.base import DataSourceAdapter


class FredAdapter(DataSourceAdapter):
    name = "fred"

    def describe(self) -> str:
        return "FRED adapter for macroeconomic series discovery and retrieval."

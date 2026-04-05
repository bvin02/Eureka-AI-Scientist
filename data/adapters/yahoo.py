from data.adapters.base import DataSourceAdapter


class YahooFinanceAdapter(DataSourceAdapter):
    name = "yahoo_finance"

    def describe(self) -> str:
        return "Yahoo Finance adapter for price history and benchmark market data."

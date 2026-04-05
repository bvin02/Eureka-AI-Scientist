from data.adapters.base import DataSourceAdapter


class EdgarAdapter(DataSourceAdapter):
    name = "sec_edgar"

    def describe(self) -> str:
        return "SEC EDGAR adapter for filing discovery and evidence retrieval."

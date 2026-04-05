import asyncio
from datetime import date
from types import SimpleNamespace

import httpx

from data.adapters.edgar import EdgarAdapter
from data.adapters.fred import FredAdapter
from data.adapters.registry import AdapterRegistry
from data.adapters.yahoo import YahooFinanceAdapter
from data.models import DatasetDiscoveryNeed


def fred_transport(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith("/series/search"):
        return httpx.Response(
            200,
            json={
                "seriess": [
                    {
                        "id": "DFII10",
                        "title": "10Y Real Yield",
                        "notes": "Real yield series",
                    }
                ]
            },
        )
    if request.url.path.endswith("/series") and "release" not in request.url.path:
        return httpx.Response(
            200,
            json={
                "seriess": [
                    {
                        "id": "DFII10",
                        "title": "10Y Real Yield",
                        "notes": "Real yield series",
                        "frequency_short": "D",
                        "observation_start": "2020-01-01",
                        "observation_end": "2020-01-03",
                        "units": "Percent",
                        "seasonal_adjustment_short": "NSA",
                        "popularity": 88,
                    }
                ]
            },
        )
    if request.url.path.endswith("/series/release"):
        return httpx.Response(200, json={"releases": [{"name": "Treasury Real Yields"}]})
    if request.url.path.endswith("/series/observations"):
        return httpx.Response(
            200,
            json={
                "observations": [
                    {"date": "2020-01-01", "value": "1.2", "realtime_start": "2020-01-01", "realtime_end": "2020-01-31"},
                    {"date": "2020-01-02", "value": "1.1", "realtime_start": "2020-01-02", "realtime_end": "2020-01-31"},
                ]
            },
        )
    raise AssertionError(f"Unhandled FRED path: {request.url}")


def sec_transport(request: httpx.Request) -> httpx.Response:
    if request.url.path.endswith(".json"):
        return httpx.Response(
            200,
            json={
                "name": "NVIDIA CORP",
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000000000-24-000001"],
                        "form": ["10-K"],
                        "filingDate": ["2024-02-21"],
                        "primaryDocument": ["nvda10k.htm"],
                    }
                },
            },
        )
    if request.url.path.endswith("nvda10k.htm"):
        return httpx.Response(200, text="<html><body>Risk factors and results of operations.</body></html>")
    raise AssertionError(f"Unhandled SEC path: {request.url}")


class FakeTicker:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.fast_info = {
            "shortName": f"{symbol} Name",
            "quoteType": "ETF",
            "currency": "USD",
            "exchange": "NYSE",
            "timezone": "America/New_York",
        }

    def history(self, period="1y", start=None, end=None, auto_adjust=False):
        rows = [
            ("2024-01-02", {"Open": 10.0, "High": 11.0, "Low": 9.5, "Close": 10.5, "Adj Close": 10.4, "Volume": 1000}),
            ("2024-01-03", {"Open": 11.0, "High": 12.0, "Low": 10.5, "Close": 11.5, "Adj Close": 11.4, "Volume": 1200}),
        ]

        class FakeHistory:
            def iterrows(self_nonlocal):
                for date_str, row in rows:
                    yield SimpleNamespace(strftime=lambda fmt, s=date_str: s), row

        return FakeHistory()


def fake_ticker_factory(symbol: str) -> FakeTicker:
    return FakeTicker(symbol)


def test_fred_adapter_discovery_fetch_and_profile() -> None:
    adapter = FredAdapter(api_key="demo", transport=httpx.MockTransport(fred_transport))
    need = DatasetDiscoveryNeed(research_need="real yields", explanatory_variables=["inflation"])
    discovery = asyncio.run(adapter.discover(need))
    assert discovery.candidates[0].external_id == "DFII10"

    result = asyncio.run(adapter.fetch("DFII10"))
    assert result.dataset is not None
    assert result.dataset.metadata.external_id == "DFII10"
    assert len(result.dataset.observations) == 2
    assert result.profile is not None
    assert result.profile.source == "fred"
    assert result.profile.row_count == 2
    assert result.profile.datetime_columns["date"].startswith("calendar date")
    assert "date" in result.profile.likely_join_keys
    assert result.profile.frequency_inference == "daily"
    assert result.profile.potential_leakage_risks


def test_yahoo_adapter_fetches_return_ready_fields() -> None:
    adapter = YahooFinanceAdapter(ticker_factory=fake_ticker_factory)
    need = DatasetDiscoveryNeed(research_need="semis rotation", target_assets=["semis"])
    discovery = asyncio.run(adapter.discover(need))
    assert discovery.candidates[0].external_id == "SOXX"

    result = asyncio.run(adapter.fetch("SOXX"))
    assert result.dataset is not None
    assert result.dataset.observations[0].adjusted_close == 10.4
    assert result.profile is not None
    assert result.profile.key_candidates == ["date"]
    assert result.profile.inferred_semantic_types["adjusted_close"] == "measure"
    assert result.profile.sample_rows
    assert result.profile.potential_leakage_risks


def test_edgar_adapter_discovers_and_fetches_filing_metadata_and_text() -> None:
    adapter = EdgarAdapter(user_agent="Eureka/test", transport=httpx.MockTransport(sec_transport))
    need = DatasetDiscoveryNeed(research_need="issuer evidence", target_assets=["nvda"])
    discovery = asyncio.run(adapter.discover(need))
    assert discovery.candidates[0].external_id == "NVDA"

    result = asyncio.run(adapter.fetch("NVDA", cik="1045810", include_text=True))
    assert result.filing is not None
    assert result.filing.form == "10-K"
    assert result.filing.filing_date == date(2024, 2, 21)
    assert result.filing.text_blocks
    assert result.profile is not None
    assert result.profile.key_candidates == ["accession_number"]
    assert result.profile.datetime_columns["filing_date"].startswith("event date")
    assert "cik" in result.profile.likely_join_keys
    assert result.profile.frequency_inference == "event"


def test_adapter_registry_exposes_v1_sources() -> None:
    registry = AdapterRegistry()
    adapters = registry.list()
    assert set(adapters.keys()) == {"fred", "yahoo_finance", "sec_edgar"}

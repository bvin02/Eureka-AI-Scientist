from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None  # type: ignore[assignment]

from data.adapters.base import DataSourceAdapter
from data.adapters.common import infer_profile
from data.models import (
    AdapterCapability,
    AdapterFetchResult,
    CanonicalDataset,
    CanonicalDatasetMetadata,
    CanonicalObservation,
    DatasetDiscoveryCandidate,
    DatasetDiscoveryNeed,
    DatasetDiscoveryResult,
    DatasetProfile,
    ProvenancePayload,
)
from domain.models import ColumnProfile, TimeCoverage


class YahooFinanceAdapter(DataSourceAdapter):
    name = "yahoo_finance"

    def __init__(self, ticker_factory: Any | None = None) -> None:
        if ticker_factory is not None:
            self.ticker_factory = ticker_factory
        elif yf is not None:
            self.ticker_factory = yf.Ticker
        else:
            self.ticker_factory = None

    def capabilities(self) -> list[AdapterCapability]:
        return [
            AdapterCapability(name="symbol_discovery", description="Infer likely equity or ETF tickers from research need."),
            AdapterCapability(name="price_history", description="Fetch daily price history and adjusted close fields."),
            AdapterCapability(name="metadata", description="Expose symbol metadata and date coverage."),
            AdapterCapability(name="return_ready_fields", description="Provide adjusted-close and raw OHLCV fields."),
        ]

    def describe(self) -> str:
        return "Yahoo Finance adapter for lightweight equity and ETF history."

    async def discover(self, need: DatasetDiscoveryNeed) -> DatasetDiscoveryResult:
        query = f"{need.research_need} {' '.join(need.target_assets)}".lower()
        symbol_map = {
            "semis": ("SOXX", "iShares Semiconductor ETF"),
            "semiconductor": ("SOXX", "iShares Semiconductor ETF"),
            "growth": ("QQQ", "Invesco QQQ Trust"),
            "tech": ("XLK", "Technology Select Sector SPDR Fund"),
            "defensive": ("XLP", "Consumer Staples Select Sector SPDR Fund"),
            "small caps": ("IWM", "iShares Russell 2000 ETF"),
            "banks": ("KBE", "SPDR S&P Bank ETF"),
            "market": ("SPY", "SPDR S&P 500 ETF"),
        }
        candidates = []
        seen: set[str] = set()
        for term, (symbol, name) in symbol_map.items():
            if term in query and symbol not in seen:
                seen.add(symbol)
                candidates.append(
                    DatasetDiscoveryCandidate(
                        provider=self.name,
                        external_id=symbol,
                        name=name,
                        description=f"Lightweight market proxy for `{term}`.",
                        dataset_kind="market_series",
                        reason=f"Ticker `{symbol}` maps to the stated research need.",
                        confidence=0.8,
                        provenance=ProvenancePayload(
                            provider=self.name,
                            endpoint="symbol_mapping",
                            request_params={"term": term},
                            reference_url=f"https://finance.yahoo.com/quote/{symbol}",
                            fetched_at=datetime.now(timezone.utc),
                        ),
                    )
                )
        if not candidates:
            candidates.append(
                DatasetDiscoveryCandidate(
                    provider=self.name,
                    external_id="SPY",
                    name="SPDR S&P 500 ETF Trust",
                    description="Default broad-market benchmark.",
                    dataset_kind="market_series",
                    reason="Fallback benchmark when no direct symbol match is inferred.",
                    confidence=0.45,
                    provenance=ProvenancePayload(
                        provider=self.name,
                        endpoint="symbol_mapping",
                        request_params={"fallback": "SPY"},
                        reference_url="https://finance.yahoo.com/quote/SPY",
                        fetched_at=datetime.now(timezone.utc),
                    ),
                )
            )
        return DatasetDiscoveryResult(provider=self.name, candidates=candidates)

    async def metadata(self, external_id: str) -> CanonicalDatasetMetadata:
        if self.ticker_factory is None:
            raise RuntimeError("yfinance is not installed. Install dependencies before using YahooFinanceAdapter.")
        ticker = self.ticker_factory(external_id)
        info = getattr(ticker, "fast_info", None) or {}
        return CanonicalDatasetMetadata(
            provider=self.name,
            external_id=external_id,
            name=info.get("shortName", external_id),
            description=info.get("quoteType", "equity_or_etf"),
            dataset_kind="market_series",
            entity_grain="ticker",
            time_grain="date",
            frequency="1d",
            currency=info.get("currency"),
            coverage=TimeCoverage(expected_frequency="1d"),
            additional_metadata={
                "exchange": info.get("exchange"),
                "timezone": info.get("timezone"),
            },
            provenance=ProvenancePayload(
                provider=self.name,
                endpoint="yfinance.fast_info",
                request_params={"symbol": external_id},
                reference_url=f"https://finance.yahoo.com/quote/{external_id}",
                fetched_at=datetime.now(timezone.utc),
            ),
        )

    async def fetch(self, external_id: str, **kwargs) -> AdapterFetchResult:
        if self.ticker_factory is None:
            raise RuntimeError("yfinance is not installed. Install dependencies before using YahooFinanceAdapter.")
        ticker = self.ticker_factory(external_id)
        history = ticker.history(
            period=kwargs.get("period", "1y"),
            start=kwargs.get("start"),
            end=kwargs.get("end"),
            auto_adjust=False,
        )
        metadata = await self.metadata(external_id)
        observations = []
        for idx, row in history.iterrows():
            observations.append(
                CanonicalObservation(
                    date=idx.strftime("%Y-%m-%d"),
                    value=float(row["Close"]) if "Close" in row else None,
                    adjusted_close=float(row["Adj Close"]) if "Adj Close" in row else None,
                    close=float(row["Close"]) if "Close" in row else None,
                    open=float(row["Open"]) if "Open" in row else None,
                    high=float(row["High"]) if "High" in row else None,
                    low=float(row["Low"]) if "Low" in row else None,
                    volume=float(row["Volume"]) if "Volume" in row else None,
                )
            )
        dataset = CanonicalDataset(
            metadata=metadata,
            columns=[
                ColumnProfile(name="date", dtype="str", semantic_role="time_key", nullable=False),
                ColumnProfile(name="open", dtype="float"),
                ColumnProfile(name="high", dtype="float"),
                ColumnProfile(name="low", dtype="float"),
                ColumnProfile(name="close", dtype="float"),
                ColumnProfile(name="adjusted_close", dtype="float", semantic_role="return_ready_price"),
                ColumnProfile(name="volume", dtype="float"),
            ],
            observations=observations,
        )
        return AdapterFetchResult(dataset=dataset, profile=self.profile(AdapterFetchResult(dataset=dataset)))

    def profile(self, result: AdapterFetchResult) -> DatasetProfile:
        return infer_profile(result)

from __future__ import annotations

from datetime import datetime, timezone

import httpx

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
from infra.settings import get_settings


class FredAdapter(DataSourceAdapter):
    name = "fred"
    base_url = "https://api.stlouisfed.org/fred"

    def __init__(
        self,
        api_key: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        settings = get_settings()
        self.api_key = api_key or settings.fred_api_key
        self.transport = transport

    def capabilities(self) -> list[AdapterCapability]:
        return [
            AdapterCapability(name="series_discovery", description="Search FRED series by economic concept."),
            AdapterCapability(name="series_metadata", description="Retrieve series metadata and release information."),
            AdapterCapability(name="time_series_fetch", description="Fetch FRED observations for a series."),
            AdapterCapability(name="release_revision_metadata", description="Retrieve realtime/vintage windows when available."),
        ]

    def describe(self) -> str:
        return "FRED adapter for macroeconomic series discovery, metadata, and time series retrieval."

    async def discover(self, need: DatasetDiscoveryNeed) -> DatasetDiscoveryResult:
        query = " ".join([need.research_need, *need.explanatory_variables, *need.target_assets]).strip()
        params = {
            "api_key": self.api_key,
            "file_type": "json",
            "search_text": query,
            "limit": 5,
            "sort_order": "desc",
        }
        async with httpx.AsyncClient(transport=self.transport, timeout=20.0) as client:
            response = await client.get(f"{self.base_url}/series/search", params=params)
            response.raise_for_status()
            payload = response.json()

        candidates = []
        for item in payload.get("seriess", []):
            candidates.append(
                DatasetDiscoveryCandidate(
                    provider=self.name,
                    external_id=item["id"],
                    name=item["title"],
                    description=item.get("notes", "")[:280],
                    dataset_kind="macro_series",
                    reason=f"Relevant FRED series match for `{query}`.",
                    confidence=0.8,
                    provenance=ProvenancePayload(
                        provider=self.name,
                        endpoint="series/search",
                        request_params={"search_text": query},
                        reference_url=f"https://fred.stlouisfed.org/series/{item['id']}",
                    fetched_at=datetime.now(timezone.utc),
                    ),
                )
            )
        return DatasetDiscoveryResult(provider=self.name, candidates=candidates)

    async def metadata(self, external_id: str) -> CanonicalDatasetMetadata:
        params = {"api_key": self.api_key, "file_type": "json", "series_id": external_id}
        async with httpx.AsyncClient(transport=self.transport, timeout=20.0) as client:
            series_resp = await client.get(f"{self.base_url}/series", params=params)
            series_resp.raise_for_status()
            release_resp = await client.get(f"{self.base_url}/series/release", params=params)
            release_resp.raise_for_status()
        series = series_resp.json()["seriess"][0]
        release = release_resp.json().get("releases", [{}])[0]
        return CanonicalDatasetMetadata(
            provider=self.name,
            external_id=external_id,
            name=series["title"],
            description=series.get("notes", ""),
            dataset_kind="macro_series",
            entity_grain="market",
            time_grain="date",
            frequency=series.get("frequency_short"),
            coverage=TimeCoverage(
                start_date=series.get("observation_start"),
                end_date=series.get("observation_end"),
                expected_frequency=series.get("frequency_short"),
            ),
            additional_metadata={
                "units": series.get("units"),
                "seasonal_adjustment": series.get("seasonal_adjustment_short"),
                "release_name": release.get("name"),
                "popularity": series.get("popularity"),
            },
            provenance=ProvenancePayload(
                provider=self.name,
                endpoint="series",
                request_params={"series_id": external_id},
                reference_url=f"https://fred.stlouisfed.org/series/{external_id}",
                    fetched_at=datetime.now(timezone.utc),
            ),
        )

    async def fetch(self, external_id: str, **kwargs) -> AdapterFetchResult:
        start = kwargs.get("start")
        end = kwargs.get("end")
        metadata = await self.metadata(external_id)
        params = {
            "api_key": self.api_key,
            "file_type": "json",
            "series_id": external_id,
        }
        if start:
            params["observation_start"] = str(start)
        if end:
            params["observation_end"] = str(end)
        async with httpx.AsyncClient(transport=self.transport, timeout=20.0) as client:
            response = await client.get(f"{self.base_url}/series/observations", params=params)
            response.raise_for_status()
            payload = response.json()
        observations = []
        for row in payload.get("observations", []):
            value = None if row.get("value") in {None, "."} else float(row["value"])
            observations.append(
                CanonicalObservation(
                    date=row["date"],
                    value=value,
                    extra_fields={
                        "realtime_start": row.get("realtime_start"),
                        "realtime_end": row.get("realtime_end"),
                    },
                )
            )
        dataset = CanonicalDataset(
            metadata=metadata,
            columns=[
                ColumnProfile(name="date", dtype="str", semantic_role="time_key", nullable=False),
                ColumnProfile(name="value", dtype="float", semantic_role="measure"),
                ColumnProfile(name="realtime_start", dtype="str"),
                ColumnProfile(name="realtime_end", dtype="str"),
            ],
            observations=observations,
        )
        return AdapterFetchResult(dataset=dataset, profile=self.profile(AdapterFetchResult(dataset=dataset)))

    def profile(self, result: AdapterFetchResult) -> DatasetProfile:
        return infer_profile(result)

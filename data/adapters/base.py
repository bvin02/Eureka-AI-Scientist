from __future__ import annotations

from typing import Protocol

from data.models import (
    AdapterCapability,
    AdapterFetchResult,
    CanonicalDatasetMetadata,
    DatasetDiscoveryNeed,
    DatasetDiscoveryResult,
    DatasetProfile,
)


class DataSourceAdapter(Protocol):
    name: str

    def capabilities(self) -> list[AdapterCapability]:
        ...

    def describe(self) -> str:
        ...

    async def discover(self, need: DatasetDiscoveryNeed) -> DatasetDiscoveryResult:
        ...

    async def fetch(self, external_id: str, **kwargs) -> AdapterFetchResult:
        ...

    async def metadata(self, external_id: str) -> CanonicalDatasetMetadata:
        ...

    def profile(self, result: AdapterFetchResult) -> DatasetProfile:
        ...

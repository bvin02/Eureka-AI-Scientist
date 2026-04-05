from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import httpx

from data.adapters.base import DataSourceAdapter
from data.models import (
    AdapterCapability,
    AdapterFetchResult,
    CanonicalDatasetMetadata,
    DatasetDiscoveryCandidate,
    DatasetDiscoveryNeed,
    DatasetDiscoveryResult,
    DatasetProfile,
    EdgarFiling,
    FilingTextBlock,
    ProvenancePayload,
)
from domain.models import ColumnProfile, TimeCoverage
from infra.settings import get_settings


class EdgarAdapter(DataSourceAdapter):
    name = "sec_edgar"

    def __init__(
        self,
        user_agent: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        settings = get_settings()
        self.user_agent = user_agent or settings.sec_user_agent
        self.transport = transport

    def capabilities(self) -> list[AdapterCapability]:
        return [
            AdapterCapability(name="filing_discovery", description="Discover likely relevant company filings."),
            AdapterCapability(name="filing_metadata", description="Fetch form, accession, date, and company metadata."),
            AdapterCapability(name="filing_text_fetch", description="Fetch filing text or document snippets when feasible."),
            AdapterCapability(name="companyfacts_access", description="Use SEC companyfacts JSON where relevant structured fields exist."),
        ]

    def describe(self) -> str:
        return "SEC EDGAR adapter for filing discovery, metadata, and basic text retrieval."

    async def discover(self, need: DatasetDiscoveryNeed) -> DatasetDiscoveryResult:
        candidates = []
        for asset in need.target_assets[:5]:
            candidates.append(
                DatasetDiscoveryCandidate(
                    provider=self.name,
                    external_id=asset.upper(),
                    name=f"{asset.upper()} filings",
                    description="Likely company filing source inferred from target assets.",
                    dataset_kind="filing_index",
                    reason=f"Use SEC filings to add issuer-specific evidence for `{asset}`.",
                    confidence=0.6,
                    provenance=ProvenancePayload(
                        provider=self.name,
                        endpoint="target_asset_mapping",
                        request_params={"asset": asset},
                        reference_url="https://www.sec.gov/edgar/search/",
                        fetched_at=datetime.now(timezone.utc),
                    ),
                )
            )
        return DatasetDiscoveryResult(provider=self.name, candidates=candidates)

    async def metadata(self, external_id: str) -> CanonicalDatasetMetadata:
        return CanonicalDatasetMetadata(
            provider=self.name,
            external_id=external_id,
            name=f"SEC filings for {external_id}",
            description="EDGAR filing discovery stream.",
            dataset_kind="filing_index",
            entity_grain="issuer",
            time_grain="filing_date",
            frequency="event",
            coverage=TimeCoverage(expected_frequency="event"),
            additional_metadata={"symbol_or_cik": external_id},
            provenance=ProvenancePayload(
                provider=self.name,
                endpoint="submissions",
                request_params={"symbol_or_cik": external_id},
                reference_url="https://www.sec.gov/edgar/search/",
                fetched_at=datetime.now(timezone.utc),
            ),
        )

    async def fetch(self, external_id: str, **kwargs) -> AdapterFetchResult:
        cik = kwargs.get("cik", external_id)
        cik_digits = str(cik).zfill(10)
        headers = {"User-Agent": self.user_agent}
        async with httpx.AsyncClient(transport=self.transport, timeout=20.0, headers=headers) as client:
            response = await client.get(f"https://data.sec.gov/submissions/CIK{cik_digits}.json")
            response.raise_for_status()
            payload = response.json()

        recent = payload.get("filings", {}).get("recent", {})
        accession_numbers = recent.get("accessionNumber", [])
        forms = recent.get("form", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])
        filing_index = kwargs.get("filing_index", 0)
        filing = EdgarFiling(
            accession_number=accession_numbers[filing_index],
            form=forms[filing_index],
            filing_date=date.fromisoformat(filing_dates[filing_index]),
            primary_document=primary_docs[filing_index] if filing_index < len(primary_docs) else None,
            cik=cik_digits,
            company_name=payload.get("name", external_id),
            filing_url=self._filing_url(cik_digits, accession_numbers[filing_index], primary_docs[filing_index] if filing_index < len(primary_docs) else None),
            provenance=ProvenancePayload(
                provider=self.name,
                endpoint="submissions",
                request_params={"cik": cik_digits},
                reference_url=f"https://data.sec.gov/submissions/CIK{cik_digits}.json",
                fetched_at=datetime.now(timezone.utc),
            ),
        )

        if kwargs.get("include_text", False) and filing.filing_url:
            async with httpx.AsyncClient(transport=self.transport, timeout=20.0, headers=headers) as client:
                text_response = await client.get(filing.filing_url)
                text_response.raise_for_status()
                filing.text_blocks = [FilingTextBlock(section="document", content=text_response.text[:4000])]

        return AdapterFetchResult(filing=filing, profile=self.profile(AdapterFetchResult(filing=filing)))

    def profile(self, result: AdapterFetchResult) -> DatasetProfile:
        filing = result.filing
        if filing is None:
            return DatasetProfile(source=self.name, row_count=0, profile_warnings=["No filing payload to profile."])
        return DatasetProfile(
            source=self.name,
            row_count=1,
            fields=[
                ColumnProfile(name="accession_number", dtype="str", semantic_role="filing_id"),
                ColumnProfile(name="form", dtype="str"),
                ColumnProfile(name="filing_date", dtype="date", semantic_role="time_key"),
                ColumnProfile(name="primary_document", dtype="str"),
            ],
            inferred_semantic_types={
                "accession_number": "filing_identifier",
                "form": "attribute",
                "filing_date": "date",
                "primary_document": "attribute",
            },
            datetime_columns={"filing_date": "event date normalized to ISO-8601 filing date"},
            entity_identifier_columns=["accession_number", "cik"],
            time_coverage=TimeCoverage(start_date=filing.filing_date, end_date=filing.filing_date, expected_frequency="event"),
            frequency_inference="event",
            key_candidates=["accession_number"],
            likely_join_keys=["filing_date", "cik"],
            cardinality_by_column={"accession_number": {"unique_count": 1, "unique_fraction": 1.0}},
            sample_rows=[
                {
                    "accession_number": filing.accession_number,
                    "form": filing.form,
                    "filing_date": filing.filing_date.isoformat(),
                    "cik": filing.cik,
                }
            ],
            potential_leakage_risks=[
                "Filing text should be aligned to actual public availability before market-reaction analysis."
            ],
        )

    def _filing_url(self, cik: str, accession_number: str, primary_document: str | None) -> str | None:
        if primary_document is None:
            return None
        accession_no_dashes = accession_number.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dashes}/{primary_document}"

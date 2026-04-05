from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import Field

from domain.models import ColumnProfile, DomainModel, TimeCoverage


class AdapterCapability(DomainModel):
    name: str
    description: str


class ProvenancePayload(DomainModel):
    provider: str
    endpoint: str
    request_params: dict[str, Any] = Field(default_factory=dict)
    reference_url: str | None = None
    fetched_at: datetime | None = None


class CanonicalDatasetMetadata(DomainModel):
    provider: str
    external_id: str
    name: str
    description: str
    dataset_kind: str
    entity_grain: str | None = None
    time_grain: str | None = None
    frequency: str | None = None
    currency: str | None = None
    coverage: TimeCoverage = Field(default_factory=TimeCoverage)
    additional_metadata: dict[str, Any] = Field(default_factory=dict)
    provenance: ProvenancePayload


class CanonicalObservation(DomainModel):
    date: str
    value: float | int | str | None = None
    adjusted_close: float | None = None
    close: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | int | None = None
    extra_fields: dict[str, Any] = Field(default_factory=dict)


class CanonicalDataset(DomainModel):
    metadata: CanonicalDatasetMetadata
    columns: list[ColumnProfile] = Field(default_factory=list)
    observations: list[CanonicalObservation] = Field(default_factory=list)


class DatasetDiscoveryNeed(DomainModel):
    research_need: str
    target_assets: list[str] = Field(default_factory=list)
    explanatory_variables: list[str] = Field(default_factory=list)
    preferred_frequency: str | None = None
    date_start: date | None = None
    date_end: date | None = None


class DatasetDiscoveryCandidate(DomainModel):
    provider: str
    external_id: str
    name: str
    description: str
    dataset_kind: str
    reason: str
    confidence: float
    provenance: ProvenancePayload


class DatasetDiscoveryResult(DomainModel):
    provider: str
    candidates: list[DatasetDiscoveryCandidate] = Field(default_factory=list)


class DatasetProfile(DomainModel):
    source: str
    row_count: int
    fields: list[ColumnProfile] = Field(default_factory=list)
    inferred_semantic_types: dict[str, str] = Field(default_factory=dict)
    datetime_columns: dict[str, str] = Field(default_factory=dict)
    entity_identifier_columns: list[str] = Field(default_factory=list)
    time_coverage: TimeCoverage = Field(default_factory=TimeCoverage)
    frequency_inference: str | None = None
    key_candidates: list[str] = Field(default_factory=list)
    likely_join_keys: list[str] = Field(default_factory=list)
    missingness_by_column: dict[str, float] = Field(default_factory=dict)
    cardinality_by_column: dict[str, dict[str, float | int]] = Field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    potential_leakage_risks: list[str] = Field(default_factory=list)
    profile_warnings: list[str] = Field(default_factory=list)


class FilingTextBlock(DomainModel):
    section: str
    content: str


class EdgarFiling(DomainModel):
    accession_number: str
    form: str
    filing_date: date
    primary_document: str | None = None
    cik: str
    company_name: str
    filing_url: str | None = None
    text_blocks: list[FilingTextBlock] = Field(default_factory=list)
    provenance: ProvenancePayload


class AdapterFetchResult(DomainModel):
    dataset: CanonicalDataset | None = None
    filing: EdgarFiling | None = None
    profile: DatasetProfile | None = None

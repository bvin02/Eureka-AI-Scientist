from datetime import datetime

from data.models import (
    AdapterFetchResult,
    CanonicalDataset,
    CanonicalDatasetMetadata,
    CanonicalObservation,
    EdgarFiling,
    ProvenancePayload,
)
from data.profiling import profile_fetch_result
from domain.models import ColumnProfile, TimeCoverage


def test_dataset_profiling_produces_machine_and_ui_ready_fields() -> None:
    dataset = CanonicalDataset(
        metadata=CanonicalDatasetMetadata(
            provider="fred",
            external_id="CPILFESL",
            name="Core CPI",
            description="Core CPI",
            dataset_kind="macro_series",
            entity_grain="market",
            time_grain="date",
            frequency="monthly",
            coverage=TimeCoverage(expected_frequency="monthly"),
            provenance=ProvenancePayload(provider="fred", endpoint="series/observations"),
        ),
        columns=[
            ColumnProfile(name="date", dtype="str"),
            ColumnProfile(name="value", dtype="float"),
            ColumnProfile(name="realtime_start", dtype="str"),
        ],
        observations=[
            CanonicalObservation(date="2024-01-01", value=300.1, extra_fields={"realtime_start": "2024-02-15"}),
            CanonicalObservation(date="2024-02-01", value=300.8, extra_fields={"realtime_start": "2024-03-12"}),
            CanonicalObservation(date="2024-03-01", value=301.0, extra_fields={"realtime_start": "2024-04-10"}),
        ],
    )
    profile = profile_fetch_result(AdapterFetchResult(dataset=dataset))

    assert profile.source == "fred"
    assert profile.inferred_semantic_types["date"] == "date"
    assert profile.datetime_columns["date"].startswith("calendar date")
    assert profile.frequency_inference == "monthly"
    assert "date" in profile.likely_join_keys
    assert profile.sample_rows
    assert profile.potential_leakage_risks


def test_filing_profile_detects_event_date_and_identifier_keys() -> None:
    filing = EdgarFiling(
        accession_number="0000000000-24-000001",
        form="10-K",
        filing_date=datetime(2024, 2, 21).date(),
        primary_document="doc.htm",
        cik="0001045810",
        company_name="NVIDIA CORP",
        filing_url="https://www.sec.gov/example",
        provenance=ProvenancePayload(provider="sec_edgar", endpoint="submissions"),
    )
    profile = profile_fetch_result(AdapterFetchResult(filing=filing))

    assert profile.source == "sec_edgar"
    assert profile.datetime_columns["filing_date"].startswith("event date")
    assert "cik" in profile.entity_identifier_columns
    assert profile.frequency_inference == "event"

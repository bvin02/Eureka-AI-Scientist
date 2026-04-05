from __future__ import annotations

from collections import Counter
from datetime import date, datetime
from statistics import median
from typing import Any

from data.models import AdapterFetchResult, DatasetProfile
from domain.models import ColumnProfile, TimeCoverage


def profile_fetch_result(result: AdapterFetchResult) -> DatasetProfile:
    rows, source, expected_frequency = _rows_for_result(result)
    if not rows:
        return DatasetProfile(
            source=source,
            row_count=0,
            profile_warnings=["No rows available to profile."],
        )

    column_names = sorted({key for row in rows for key in row.keys()})
    semantic_types = {name: _infer_semantic_type(name, [row.get(name) for row in rows]) for name in column_names}
    datetime_columns = {
        name: _normalize_datetime_interpretation(name, [row.get(name) for row in rows])
        for name in column_names
        if semantic_types[name] in {"date", "datetime"}
    }
    entity_identifier_columns = [
        name
        for name in column_names
        if semantic_types[name] in {"entity_identifier", "ticker", "issuer_identifier", "filing_identifier"}
    ]

    fields = [
        ColumnProfile(
            name=name,
            dtype=_python_dtype([row.get(name) for row in rows]),
            semantic_role=semantic_types[name],
            nullable=any(row.get(name) in (None, "", []) for row in rows),
            distinct_count=len({repr(row.get(name)) for row in rows}),
            missing_fraction=_missing_fraction(name, rows),
            sample_values=[_safe_str(row.get(name)) for row in rows[:3]],
        )
        for name in column_names
    ]
    missingness = {name: _missing_fraction(name, rows) for name in column_names}
    cardinality = {
        name: {
            "unique_count": len({repr(row.get(name)) for row in rows}),
            "unique_fraction": len({repr(row.get(name)) for row in rows}) / len(rows),
        }
        for name in column_names
    }
    time_column = next(iter(datetime_columns.keys()), None)
    coverage = _time_coverage(time_column, rows, expected_frequency)
    frequency = _infer_frequency(time_column, rows, expected_frequency)
    join_keys = _likely_join_keys(time_column, entity_identifier_columns, semantic_types)
    leakage_risks = _potential_leakage_risks(source, column_names, semantic_types, expected_frequency)
    warnings = _quality_warnings(rows, missingness, datetime_columns, join_keys)

    return DatasetProfile(
        source=source,
        row_count=len(rows),
        fields=fields,
        inferred_semantic_types=semantic_types,
        datetime_columns=datetime_columns,
        entity_identifier_columns=entity_identifier_columns,
        time_coverage=coverage,
        frequency_inference=frequency,
        key_candidates=join_keys,
        likely_join_keys=join_keys,
        missingness_by_column=missingness,
        cardinality_by_column=cardinality,
        sample_rows=rows[:3],
        potential_leakage_risks=leakage_risks,
        profile_warnings=warnings,
    )


def _rows_for_result(result: AdapterFetchResult) -> tuple[list[dict[str, Any]], str, str | None]:
    if result.dataset is not None:
        dataset = result.dataset
        rows = []
        for observation in dataset.observations:
            row = observation.model_dump(mode="json", exclude_none=True)
            extra_fields = row.pop("extra_fields", {}) or {}
            row.update(extra_fields)
            rows.append(row)
        return rows, dataset.metadata.provider, dataset.metadata.frequency
    if result.filing is not None:
        filing = result.filing
        rows = [
            {
                "accession_number": filing.accession_number,
                "form": filing.form,
                "filing_date": filing.filing_date.isoformat(),
                "primary_document": filing.primary_document,
                "cik": filing.cik,
                "company_name": filing.company_name,
                "filing_url": filing.filing_url,
            }
        ]
        return rows, filing.provenance.provider, "event"
    return [], "unknown", None


def _infer_semantic_type(name: str, values: list[Any]) -> str:
    lower = name.lower()
    if lower in {"date", "filing_date", "datetime", "timestamp", "realtime_start", "realtime_end"}:
        return "date"
    if lower in {"symbol", "ticker"}:
        return "ticker"
    if lower in {"cik"}:
        return "issuer_identifier"
    if "accession" in lower:
        return "filing_identifier"
    if lower.endswith("_id") or lower == "id":
        return "entity_identifier"
    if lower in {"open", "high", "low", "close", "adjusted_close", "volume", "value"}:
        return "measure"
    if all(_is_number(value) for value in values if value not in (None, "")):
        return "measure"
    return "attribute"


def _normalize_datetime_interpretation(name: str, values: list[Any]) -> str:
    lower = name.lower()
    if lower == "filing_date":
        return "event date normalized to ISO-8601 filing date"
    if lower.startswith("realtime_"):
        return "FRED vintage window boundary normalized to ISO-8601 date"
    return "calendar date normalized to ISO-8601 date"


def _python_dtype(values: list[Any]) -> str:
    non_null = [value for value in values if value not in (None, "", [])]
    if not non_null:
        return "unknown"
    first = non_null[0]
    if _is_number(first):
        return "float" if isinstance(first, float) else "int"
    return type(first).__name__


def _missing_fraction(name: str, rows: list[dict[str, Any]]) -> float:
    total = len(rows)
    missing = sum(1 for row in rows if row.get(name) in (None, "", []))
    return missing / total if total else 0.0


def _time_coverage(time_column: str | None, rows: list[dict[str, Any]], expected_frequency: str | None) -> TimeCoverage:
    if time_column is None:
        return TimeCoverage(expected_frequency=expected_frequency)
    parsed = sorted(_parse_date(row.get(time_column)) for row in rows if _parse_date(row.get(time_column)) is not None)
    return TimeCoverage(
        start_date=parsed[0] if parsed else None,
        end_date=parsed[-1] if parsed else None,
        expected_frequency=expected_frequency,
    )


def _infer_frequency(time_column: str | None, rows: list[dict[str, Any]], expected_frequency: str | None) -> str | None:
    if expected_frequency in {"event", "1d", "D"}:
        return {"event": "event", "1d": "daily", "D": "daily"}[expected_frequency]
    if time_column is None:
        return expected_frequency
    dates = sorted(_parse_date(row.get(time_column)) for row in rows if _parse_date(row.get(time_column)) is not None)
    if len(dates) < 2:
        return expected_frequency or "unknown"
    deltas = [(dates[idx] - dates[idx - 1]).days for idx in range(1, len(dates))]
    typical = median(deltas)
    if typical <= 2:
        return "daily"
    if 25 <= typical <= 35:
        return "monthly"
    if 80 <= typical <= 100:
        return "quarterly"
    if 360 <= typical <= 370:
        return "annual"
    return expected_frequency or "irregular"


def _likely_join_keys(
    time_column: str | None,
    entity_identifier_columns: list[str],
    semantic_types: dict[str, str],
) -> list[str]:
    keys: list[str] = []
    if time_column is not None:
        keys.append(time_column)
    keys.extend(entity_identifier_columns)
    if not keys:
        for name, semantic_type in semantic_types.items():
            if semantic_type in {"ticker", "issuer_identifier"}:
                keys.append(name)
    return keys


def _potential_leakage_risks(
    source: str,
    column_names: list[str],
    semantic_types: dict[str, str],
    expected_frequency: str | None,
) -> list[str]:
    risks: list[str] = []
    lower_names = {name.lower() for name in column_names}
    if source == "fred" and {"realtime_start", "realtime_end"} & lower_names:
        risks.append("FRED realtime/vintage fields indicate revision windows; use vintage-aware joins to avoid revision leakage.")
    if source == "yahoo_finance" and "adjusted_close" in lower_names:
        risks.append("Adjusted close is return-ready but can be unsuitable for strict event-timing analyses because adjustments reflect later corporate actions.")
    if source == "sec_edgar":
        risks.append("Filing content should be timestamped to actual availability, not just filing date, before joining to market reactions.")
    if expected_frequency in {"monthly", "M"} and "date" in lower_names:
        risks.append("Lower-frequency macro data may require publication-lag alignment before joining to daily market data.")
    return risks


def _quality_warnings(
    rows: list[dict[str, Any]],
    missingness: dict[str, float],
    datetime_columns: dict[str, str],
    join_keys: list[str],
) -> list[str]:
    warnings: list[str] = []
    if len(rows) < 3:
        warnings.append("Very small sample detected; profile and frequency inference may be unstable.")
    heavy_missing = [name for name, fraction in missingness.items() if fraction > 0.25]
    if heavy_missing:
        warnings.append(f"High missingness detected in: {', '.join(sorted(heavy_missing))}.")
    if not datetime_columns:
        warnings.append("No obvious date/time column detected; time-based joins may require manual override.")
    if not join_keys:
        warnings.append("No likely join key detected automatically.")
    return warnings


def _safe_str(value: Any) -> str:
    return str(value)[:120]


def _parse_date(value: Any) -> date | None:
    if value in (None, "", []):
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)

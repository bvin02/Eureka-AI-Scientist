from __future__ import annotations

import re
from dataclasses import dataclass

from domain.enums import MergeJoinType, TimeAlignmentPolicy
from pydantic import Field

from domain.models import DomainModel
from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.prompts.registry import PromptRegistry
from orchestration.contracts import (
    DroppedColumnProposal,
    MergeDatasetSelectionProposal,
    MergeJoinEdgeProposal,
    MergeMappingProposal,
    MergePlanProposal,
    ResearchQuestionPlan,
)


class MergePlannerDatasetProfile(DomainModel):
    dataset_external_id: str
    dataset_name: str
    provider: str
    dataset_kind: str
    frequency: str | None = None
    columns: list[str] = Field(default_factory=list)
    key_candidates: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)


class MergePlannerInput(DomainModel):
    research_question: ResearchQuestionPlan
    hypotheses: list[str] = Field(default_factory=list)
    requested_tests: list[str] = Field(default_factory=list)
    dataset_profiles: list[MergePlannerDatasetProfile] = Field(default_factory=list)


@dataclass(frozen=True)
class _MappingScore:
    target: str
    semantic_role: str
    explanation: str
    score: float


class DeterministicMergePlanner:
    _SYNONYM_TARGETS = {
        "date": ("timestamp", "time_key"),
        "datetime": ("timestamp", "time_key"),
        "timestamp": ("timestamp", "time_key"),
        "filing_date": ("filing_timestamp", "time_key"),
        "symbol": ("ticker", "entity_key"),
        "ticker": ("ticker", "entity_key"),
        "cik": ("issuer_id", "entity_key"),
        "accession_number": ("filing_id", "entity_key"),
        "adjusted_close": ("price_adjusted", "measure"),
        "close": ("price_close", "measure"),
        "open": ("price_open", "measure"),
        "high": ("price_high", "measure"),
        "low": ("price_low", "measure"),
        "volume": ("volume", "measure"),
        "value": ("value", "measure"),
    }

    _DROP_HINTS = {
        "realtime_end": "FRED vintage end boundary is metadata and usually not a modeling feature.",
        "primary_document": "Raw filing document path is not directly useful for numerical merge features.",
    }

    def plan(self, planner_input: MergePlannerInput) -> MergePlanProposal:
        ranked = sorted(
            planner_input.dataset_profiles,
            key=lambda item: self._dataset_relevance_score(item, planner_input),
            reverse=True,
        )
        chosen = ranked[: max(2, min(len(ranked), 4))]

        chosen_payload = [
            MergeDatasetSelectionProposal(
                dataset_external_id=item.dataset_external_id,
                role=self._dataset_role(item),
                reason=self._dataset_reason(item, planner_input),
                confidence=round(self._dataset_relevance_score(item, planner_input), 2),
            )
            for item in chosen
        ]

        join_graph: list[MergeJoinEdgeProposal] = []
        mappings: list[MergeMappingProposal] = []
        dropped: list[DroppedColumnProposal] = []
        ambiguities: list[str] = []
        warnings: list[str] = []

        base = self._choose_base_dataset(chosen)
        for other in [item for item in chosen if item.dataset_external_id != base.dataset_external_id]:
            join_keys, left_time, right_time = self._infer_join_keys(base, other)
            edge_confidence = 0.55 + 0.1 * int(bool(join_keys)) + 0.1 * int(bool(left_time and right_time))
            join_graph.append(
                MergeJoinEdgeProposal(
                    left_dataset_external_id=base.dataset_external_id,
                    right_dataset_external_id=other.dataset_external_id,
                    join_type=self._infer_join_type(base, other),
                    join_keys=join_keys,
                    left_time_column=left_time,
                    right_time_column=right_time,
                    confidence=round(min(edge_confidence, 0.93), 2),
                    rationale=self._join_rationale(base, other, join_keys, left_time, right_time),
                )
            )
            if not join_keys:
                ambiguities.append(
                    f"No deterministic entity key overlap between {base.dataset_external_id} and {other.dataset_external_id}; relying on temporal alignment."
                )

        for dataset in chosen:
            for column in dataset.columns:
                mapped = self._map_column(dataset, column)
                include = mapped.semantic_role != "metadata"
                drop_reason = None if include else self._drop_reason(column)
                if not include:
                    dropped.append(
                        DroppedColumnProposal(
                            dataset_external_id=dataset.dataset_external_id,
                            column=column,
                            reason=drop_reason or "Column is metadata noise for quantitative merge output.",
                            confidence=round(mapped.score, 2),
                        )
                    )

                mappings.append(
                    MergeMappingProposal(
                        source_dataset_external_id=dataset.dataset_external_id,
                        source_column=column,
                        target_column=mapped.target,
                        semantic_role=mapped.semantic_role,
                        match_explanation=mapped.explanation,
                        date_normalization_rule=self._date_norm_rule(column),
                        frequency_rule=self._frequency_rule(dataset.frequency),
                        lag_rule=self._lag_rule(dataset),
                        include_in_output=include,
                        drop_reason=drop_reason,
                        leakage_risk=self._column_leakage_risk(dataset, column),
                        ambiguity_note=self._column_ambiguity_note(dataset, column),
                        confidence=round(mapped.score, 2),
                        notes=f"Mapped from {dataset.provider}:{dataset.dataset_external_id}",
                    )
                )

        mixed_frequency = sorted({(item.frequency or "unknown").lower() for item in chosen})
        if len(mixed_frequency) > 1:
            warnings.append(
                "Frequency mismatch detected; convert higher-frequency market series to the planning grain before joining to macro/filing inputs."
            )

        if any(item.provider.lower() == "fred" for item in chosen):
            warnings.append("Macro publication timing may introduce lookahead bias without lag-aware alignment.")
        if any(item.provider.lower() == "sec_edgar" for item in chosen):
            warnings.append("Filing fields should align to first public availability timestamp, not fiscal period labels.")

        date_alignment_strategy = self._date_alignment_strategy(chosen)
        frequency_conversion_strategy = self._frequency_conversion_strategy(chosen)
        lag_policy = self._lag_policy(chosen)

        mapped_confidences = [item.confidence for item in mappings if item.include_in_output]
        edge_confidences = [edge.confidence for edge in join_graph]
        base_confidence = (sum(mapped_confidences) + sum(edge_confidences)) / max(1, len(mapped_confidences) + len(edge_confidences))
        ambiguity_penalty = min(0.2, 0.03 * len(ambiguities) + 0.02 * len(warnings))
        confidence = max(0.35, min(0.96, base_confidence - ambiguity_penalty))

        return MergePlanProposal(
            output_name="canonical_research_dataset",
            chosen_datasets=chosen_payload,
            join_graph=join_graph,
            join_type=join_graph[0].join_type,
            time_alignment_policy=TimeAlignmentPolicy.PUBLICATION_LAG if "publication" in lag_policy.lower() else TimeAlignmentPolicy.ASOF_PREVIOUS,
            date_alignment_strategy=date_alignment_strategy,
            frequency_conversion_strategy=frequency_conversion_strategy,
            lag_policy=lag_policy,
            lag_assumption=lag_policy,
            mappings=mappings,
            dropped_columns=dropped,
            unresolved_ambiguities=sorted(set(ambiguities)),
            warnings=sorted(set(warnings)),
            ambiguity_notes=sorted(set(ambiguities)),
            validation_checks=[
                "join_key_coverage",
                "timestamp_monotonicity",
                "no_lookahead",
                "post_merge_null_rate",
            ],
            confidence=round(confidence, 2),
        )

    def _dataset_relevance_score(self, dataset: MergePlannerDatasetProfile, planner_input: MergePlannerInput) -> float:
        question_tokens = self._tokens(planner_input.research_question.canonical_question)
        hypothesis_tokens = self._tokens(" ".join(planner_input.hypotheses))
        test_tokens = self._tokens(" ".join(planner_input.requested_tests))
        dataset_tokens = self._tokens(" ".join([dataset.dataset_external_id, dataset.dataset_name, dataset.dataset_kind] + dataset.columns))

        overlap = len((question_tokens | hypothesis_tokens | test_tokens) & dataset_tokens)
        base = 0.45 + min(0.35, overlap * 0.03)

        if dataset.provider.lower() == "fred":
            base += 0.08
        if dataset.provider.lower() == "yahoo_finance":
            base += 0.06
        if dataset.provider.lower() == "sec_edgar":
            base += 0.04
        if dataset.frequency and dataset.frequency.lower() in {"daily", "monthly", "quarterly", "event"}:
            base += 0.04
        if dataset.key_candidates:
            base += 0.03
        if dataset.quality_flags:
            base -= min(0.08, len(dataset.quality_flags) * 0.01)
        return max(0.35, min(0.95, base))

    def _dataset_role(self, dataset: MergePlannerDatasetProfile) -> str:
        kind = dataset.dataset_kind.lower()
        if "market" in kind:
            return "target_market_panel"
        if "macro" in kind:
            return "macro_explanatory"
        if "filing" in kind or dataset.provider.lower() == "sec_edgar":
            return "event_or_fundamental_context"
        return "auxiliary"

    def _dataset_reason(self, dataset: MergePlannerDatasetProfile, planner_input: MergePlannerInput) -> str:
        role = self._dataset_role(dataset)
        return (
            f"Selected as {role} based on overlap with research question terms and available joinable keys "
            f"({', '.join(dataset.key_candidates or ['time proxy'])})."
        )

    def _choose_base_dataset(self, datasets: list[MergePlannerDatasetProfile]) -> MergePlannerDatasetProfile:
        def score(item: MergePlannerDatasetProfile) -> tuple[int, int, int]:
            market_bias = 1 if "market" in item.dataset_kind.lower() else 0
            has_time = 1 if any("date" in col.lower() or "time" in col.lower() for col in item.columns) else 0
            return (market_bias, has_time, len(item.columns))

        return sorted(datasets, key=score, reverse=True)[0]

    def _infer_join_keys(
        self,
        left: MergePlannerDatasetProfile,
        right: MergePlannerDatasetProfile,
    ) -> tuple[list[str], str | None, str | None]:
        left_cols = {col.lower(): col for col in left.columns}
        right_cols = {col.lower(): col for col in right.columns}

        entity_candidates = ["ticker", "symbol", "cik", "accession_number", "issuer_id", "id"]
        keys: list[str] = []
        for key in entity_candidates:
            if key in left_cols and key in right_cols:
                keys.append(left_cols[key])

        left_time = self._first_time_column(left.columns)
        right_time = self._first_time_column(right.columns)
        if left_time and right_time:
            keys.append(left_time)

        return sorted(set(keys)), left_time, right_time

    def _first_time_column(self, columns: list[str]) -> str | None:
        for col in columns:
            if re.search(r"(date|time|timestamp)$", col.lower()) or col.lower() in {
                "date",
                "filing_date",
                "realtime_start",
                "realtime_end",
            }:
                return col
        return None

    def _infer_join_type(self, left: MergePlannerDatasetProfile, right: MergePlannerDatasetProfile) -> MergeJoinType:
        if "event" in (left.frequency or "").lower() or "event" in (right.frequency or "").lower():
            return MergeJoinType.ASOF
        if {"daily", "monthly", "quarterly"} & {(left.frequency or "").lower(), (right.frequency or "").lower()}:
            return MergeJoinType.LEFT
        return MergeJoinType.ASOF

    def _join_rationale(
        self,
        left: MergePlannerDatasetProfile,
        right: MergePlannerDatasetProfile,
        join_keys: list[str],
        left_time: str | None,
        right_time: str | None,
    ) -> str:
        if join_keys:
            return (
                f"Join {right.dataset_external_id} into {left.dataset_external_id} using keys "
                f"{', '.join(join_keys)} with time alignment {left_time or 'n/a'}~{right_time or 'n/a'}."
            )
        return (
            f"No explicit shared entity keys found; use as-of temporal join between "
            f"{left.dataset_external_id} and {right.dataset_external_id}."
        )

    def _map_column(self, dataset: MergePlannerDatasetProfile, column: str) -> _MappingScore:
        lower = column.lower()
        if lower in self._DROP_HINTS:
            return _MappingScore(
                target=f"drop_{lower}",
                semantic_role="metadata",
                explanation="Column recognized as auxiliary metadata not intended for modeling features.",
                score=0.88,
            )
        if lower in self._SYNONYM_TARGETS:
            target, role = self._SYNONYM_TARGETS[lower]
            return _MappingScore(
                target=target,
                semantic_role=role,
                explanation=f"Mapped `{column}` via semantic synonym dictionary for quant datasets.",
                score=0.9,
            )
        if any(token in lower for token in ["return", "ret", "pct_change", "change"]):
            return _MappingScore(
                target=f"{dataset.dataset_external_id.lower()}_{lower}",
                semantic_role="measure",
                explanation="Column name indicates return/change-like numeric driver.",
                score=0.82,
            )
        if any(token in lower for token in ["real", "yield", "inflation", "cpi", "unrate", "payems"]):
            return _MappingScore(
                target=f"macro_{lower}",
                semantic_role="measure",
                explanation="Column appears macro-economic; mapped as explanatory measure.",
                score=0.8,
            )
        token_strength = min(0.2, len(self._tokens(lower)) * 0.03)
        confidence = 0.55 + token_strength
        return _MappingScore(
            target=f"{dataset.dataset_external_id.lower()}_{lower}",
            semantic_role="attribute",
            explanation="Fallback semantic mapping generated from dataset namespace and column tokenization.",
            score=confidence,
        )

    def _date_norm_rule(self, column: str) -> str | None:
        lower = column.lower()
        if "date" in lower or "time" in lower:
            return "Normalize to UTC calendar date (YYYY-MM-DD) and persist timezone-naive analysis timestamp."
        return None

    def _frequency_rule(self, frequency: str | None) -> str | None:
        if not frequency:
            return "Infer frequency from deltas and align to canonical analysis grain."
        lower = frequency.lower()
        if lower == "daily":
            return "Retain daily values and aggregate to monthly period-end when merged with lower-frequency macro signals."
        if lower == "monthly":
            return "Forward-fill monthly releases within month after publication lag adjustment."
        if lower == "event":
            return "Treat as event-frequency and join as-of to nearest previous timestamp."
        return f"Respect source frequency `{frequency}` then align to canonical grain."

    def _lag_rule(self, dataset: MergePlannerDatasetProfile) -> str:
        if dataset.provider.lower() == "fred":
            return "Use publication-lag alignment: macro observations become available on first tradable date after release."
        if dataset.provider.lower() == "sec_edgar":
            return "Use filing-availability lag: align records to first timestamp filing is publicly accessible."
        return "No additional lag beyond source timestamp unless downstream leakage checks fail."

    def _drop_reason(self, column: str) -> str | None:
        return self._DROP_HINTS.get(column.lower())

    def _column_leakage_risk(self, dataset: MergePlannerDatasetProfile, column: str) -> str | None:
        lower = column.lower()
        if lower in {"realtime_start", "realtime_end"}:
            return "Vintage/revision window field can leak revised data if not constrained to real-time availability."
        if dataset.provider.lower() == "yahoo_finance" and lower == "adjusted_close":
            return "Adjusted close includes corporate action adjustments that may be realized after the event timestamp."
        if dataset.provider.lower() == "sec_edgar" and lower in {"filing_date", "accession_number"}:
            return "Filing date alone may precede intraday availability; align to acceptance timestamp when possible."
        return None

    def _column_ambiguity_note(self, dataset: MergePlannerDatasetProfile, column: str) -> str | None:
        lower = column.lower()
        if lower in {"value", "close"}:
            return f"`{column}` may represent level or transform-ready signal; confirm downstream feature engineering intent."
        if lower.endswith("_id"):
            return f"`{column}` may be technical identifier rather than economic entity key."
        return None

    def _date_alignment_strategy(self, chosen: list[MergePlannerDatasetProfile]) -> str:
        providers = {item.provider.lower() for item in chosen}
        if "fred" in providers:
            return "Normalize all date columns to ISO-8601; align macro observations using publication-aware as-of join to market timeline."
        if "sec_edgar" in providers:
            return "Normalize filing and market timestamps to UTC date and align filing events to previous tradable bar."
        return "Normalize all date columns to ISO-8601 and align on shared calendar keys."

    def _frequency_conversion_strategy(self, chosen: list[MergePlannerDatasetProfile]) -> str:
        frequencies = {(item.frequency or "unknown").lower() for item in chosen}
        if "daily" in frequencies and "monthly" in frequencies:
            return "Convert daily market features to monthly period-end aggregates and align to lag-adjusted monthly macro releases."
        if "event" in frequencies and len(frequencies) > 1:
            return "Keep event streams sparse and join as-of against the nearest previous panel timestamp."
        return "Retain native frequency when consistent; otherwise convert to the most common analysis grain."

    def _lag_policy(self, chosen: list[MergePlannerDatasetProfile]) -> str:
        providers = {item.provider.lower() for item in chosen}
        if "fred" in providers and "yahoo_finance" in providers:
            return "Apply publication lag to macro releases before merging with market returns to avoid lookahead bias."
        if "sec_edgar" in providers:
            return "Apply filing availability lag and prohibit joins using information before public timestamp."
        return "Apply conservative as-of previous timestamp lag for all non-synchronous sources."

    def _tokens(self, text: str) -> set[str]:
        return {part for part in re.split(r"[^a-z0-9]+", text.lower()) if len(part) > 2}


class MergePlanner:
    def __init__(
        self,
        gateway: OpenAIResponsesGateway | None = None,
        prompt_registry: PromptRegistry | None = None,
        fallback: DeterministicMergePlanner | None = None,
    ) -> None:
        self.gateway = gateway or OpenAIResponsesGateway()
        self.prompt_registry = prompt_registry or PromptRegistry()
        self.fallback = fallback or DeterministicMergePlanner()

    async def plan(self, planner_input: MergePlannerInput) -> MergePlanProposal:
        if self.gateway.client is None:
            return self.fallback.plan(planner_input)

        request = PromptRequest(
            prompt_name="merge_planner",
            system_prompt=self.prompt_registry.get_prompt_text("merge_planner"),
            user_prompt=planner_input.model_dump_json(indent=2),
        )
        try:
            return await self.gateway.generate_structured(request=request, schema=MergePlanProposal)
        except RuntimeError:
            return self.fallback.plan(planner_input)

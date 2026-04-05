from __future__ import annotations

from pydantic import Field, model_validator

from domain.enums import AnalysisType, EvidenceStance, MergeJoinType, TimeAlignmentPolicy
from domain.models import CaveatRecord, ColumnProfile, DomainModel, TransformSpec


class ResearchQuestionPlan(DomainModel):
    canonical_question: str
    market_universe: list[str] = Field(default_factory=list)
    benchmark: str | None = None
    horizon: str
    frequency: str | None = None
    unit_of_analysis: str
    success_criteria: list[str] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)


class HypothesisProposal(DomainModel):
    label: str
    title: str
    thesis: str
    mechanism: str
    required_variables: list[str] = Field(default_factory=list)
    preferred_proxies: list[str] = Field(default_factory=list)
    recommended_test_type: AnalysisType | None = None
    expected_direction: str | None = None
    target_assets: list[str] = Field(default_factory=list)
    explanatory_variables: list[str] = Field(default_factory=list)
    likely_caveats: list[str] = Field(default_factory=list)
    confidence_level: float
    novelty_usefulness_note: str


class HypothesisRewriteProposal(DomainModel):
    title: str
    thesis: str
    mechanism: str
    required_variables: list[str] = Field(default_factory=list)
    preferred_proxies: list[str] = Field(default_factory=list)
    recommended_test_type: AnalysisType | None = None
    expected_direction: str | None = None
    explanatory_variables: list[str] = Field(default_factory=list)
    likely_caveats: list[str] = Field(default_factory=list)
    confidence_level: float
    novelty_usefulness_note: str


class HypothesisProposalSet(DomainModel):
    hypotheses: list[HypothesisProposal] = Field(default_factory=list)


class EvidenceItemProposal(DomainModel):
    provider: str
    title: str
    source: str
    date: str | None = None
    short_claim_summary: str
    methodology_summary: str | None = None
    data_used: list[str] = Field(default_factory=list)
    relevance_to_hypothesis: str
    evidence_stance: EvidenceStance = EvidenceStance.ADJACENT
    citation: str | None = None
    extracted_claims: list[str] = Field(default_factory=list)


class EvidenceSummarySet(DomainModel):
    evidence_items: list[EvidenceItemProposal] = Field(default_factory=list)


class DatasetCandidate(DomainModel):
    provider: str
    external_id: str
    name: str
    description: str
    dataset_kind: str
    entity_grain: str | None = None
    time_grain: str | None = None
    frequency: str | None = None


class DatasetDiscoverySet(DomainModel):
    datasets: list[DatasetCandidate] = Field(default_factory=list)


class DatasetProfileProposal(DomainModel):
    dataset_external_id: str
    row_count: int | None = None
    columns: list[ColumnProfile] = Field(default_factory=list)
    key_candidates: list[str] = Field(default_factory=list)
    quality_flags: list[str] = Field(default_factory=list)


class DatasetProfileSet(DomainModel):
    profiles: list[DatasetProfileProposal] = Field(default_factory=list)


class MergeMappingProposal(DomainModel):
    source_dataset_external_id: str
    source_column: str
    target_column: str
    semantic_role: str
    match_explanation: str
    date_normalization_rule: str | None = None
    frequency_rule: str | None = None
    lag_rule: str | None = None
    include_in_output: bool = True
    drop_reason: str | None = None
    leakage_risk: str | None = None
    ambiguity_note: str | None = None
    transforms: list[TransformSpec] = Field(default_factory=list)
    confidence: float
    notes: str | None = None


class MergeJoinEdgeProposal(DomainModel):
    left_dataset_external_id: str
    right_dataset_external_id: str
    join_type: MergeJoinType
    join_keys: list[str] = Field(default_factory=list)
    left_time_column: str | None = None
    right_time_column: str | None = None
    confidence: float
    rationale: str


class MergeDatasetSelectionProposal(DomainModel):
    dataset_external_id: str
    role: str
    reason: str
    confidence: float


class DroppedColumnProposal(DomainModel):
    dataset_external_id: str
    column: str
    reason: str
    confidence: float


class MergePlanProposal(DomainModel):
    output_name: str
    chosen_datasets: list[MergeDatasetSelectionProposal] = Field(default_factory=list)
    join_graph: list[MergeJoinEdgeProposal] = Field(default_factory=list)
    join_type: MergeJoinType
    time_alignment_policy: TimeAlignmentPolicy
    date_alignment_strategy: str
    frequency_conversion_strategy: str
    lag_policy: str
    lag_assumption: str
    mappings: list[MergeMappingProposal] = Field(default_factory=list)
    dropped_columns: list[DroppedColumnProposal] = Field(default_factory=list)
    unresolved_ambiguities: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    ambiguity_notes: list[str] = Field(default_factory=list)
    validation_checks: list[str] = Field(default_factory=list)
    confidence: float

    @model_validator(mode="after")
    def validate_plan_has_join_graph(self) -> "MergePlanProposal":
        if len(self.chosen_datasets) < 2:
            raise ValueError("Merge plan must choose at least two datasets.")
        if not self.join_graph:
            raise ValueError("Merge plan must include at least one join edge.")
        return self


class AnalysisSpecProposal(DomainModel):
    analysis_type: AnalysisType
    title: str
    objective: str
    dependent_variable: str | None = None
    independent_variables: list[str] = Field(default_factory=list)
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)


class TestPlanProposal(DomainModel):
    title: str
    objective: str
    analyses: list[AnalysisSpecProposal] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)


class ResultSummaryProposal(DomainModel):
    summary: str
    key_findings: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class NextStepProposal(DomainModel):
    summary: str
    next_steps: list[str] = Field(default_factory=list)

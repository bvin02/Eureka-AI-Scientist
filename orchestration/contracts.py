from __future__ import annotations

from pydantic import Field

from domain.enums import AnalysisType, MergeJoinType, TimeAlignmentPolicy
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
    summary: str
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
    left_column: str
    right_column: str
    semantic_role: str
    transforms: list[TransformSpec] = Field(default_factory=list)
    confidence: float
    notes: str | None = None


class MergePlanProposal(DomainModel):
    output_name: str
    join_type: MergeJoinType
    time_alignment_policy: TimeAlignmentPolicy
    lag_assumption: str
    mappings: list[MergeMappingProposal] = Field(default_factory=list)
    ambiguity_notes: list[str] = Field(default_factory=list)
    validation_checks: list[str] = Field(default_factory=list)
    confidence: float


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

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from domain.enums import (
    AnalysisType,
    AnalysisDatasetKind,
    ApprovalStatus,
    ArtifactKind,
    BranchStatus,
    EntityKind,
    HypothesisStatus,
    InvestigationStatus,
    MergeJoinType,
    NotebookEntryKind,
    ProvenanceSourceType,
    ResultArtifactType,
    StageRunStatus,
    TimeAlignmentPolicy,
    UserDecisionType,
    WarningSeverity,
    WorkflowStage,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", use_enum_values=False, protected_namespaces=())
    schema_version: int = 1


class IdentifiedRecord(DomainModel):
    id: UUID = Field(default_factory=uuid4)


class AuditTimestamps(DomainModel):
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class ImmutableRecord(IdentifiedRecord):
    created_at: datetime = Field(default_factory=utc_now)


class AssumptionRecord(IdentifiedRecord):
    label: str
    detail: str
    category: str
    impact: str
    created_at: datetime = Field(default_factory=utc_now)


class CaveatRecord(IdentifiedRecord):
    label: str
    detail: str
    severity: WarningSeverity = WarningSeverity.WARNING
    created_at: datetime = Field(default_factory=utc_now)


class EntityRef(DomainModel):
    entity_type: EntityKind
    entity_id: UUID


class ReproducibilityMetadata(DomainModel):
    code_version: str | None = None
    workflow_version: str | None = None
    schema_version_label: str | None = None
    executor_name: str | None = None
    environment: str | None = None
    prompt_name: str | None = None
    prompt_version: str | None = None
    model_name: str | None = None
    random_seed: int | None = None
    deterministic: bool = True
    input_fingerprint: str | None = None
    config_fingerprint: str | None = None
    output_fingerprint: str | None = None
    dependency_versions: dict[str, str] = Field(default_factory=dict)
    upstream_artifact_checksums: dict[str, str] = Field(default_factory=dict)


class InvalidationMetadata(DomainModel):
    is_invalidated: bool = False
    invalidated_at: datetime | None = None
    invalidated_by_stage_run_id: UUID | None = None
    invalidation_reason: str | None = None
    invalidated_entity_refs: list[EntityRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_invalidation(self) -> "InvalidationMetadata":
        if self.is_invalidated:
            required = [
                self.invalidated_at,
                self.invalidated_by_stage_run_id,
                self.invalidation_reason,
            ]
            if any(value is None for value in required):
                raise ValueError(
                    "Invalidated records must include invalidated_at, invalidated_by_stage_run_id, "
                    "and invalidation_reason."
                )
        return self


class TimeCoverage(DomainModel):
    start_date: date | None = None
    end_date: date | None = None
    timezone: str | None = None
    expected_frequency: str | None = None


class ColumnProfile(DomainModel):
    name: str
    dtype: str
    semantic_role: str | None = None
    nullable: bool = True
    distinct_count: int | None = None
    missing_fraction: float | None = None
    sample_values: list[str] = Field(default_factory=list)


class TransformSpec(DomainModel):
    operation: str
    source_columns: list[str] = Field(default_factory=list)
    parameters: dict[str, Any] = Field(default_factory=dict)
    description: str | None = None


class AnalysisSpec(DomainModel):
    analysis_type: AnalysisType
    title: str
    objective: str
    dependent_variable: str | None = None
    independent_variables: list[str] = Field(default_factory=list)
    parameters: dict[str, Any] = Field(default_factory=dict)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)


class ArtifactRef(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID | None = None
    stage_run_id: UUID | None = None
    artifact_kind: ArtifactKind
    role: str
    uri: str
    storage_backend: str
    mime_type: str
    checksum_sha256: str | None = None
    byte_size: int | None = None
    artifact_version: int = 1
    parent_artifact_ref_id: UUID | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SystemHealth(DomainModel):
    status: str
    environment: str
    app_name: str
    openai_model: str


class ArchitectureDecision(DomainModel):
    component: str
    selection: str
    rationale: str


class StageDescriptor(DomainModel):
    stage: WorkflowStage
    label: str
    depends_on: list[WorkflowStage] = Field(default_factory=list)
    requires_approval: bool = False
    model_mediated: bool = False
    notebook_emission_required: bool = True


class AnalysisMethodDescriptor(DomainModel):
    key: str
    label: str
    description: str


class Investigation(IdentifiedRecord, AuditTimestamps):
    title: str
    raw_prompt: str
    status: InvestigationStatus = InvestigationStatus.ACTIVE
    current_branch_id: UUID | None = None
    root_branch_id: UUID | None = None
    head_stage_run_id: UUID | None = None
    notebook_version: int = 0
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    reproducibility: ReproducibilityMetadata = Field(default_factory=ReproducibilityMetadata)


class Branch(IdentifiedRecord, AuditTimestamps):
    investigation_id: UUID
    name: str
    status: BranchStatus = BranchStatus.ACTIVE
    parent_branch_id: UUID | None = None
    forked_from_notebook_entry_id: UUID | None = None
    forked_from_stage_run_id: UUID | None = None
    fork_point_notebook_version: int | None = None
    lineage_branch_ids: list[UUID] = Field(default_factory=list)
    lineage_depth: int = 0
    head_notebook_version: int = 0
    head_notebook_entry_id: UUID | None = None
    head_stage_run_id: UUID | None = None
    archived_at: datetime | None = None

    @model_validator(mode="after")
    def validate_branch_lineage(self) -> "Branch":
        if self.parent_branch_id is not None:
            if (
                self.forked_from_notebook_entry_id is None
                and self.forked_from_stage_run_id is None
                and self.fork_point_notebook_version is None
            ):
                raise ValueError(
                    "Forked branches must record a notebook entry, stage run, or notebook version anchor."
                )
        return self


class ResearchQuestion(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    prompt_text: str
    canonical_question: str
    market_universe: list[str] = Field(default_factory=list)
    benchmark: str | None = None
    horizon: str
    frequency: str | None = None
    unit_of_analysis: str
    success_criteria: list[str] = Field(default_factory=list)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)


class Hypothesis(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    research_question_id: UUID
    stage_run_id: UUID
    label: str
    thesis: str
    mechanism: str
    expected_direction: str | None = None
    target_assets: list[str] = Field(default_factory=list)
    explanatory_variables: list[str] = Field(default_factory=list)
    falsifiers: list[str] = Field(default_factory=list)
    priority_score: float | None = None
    status: HypothesisStatus = HypothesisStatus.PROPOSED
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)


class EvidenceSource(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    provider: str
    source_type: ProvenanceSourceType
    title: str
    url: str | None = None
    citation: str | None = None
    published_at: datetime | None = None
    summary: str
    extracted_claims: list[str] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)


class DatasetSource(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    provider: str
    external_id: str
    name: str
    description: str
    dataset_kind: str
    source_url: str | None = None
    entity_grain: str | None = None
    time_grain: str | None = None
    frequency: str | None = None
    coverage: TimeCoverage = Field(default_factory=TimeCoverage)
    access_metadata: dict[str, Any] = Field(default_factory=dict)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)


class DatasetProfile(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    dataset_source_id: UUID
    stage_run_id: UUID
    row_count: int | None = None
    columns: list[ColumnProfile] = Field(default_factory=list)
    key_candidates: list[str] = Field(default_factory=list)
    time_coverage: TimeCoverage = Field(default_factory=TimeCoverage)
    missingness_by_column: dict[str, float] = Field(default_factory=dict)
    duplicate_row_count: int | None = None
    quality_score: float | None = None
    quality_flags: list[str] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)


class MergeMapping(ImmutableRecord):
    merge_plan_id: UUID
    stage_run_id: UUID
    left_column: str
    right_column: str
    semantic_role: str
    transforms: list[TransformSpec] = Field(default_factory=list)
    confidence: float
    notes: str | None = None
    nullable_mismatch: bool = False


class MergePlan(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    left_dataset_source_id: UUID
    right_dataset_source_id: UUID
    output_name: str
    join_type: MergeJoinType
    mappings: list[MergeMapping] = Field(default_factory=list)
    time_alignment_policy: TimeAlignmentPolicy
    lag_assumption: str
    filters: list[str] = Field(default_factory=list)
    derived_transforms: list[TransformSpec] = Field(default_factory=list)
    validation_checks: list[str] = Field(default_factory=list)
    ambiguity_notes: list[str] = Field(default_factory=list)
    confidence: float
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    approved_by_checkpoint_id: UUID | None = None


class AnalysisDataset(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    merge_plan_id: UUID
    dataset_kind: AnalysisDatasetKind
    test_plan_id: UUID | None = None
    name: str
    grain: str
    frequency: str
    row_count: int | None = None
    feature_columns: list[str] = Field(default_factory=list)
    target_columns: list[str] = Field(default_factory=list)
    identifier_columns: list[str] = Field(default_factory=list)
    time_column: str | None = None
    upstream_dataset_source_ids: list[UUID] = Field(default_factory=list)
    materialized_artifact_ref_id: UUID | None = None
    reproducibility: ReproducibilityMetadata = Field(default_factory=ReproducibilityMetadata)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_dataset_kind(self) -> "AnalysisDataset":
        if self.dataset_kind == AnalysisDatasetKind.CANONICAL and self.test_plan_id is not None:
            raise ValueError("Canonical analysis datasets must not reference a test_plan_id.")
        if self.dataset_kind == AnalysisDatasetKind.MATERIALIZED and self.test_plan_id is None:
            raise ValueError("Materialized analysis datasets must reference a test_plan_id.")
        return self


class TestPlan(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    canonical_dataset_id: UUID
    selected_hypothesis_ids: list[UUID] = Field(default_factory=list)
    title: str
    objective: str
    analyses: list[AnalysisSpec] = Field(default_factory=list)
    approval_required: bool = True
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    assumptions: list[AssumptionRecord] = Field(default_factory=list)
    caveats: list[CaveatRecord] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)


class AnalysisRun(IdentifiedRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    test_plan_id: UUID
    analysis_dataset_id: UUID
    analysis_type: AnalysisType
    config: dict[str, Any] = Field(default_factory=dict)
    status: StageRunStatus
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    result_artifact_ids: list[UUID] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    reproducibility: ReproducibilityMetadata = Field(default_factory=ReproducibilityMetadata)

    @model_validator(mode="after")
    def validate_terminal_state(self) -> "AnalysisRun":
        if self.status == StageRunStatus.COMPLETED and self.completed_at is None:
            raise ValueError("Completed analysis runs must include completed_at.")
        return self


class ResultArtifact(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    analysis_run_id: UUID | None = None
    artifact_ref_id: UUID
    artifact_type: ResultArtifactType
    title: str
    description: str
    metric_summary: dict[str, float | str | int] = Field(default_factory=dict)
    notebook_visible: bool = True
    provenance_ids: list[UUID] = Field(default_factory=list)


class Warning(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID | None = None
    stage_run_id: UUID | None = None
    subject_ref: EntityRef | None = None
    owner_ref: EntityRef | None = None
    severity: WarningSeverity
    code: str
    message: str
    details: str | None = None
    mitigation: str | None = None


class ProvenanceRecord(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID | None = None
    stage_run_id: UUID | None = None
    subject_ref: EntityRef
    owner_ref: EntityRef | None = None
    source_type: ProvenanceSourceType
    source_label: str
    source_uri: str | None = None
    external_reference: str | None = None
    citation_text: str | None = None
    source_artifact_ref_id: UUID | None = None
    content_fingerprint: str | None = None
    model_name: str | None = None
    prompt_name: str | None = None
    prompt_version: str | None = None
    tool_calls: list[dict[str, Any]] = Field(default_factory=list)
    input_fingerprint: str | None = None
    output_fingerprint: str | None = None
    recorded_at: datetime = Field(default_factory=utc_now)


class UserDecision(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    actor_label: str
    decision_type: UserDecisionType
    stage_run_id: UUID | None = None
    notebook_entry_id: UUID | None = None
    approval_checkpoint_id: UUID | None = None
    selected_refs: list[EntityRef] = Field(default_factory=list)
    rationale: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    affects_stage_run_ids: list[UUID] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)


class StageRun(IdentifiedRecord):
    investigation_id: UUID
    branch_id: UUID
    stage: WorkflowStage
    attempt: int = 1
    status: StageRunStatus
    input_refs: list[EntityRef] = Field(default_factory=list)
    output_refs: list[EntityRef] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    upstream_stage_run_ids: list[UUID] = Field(default_factory=list)
    supersedes_stage_run_id: UUID | None = None
    approval_checkpoint_id: UUID | None = None
    notebook_entry_ids: list[UUID] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=utc_now)
    completed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    failure_message: str | None = None
    recovery_options: list[str] = Field(default_factory=list)
    output_reproducibility: ReproducibilityMetadata = Field(default_factory=ReproducibilityMetadata)
    invalidation: InvalidationMetadata = Field(default_factory=InvalidationMetadata)

    @model_validator(mode="after")
    def validate_stage_run(self) -> "StageRun":
        terminal_statuses = {
            StageRunStatus.COMPLETED,
            StageRunStatus.FAILED,
            StageRunStatus.CANCELLED,
            StageRunStatus.INVALIDATED,
        }
        if self.status in terminal_statuses and self.completed_at is None:
            raise ValueError("Terminal stage runs must include completed_at.")
        if self.status == StageRunStatus.AWAITING_APPROVAL and self.approval_checkpoint_id is None:
            raise ValueError("Awaiting approval stage runs must reference an approval checkpoint.")
        return self


class ApprovalCheckpoint(IdentifiedRecord):
    investigation_id: UUID
    branch_id: UUID
    stage_run_id: UUID
    stage: WorkflowStage | None = None
    status: ApprovalStatus = ApprovalStatus.PENDING
    requested_by: str
    requested_at: datetime = Field(default_factory=utc_now)
    resolved_at: datetime | None = None
    approver_label: str | None = None
    decision_id: UUID | None = None
    request_notebook_entry_id: UUID
    resolution_notebook_entry_id: UUID | None = None
    subject_refs: list[EntityRef] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    notes: str | None = None
    superseded_by_checkpoint_id: UUID | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> "ApprovalCheckpoint":
        if self.status in {ApprovalStatus.APPROVED, ApprovalStatus.REJECTED}:
            if self.resolved_at is None or self.decision_id is None or self.approver_label is None:
                raise ValueError(
                    "Resolved approval checkpoints must include resolved_at, decision_id, and approver_label."
                )
        return self


class NotebookEntry(IdentifiedRecord):
    investigation_id: UUID
    branch_id: UUID
    notebook_version: int
    kind: NotebookEntryKind
    title: str
    summary: str
    previous_entry_id: UUID | None = None
    stage_run_id: UUID | None = None
    approval_checkpoint_id: UUID | None = None
    user_decision_id: UUID | None = None
    related_refs: list[EntityRef] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    notebook_state_artifact_ref_id: UUID | None = None
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def validate_anchor(self) -> "NotebookEntry":
        if (
            self.stage_run_id is None
            and self.approval_checkpoint_id is None
            and self.user_decision_id is None
        ):
            raise ValueError(
                "Notebook entries must reference at least one of stage_run_id, "
                "approval_checkpoint_id, or user_decision_id."
            )
        return self


class StageRunSummary(ImmutableRecord):
    investigation_id: UUID
    branch_id: UUID
    stage: WorkflowStage
    attempt: int = 1
    status: StageRunStatus
    invalidated_by_stage_run_id: UUID | None = None


class InvestigationSummary(IdentifiedRecord):
    title: str
    status: InvestigationStatus = InvestigationStatus.ACTIVE
    active_stage: WorkflowStage = WorkflowStage.INTAKE
    created_at: datetime = Field(default_factory=utc_now)


class BranchSummary(IdentifiedRecord):
    investigation_id: UUID
    name: str
    parent_branch_id: UUID | None = None
    created_at: datetime = Field(default_factory=utc_now)


class NotebookEntrySummary(IdentifiedRecord):
    branch_id: UUID
    stage: WorkflowStage | None = None
    title: str
    created_at: datetime = Field(default_factory=utc_now)

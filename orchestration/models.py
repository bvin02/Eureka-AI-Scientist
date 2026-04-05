from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from pydantic import Field

from domain.enums import ApprovalStatus, NotebookEntryKind, StageRunStatus, WorkflowStage
from domain.models import (
    AnalysisDataset,
    AnalysisRun,
    ApprovalCheckpoint,
    ArtifactRef,
    Branch,
    DatasetProfile,
    DatasetSource,
    DomainModel,
    EntityRef,
    EvidenceSource,
    Hypothesis,
    Investigation,
    MergePlan,
    NotebookEntry,
    ProvenanceRecord,
    ResearchQuestion,
    ResultArtifact,
    StageRun,
    TestPlan,
    UserDecision,
    Warning,
)


class RecoveryOption(DomainModel):
    action: str
    label: str
    description: str
    target_stage: WorkflowStage | None = None


class StageFailure(Exception):
    def __init__(
        self,
        message: str,
        recovery_options: list[RecoveryOption] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.recovery_options = recovery_options or []


class WorkflowSnapshot(DomainModel):
    workflow_version: str = "1.0"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    investigations: dict[UUID, Investigation] = Field(default_factory=dict)
    branches: dict[UUID, Branch] = Field(default_factory=dict)
    research_questions: dict[UUID, ResearchQuestion] = Field(default_factory=dict)
    hypotheses: dict[UUID, Hypothesis] = Field(default_factory=dict)
    evidence_sources: dict[UUID, EvidenceSource] = Field(default_factory=dict)
    dataset_sources: dict[UUID, DatasetSource] = Field(default_factory=dict)
    dataset_profiles: dict[UUID, DatasetProfile] = Field(default_factory=dict)
    merge_plans: dict[UUID, MergePlan] = Field(default_factory=dict)
    analysis_datasets: dict[UUID, AnalysisDataset] = Field(default_factory=dict)
    test_plans: dict[UUID, TestPlan] = Field(default_factory=dict)
    analysis_runs: dict[UUID, AnalysisRun] = Field(default_factory=dict)
    result_artifacts: dict[UUID, ResultArtifact] = Field(default_factory=dict)
    notebook_entries: dict[UUID, NotebookEntry] = Field(default_factory=dict)
    warnings: dict[UUID, Warning] = Field(default_factory=dict)
    provenance_records: dict[UUID, ProvenanceRecord] = Field(default_factory=dict)
    user_decisions: dict[UUID, UserDecision] = Field(default_factory=dict)
    stage_runs: dict[UUID, StageRun] = Field(default_factory=dict)
    approval_checkpoints: dict[UUID, ApprovalCheckpoint] = Field(default_factory=dict)
    artifact_refs: dict[UUID, ArtifactRef] = Field(default_factory=dict)


class BranchRuntimeState(DomainModel):
    branch_id: UUID
    next_stage: WorkflowStage | None = None
    latest_completed_stage: WorkflowStage | None = None
    active_stage_run_id: UUID | None = None
    pending_approval_checkpoint_id: UUID | None = None
    blocked_reason: str | None = None
    resumable: bool = True
    completed_stage_runs: dict[WorkflowStage, UUID] = Field(default_factory=dict)
    invalidated_stage_run_ids: list[UUID] = Field(default_factory=list)
    recovery_options: list[RecoveryOption] = Field(default_factory=list)


class WorkflowState(DomainModel):
    investigation_id: UUID
    current_branch_id: UUID
    current_stage: WorkflowStage | None = None
    branch_states: list[BranchRuntimeState] = Field(default_factory=list)
    stage_order: list[WorkflowStage] = Field(default_factory=list)
    snapshot: WorkflowSnapshot


class WorkflowEvent(DomainModel):
    kind: NotebookEntryKind
    title: str
    summary: str
    stage: WorkflowStage | None = None
    stage_run_id: UUID | None = None
    approval_checkpoint_id: UUID | None = None
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)


class WorkflowRunResult(DomainModel):
    state: WorkflowState
    executed_stage_runs: list[UUID] = Field(default_factory=list)
    emitted_notebook_entry_ids: list[UUID] = Field(default_factory=list)


class ApprovalResolution(DomainModel):
    checkpoint_id: UUID
    status: ApprovalStatus
    actor_label: str
    rationale: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class BranchForkRequest(DomainModel):
    source_branch_id: UUID
    anchor_stage_run_id: UUID
    actor_label: str
    new_branch_name: str
    rationale: str | None = None


class UserEditRequest(DomainModel):
    branch_id: UUID
    anchor_stage: WorkflowStage
    decision_action: str
    actor_label: str
    rationale: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class ModelStagePayload(DomainModel):
    prompt_name: str
    raw_output: dict[str, Any] = Field(default_factory=dict)
    output_refs: list[EntityRef] = Field(default_factory=list)


class StageExecutionResult(DomainModel):
    title: str
    summary: str
    related_refs: list[EntityRef] = Field(default_factory=list)
    warning_ids: list[UUID] = Field(default_factory=list)
    provenance_ids: list[UUID] = Field(default_factory=list)
    artifact_ref_ids: list[UUID] = Field(default_factory=list)
    active_checkpoint_id: UUID | None = None
    notebook_kind: NotebookEntryKind = NotebookEntryKind.STAGE
    recovery_options: list[RecoveryOption] = Field(default_factory=list)
    model_payload: ModelStagePayload | None = None

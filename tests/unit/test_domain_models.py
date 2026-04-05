from datetime import datetime, timezone
from uuid import uuid4

import pytest

from domain.enums import (
    AnalysisDatasetKind,
    ApprovalStatus,
    ArtifactKind,
    EntityKind,
    NotebookEntryKind,
    StageRunStatus,
    WarningSeverity,
    WorkflowStage,
)
from domain.models import (
    AnalysisDataset,
    ApprovalCheckpoint,
    ArtifactRef,
    Branch,
    EntityRef,
    InvalidationMetadata,
    NotebookEntry,
    StageRun,
)


def test_invalidation_metadata_requires_reasoning_fields_when_invalidated() -> None:
    with pytest.raises(ValueError):
        InvalidationMetadata(is_invalidated=True)


def test_stage_run_requires_approval_checkpoint_when_waiting_for_approval() -> None:
    with pytest.raises(ValueError):
        StageRun(
            investigation_id=uuid4(),
            branch_id=uuid4(),
            stage=WorkflowStage.AWAIT_USER_MERGE_APPROVAL,
            status=StageRunStatus.AWAITING_APPROVAL,
        )


def test_approval_checkpoint_requires_decision_fields_when_resolved() -> None:
    with pytest.raises(ValueError):
        ApprovalCheckpoint(
            investigation_id=uuid4(),
            branch_id=uuid4(),
            stage_run_id=uuid4(),
            requested_by="system",
            request_notebook_entry_id=uuid4(),
            status=ApprovalStatus.APPROVED,
        )


def test_notebook_entry_requires_anchor_reference() -> None:
    with pytest.raises(ValueError):
        NotebookEntry(
            investigation_id=uuid4(),
            branch_id=uuid4(),
            notebook_version=1,
            kind=NotebookEntryKind.STAGE,
            title="Entry",
            summary="No anchor",
        )


def test_valid_stage_run_supports_immutable_outputs_and_invalidation() -> None:
    artifact = ArtifactRef(
        investigation_id=uuid4(),
        branch_id=uuid4(),
        artifact_kind=ArtifactKind.DATASET_FILE,
        role="canonical_dataset",
        uri="artifacts/datasets/canonical.parquet",
        storage_backend="local",
        mime_type="application/x-parquet",
    )

    stage_run = StageRun(
        investigation_id=uuid4(),
        branch_id=uuid4(),
        stage=WorkflowStage.BUILD_CANONICAL_DATASET,
        status=StageRunStatus.COMPLETED,
        artifact_ref_ids=[artifact.id],
        output_refs=[EntityRef(entity_type=EntityKind.ARTIFACT_REF, entity_id=artifact.id)],
        completed_at=datetime.now(timezone.utc),
    )

    assert stage_run.output_refs[0].entity_type == EntityKind.ARTIFACT_REF
    assert stage_run.invalidation.is_invalidated is False


def test_branch_requires_fork_anchor_when_parent_branch_is_present() -> None:
    with pytest.raises(ValueError):
        Branch(
            investigation_id=uuid4(),
            name="Fork",
            parent_branch_id=uuid4(),
        )


def test_analysis_dataset_kind_prevents_overloaded_records() -> None:
    with pytest.raises(ValueError):
        AnalysisDataset(
            investigation_id=uuid4(),
            branch_id=uuid4(),
            stage_run_id=uuid4(),
            merge_plan_id=uuid4(),
            dataset_kind=AnalysisDatasetKind.CANONICAL,
            test_plan_id=uuid4(),
            name="bad",
            grain="monthly",
            frequency="monthly",
        )


def test_warning_severity_enum_is_available_for_domain_records() -> None:
    assert WarningSeverity.CRITICAL == "critical"

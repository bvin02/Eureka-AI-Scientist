from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from domain.enums import WorkflowStage
from domain.models import (
    AnalysisDataset,
    AnalysisRun,
    ApprovalCheckpoint,
    ArtifactRef,
    Branch,
    DatasetProfile,
    DatasetSource,
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
from orchestration.models import WorkflowSnapshot


class WorkflowStore:
    def __init__(self, snapshot: WorkflowSnapshot | None = None) -> None:
        self.snapshot = snapshot or WorkflowSnapshot(created_at=datetime.now(timezone.utc))

    def to_json(self) -> str:
        return self.snapshot.model_dump_json(indent=2)

    @classmethod
    def from_json(cls, payload: str) -> "WorkflowStore":
        return cls(WorkflowSnapshot.model_validate_json(payload))

    def save_json(self, path: Path) -> None:
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load_json(cls, path: Path) -> "WorkflowStore":
        return cls.from_json(path.read_text(encoding="utf-8"))

    def put(self, record: Any) -> None:
        mapping = self._mapping_for(record)
        mapping[record.id] = record

    def _mapping_for(self, record: Any) -> dict[UUID, Any]:
        if isinstance(record, Investigation):
            return self.snapshot.investigations
        if isinstance(record, Branch):
            return self.snapshot.branches
        if isinstance(record, ResearchQuestion):
            return self.snapshot.research_questions
        if isinstance(record, Hypothesis):
            return self.snapshot.hypotheses
        if isinstance(record, EvidenceSource):
            return self.snapshot.evidence_sources
        if isinstance(record, DatasetSource):
            return self.snapshot.dataset_sources
        if isinstance(record, DatasetProfile):
            return self.snapshot.dataset_profiles
        if isinstance(record, MergePlan):
            return self.snapshot.merge_plans
        if isinstance(record, AnalysisDataset):
            return self.snapshot.analysis_datasets
        if isinstance(record, TestPlan):
            return self.snapshot.test_plans
        if isinstance(record, AnalysisRun):
            return self.snapshot.analysis_runs
        if isinstance(record, ResultArtifact):
            return self.snapshot.result_artifacts
        if isinstance(record, NotebookEntry):
            return self.snapshot.notebook_entries
        if isinstance(record, Warning):
            return self.snapshot.warnings
        if isinstance(record, ProvenanceRecord):
            return self.snapshot.provenance_records
        if isinstance(record, UserDecision):
            return self.snapshot.user_decisions
        if isinstance(record, StageRun):
            return self.snapshot.stage_runs
        if isinstance(record, ApprovalCheckpoint):
            return self.snapshot.approval_checkpoints
        if isinstance(record, ArtifactRef):
            return self.snapshot.artifact_refs
        raise TypeError(f"Unsupported record type: {type(record)!r}")

    def stage_runs_for_branch(self, branch_id: UUID) -> list[StageRun]:
        return [run for run in self.snapshot.stage_runs.values() if run.branch_id == branch_id]

    def notebook_entries_for_branch(self, branch_id: UUID) -> list[NotebookEntry]:
        return sorted(
            [entry for entry in self.snapshot.notebook_entries.values() if entry.branch_id == branch_id],
            key=lambda entry: entry.notebook_version,
        )

    def latest_notebook_entry(self, branch_id: UUID) -> NotebookEntry | None:
        entries = self.notebook_entries_for_branch(branch_id)
        return entries[-1] if entries else None

    def next_notebook_version(self, branch_id: UUID) -> int:
        branch = self.snapshot.branches[branch_id]
        return branch.head_notebook_version + 1

    def latest_branch_stage_run(self, branch_id: UUID, stage: WorkflowStage) -> StageRun | None:
        runs = [
            run
            for run in self.stage_runs_for_branch(branch_id)
            if run.stage == stage
        ]
        if not runs:
            return None
        runs.sort(key=lambda run: (run.attempt, run.started_at))
        return runs[-1]

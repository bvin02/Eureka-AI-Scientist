from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from domain.enums import ArtifactKind, NotebookEntryKind
from domain.models import (
    ArtifactRef,
    Branch,
    EntityRef,
    NotebookEntry,
    StageRun,
    UserDecision,
)
from infra.artifact_store import LocalArtifactStore
from orchestration.store import WorkflowStore


def notebook_capabilities() -> list[str]:
    return ["timeline_entries", "branching", "compare_branches", "memo_export"]


class NotebookService:
    def __init__(self, store: WorkflowStore, artifact_store: LocalArtifactStore) -> None:
        self.store = store
        self.artifact_store = artifact_store

    def append_stage_entry(
        self,
        branch: Branch,
        title: str,
        summary: str,
        related_refs: list[EntityRef],
        artifact_ref_ids: list[UUID],
        warning_ids: list[UUID],
        provenance_ids: list[UUID],
        stage_run: StageRun | None = None,
        kind: NotebookEntryKind = NotebookEntryKind.STAGE,
        approval_checkpoint_id: UUID | None = None,
        user_decision_id: UUID | None = None,
    ) -> NotebookEntry:
        version = self.store.next_notebook_version(branch.id)
        previous_entry = self.store.latest_notebook_entry(branch.id)
        entry = NotebookEntry(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            notebook_version=version,
            kind=kind,
            title=title,
            summary=summary,
            previous_entry_id=previous_entry.id if previous_entry is not None else None,
            stage_run_id=stage_run.id if stage_run is not None else None,
            approval_checkpoint_id=approval_checkpoint_id,
            user_decision_id=user_decision_id,
            related_refs=related_refs,
            artifact_ref_ids=artifact_ref_ids,
            warning_ids=warning_ids,
            provenance_ids=provenance_ids,
            created_at=datetime.now(timezone.utc),
        )
        self.store.put(entry)
        branch.head_notebook_version = version
        branch.head_notebook_entry_id = entry.id
        branch.updated_at = datetime.now(timezone.utc)
        self.store.put(branch)
        snapshot_artifact = self._write_notebook_snapshot(branch.id, version)
        self.store.put(snapshot_artifact)
        entry.notebook_state_artifact_ref_id = snapshot_artifact.id
        self.store.put(entry)
        return entry

    def append_decision_entry(
        self,
        branch: Branch,
        decision: UserDecision,
        title: str,
        summary: str,
        related_refs: list[EntityRef],
    ) -> NotebookEntry:
        stage_run = (
            self.store.snapshot.stage_runs[decision.stage_run_id]
            if decision.stage_run_id is not None
            else None
        )
        return self.append_stage_entry(
            branch=branch,
            title=title,
            summary=summary,
            related_refs=related_refs,
            artifact_ref_ids=[],
            warning_ids=[],
            provenance_ids=[],
            stage_run=stage_run,
            kind=NotebookEntryKind.DECISION,
            user_decision_id=decision.id,
        )

    def _write_notebook_snapshot(self, branch_id: UUID, version: int) -> ArtifactRef:
        payload = self.store.snapshot.model_dump(mode="json")
        relative_path = f"notebook/{branch_id}/snapshot_v{version}.json"
        path, checksum, byte_size = self.artifact_store.write_json(relative_path, payload)
        return ArtifactRef(
            investigation_id=self.store.snapshot.branches[branch_id].investigation_id,
            branch_id=branch_id,
            artifact_kind=ArtifactKind.NOTEBOOK_SNAPSHOT,
            role="notebook_state",
            uri=str(Path(path)),
            storage_backend="local",
            mime_type="application/json",
            checksum_sha256=checksum,
            byte_size=byte_size,
            metadata={"notebook_version": version},
        )

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from domain.enums import (
    AnalysisDatasetKind,
    ApprovalStatus,
    ArtifactKind,
    EntityKind,
    NotebookEntryKind,
    ProvenanceSourceType,
    StageRunStatus,
    UserDecisionType,
    WarningSeverity,
    WorkflowStage,
)
from domain.models import (
    AnalysisDataset,
    AnalysisRun,
    AnalysisSpec,
    ApprovalCheckpoint,
    ArtifactRef,
    Branch,
    CaveatRecord,
    DatasetProfile,
    DatasetSource,
    EntityRef,
    EvidenceSource,
    Hypothesis,
    Investigation,
    JoinEdge,
    MergeMapping,
    MergePlan,
    DroppedColumn,
    NotebookEntry,
    ProvenanceRecord,
    ReproducibilityMetadata,
    ResearchQuestion,
    ResultArtifact,
    StageRun,
    TestPlan,
    TransformSpec,
    UserDecision,
    Warning,
    utc_now,
)
from infra.artifact_store import LocalArtifactStore
from infra.settings import get_settings
from data.adapters.registry import AdapterRegistry
from llm.merge_planner import MergePlannerDatasetProfile, MergePlannerInput
from notebook.service import NotebookService
from orchestration.fingerprints import fingerprint
from orchestration.contracts import CanonicalBuildInput, MergePlanProposal, ResearchQuestionPlan
from orchestration.model_adapter import DeterministicWorkflowModelAdapter, WorkflowModelAdapter
from orchestration.models import (
    ApprovalResolution,
    BranchForkRequest,
    BranchRuntimeState,
    ModelStagePayload,
    RecoveryOption,
    StageExecutionResult,
    StageFailure,
    UserEditRequest,
    WorkflowRunResult,
    WorkflowState,
)
from orchestration.state_machine import WorkflowDefinition
from orchestration.store import WorkflowStore


class WorkflowEngine:
    def __init__(
        self,
        store: WorkflowStore | None = None,
        workflow: WorkflowDefinition | None = None,
        model_adapter: WorkflowModelAdapter | None = None,
        artifact_store: LocalArtifactStore | None = None,
    ) -> None:
        settings = get_settings()
        self.store = store or WorkflowStore()
        self.workflow = workflow or WorkflowDefinition()
        self.model_adapter = model_adapter or DeterministicWorkflowModelAdapter()
        self.artifact_store = artifact_store or LocalArtifactStore(Path(settings.artifacts_dir))
        self.notebook_service = NotebookService(self.store, self.artifact_store)

    async def create_investigation(
        self,
        title: str,
        raw_prompt: str,
        branch_name: str = "main",
    ) -> WorkflowState:
        investigation = Investigation(title=title, raw_prompt=raw_prompt)
        branch = Branch(investigation_id=investigation.id, name=branch_name)
        investigation.root_branch_id = branch.id
        investigation.current_branch_id = branch.id
        self.store.put(investigation)
        self.store.put(branch)
        return self.get_state(investigation.id, branch.id)

    def get_state(self, investigation_id: UUID, branch_id: UUID | None = None) -> WorkflowState:
        investigation = self.store.snapshot.investigations[investigation_id]
        current_branch_id = branch_id or investigation.current_branch_id
        if current_branch_id is None:
            raise RuntimeError("Investigation has no current branch.")
        branch_ids = [branch.id for branch in self.store.snapshot.branches.values() if branch.investigation_id == investigation_id]
        branch_states = [self._branch_runtime_state(branch_id=item) for item in branch_ids]
        current_state = next(state for state in branch_states if state.branch_id == current_branch_id)
        return WorkflowState(
            investigation_id=investigation_id,
            current_branch_id=current_branch_id,
            current_stage=current_state.next_stage,
            branch_states=branch_states,
            stage_order=[descriptor.stage for descriptor in self.workflow.ordered_stages()],
            snapshot=self.store.snapshot,
        )

    async def run_until_blocked(
        self,
        investigation_id: UUID,
        branch_id: UUID | None = None,
    ) -> WorkflowRunResult:
        executed_stage_runs: list[UUID] = []
        emitted_notebook_entry_ids: list[UUID] = []
        state = self.get_state(investigation_id, branch_id)
        while state.current_stage is not None:
            current_branch_state = self._branch_runtime_state(state.current_branch_id)
            if current_branch_state.pending_approval_checkpoint_id is not None:
                break
            if current_branch_state.blocked_reason == "stage_failed":
                break
            stage_run, notebook_entry = await self.run_next_stage(
                investigation_id=investigation_id,
                branch_id=state.current_branch_id,
            )
            executed_stage_runs.append(stage_run.id)
            emitted_notebook_entry_ids.append(notebook_entry.id)
            state = self.get_state(investigation_id, state.current_branch_id)
        return WorkflowRunResult(
            state=state,
            executed_stage_runs=executed_stage_runs,
            emitted_notebook_entry_ids=emitted_notebook_entry_ids,
        )

    async def run_next_stage(
        self,
        investigation_id: UUID,
        branch_id: UUID | None = None,
    ) -> tuple[StageRun, NotebookEntry]:
        state = self.get_state(investigation_id, branch_id)
        branch_runtime = self._branch_runtime_state(state.current_branch_id)
        if branch_runtime.pending_approval_checkpoint_id is not None:
            raise RuntimeError("Branch is awaiting approval and cannot advance.")
        stage = branch_runtime.next_stage
        if stage is None:
            raise RuntimeError("Workflow is already complete.")

        branch = self.store.snapshot.branches[state.current_branch_id]
        stage_run = self._start_stage_run(branch, stage)
        try:
            result = await self._execute_stage(branch, stage_run)
            stage_run.output_refs = result.related_refs
            stage_run.artifact_ref_ids = result.artifact_ref_ids
            stage_run.warning_ids = result.warning_ids
            stage_run.provenance_ids = result.provenance_ids
            stage_run.recovery_options = [option.label for option in result.recovery_options]

            notebook_entry = self.notebook_service.append_stage_entry(
                branch=branch,
                title=result.title,
                summary=result.summary,
                related_refs=result.related_refs,
                artifact_ref_ids=result.artifact_ref_ids,
                warning_ids=result.warning_ids,
                provenance_ids=result.provenance_ids,
                stage_run=stage_run,
                kind=result.notebook_kind,
            )
            stage_run.notebook_entry_ids.append(notebook_entry.id)

            if result.active_checkpoint_id is not None:
                checkpoint = self.store.snapshot.approval_checkpoints[result.active_checkpoint_id]
                checkpoint.request_notebook_entry_id = notebook_entry.id
                self.store.put(checkpoint)
                notebook_entry.approval_checkpoint_id = checkpoint.id
                self.store.put(notebook_entry)
                stage_run.status = StageRunStatus.AWAITING_APPROVAL
                stage_run.approval_checkpoint_id = result.active_checkpoint_id
            else:
                stage_run.status = StageRunStatus.COMPLETED
                stage_run.completed_at = utc_now()

            stage_run.output_reproducibility.output_fingerprint = fingerprint(
                {
                    "related_refs": [ref.model_dump(mode="json") for ref in result.related_refs],
                    "artifact_ref_ids": [str(item) for item in result.artifact_ref_ids],
                    "warning_ids": [str(item) for item in result.warning_ids],
                    "provenance_ids": [str(item) for item in result.provenance_ids],
                }
            )
            self.store.put(stage_run)
            branch.head_stage_run_id = stage_run.id
            branch.updated_at = utc_now()
            self.store.put(branch)
            investigation = self.store.snapshot.investigations[investigation_id]
            investigation.head_stage_run_id = stage_run.id
            investigation.updated_at = utc_now()
            self.store.put(investigation)
            return stage_run, notebook_entry
        except StageFailure as failure:
            stage_run.status = StageRunStatus.FAILED
            stage_run.completed_at = utc_now()
            stage_run.failure_message = failure.message
            stage_run.recovery_options = [option.label for option in failure.recovery_options]
            self.store.put(stage_run)
            warning = self._create_warning(
                branch=branch,
                stage_run=stage_run,
                code="stage_failed",
                message=failure.message,
                severity=WarningSeverity.CRITICAL,
                mitigation="Use the recovery options to retry, edit inputs, or branch from this stage.",
            )
            provenance = self._create_provenance(
                branch=branch,
                stage_run=stage_run,
                subject_ref=EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=stage_run.id),
                source_type=ProvenanceSourceType.SYSTEM,
                source_label="stage_failure",
                source_uri=None,
            )
            stage_run.warning_ids = [warning.id]
            stage_run.provenance_ids = [provenance.id]
            self.store.put(warning)
            self.store.put(provenance)
            notebook_entry = self.notebook_service.append_stage_entry(
                branch=branch,
                title=f"{self.workflow.descriptor(stage).label} Failed",
                summary=failure.message,
                related_refs=[],
                artifact_ref_ids=[],
                warning_ids=[warning.id],
                provenance_ids=[provenance.id],
                stage_run=stage_run,
                kind=NotebookEntryKind.WARNING,
            )
            stage_run.notebook_entry_ids.append(notebook_entry.id)
            self.store.put(stage_run)
            return stage_run, notebook_entry

    def resolve_approval(
        self,
        resolution: ApprovalResolution,
    ) -> WorkflowState:
        checkpoint = self.store.snapshot.approval_checkpoints[resolution.checkpoint_id]
        branch = self.store.snapshot.branches[checkpoint.branch_id]
        stage_run = self.store.snapshot.stage_runs[checkpoint.stage_run_id]

        decision = UserDecision(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            actor_label=resolution.actor_label,
            decision_type=(
                UserDecisionType.APPROVE_MERGE_PLAN
                if stage_run.stage == WorkflowStage.AWAIT_USER_MERGE_APPROVAL and resolution.status == ApprovalStatus.APPROVED
                else UserDecisionType.REJECT_TEST_PLAN
                if stage_run.stage == WorkflowStage.AWAIT_USER_TEST_APPROVAL and resolution.status == ApprovalStatus.REJECTED
                else UserDecisionType.APPROVE_TEST_PLAN
                if stage_run.stage == WorkflowStage.AWAIT_USER_TEST_APPROVAL
                else UserDecisionType.OVERRIDE_MERGE_PLAN
            ),
            stage_run_id=stage_run.id,
            approval_checkpoint_id=checkpoint.id,
            rationale=resolution.rationale,
            payload=resolution.payload,
            selected_refs=checkpoint.subject_refs,
        )
        self.store.put(decision)

        decision_entry = self.notebook_service.append_decision_entry(
            branch=branch,
            decision=decision,
            title=f"Approval resolved: {stage_run.stage.value}",
            summary=resolution.rationale or resolution.status.value,
            related_refs=checkpoint.subject_refs,
        )

        checkpoint.status = resolution.status
        checkpoint.approver_label = resolution.actor_label
        checkpoint.decision_id = decision.id
        checkpoint.resolved_at = utc_now()
        checkpoint.resolution_notebook_entry_id = decision_entry.id
        self.store.put(checkpoint)

        stage_run.status = StageRunStatus.COMPLETED if resolution.status == ApprovalStatus.APPROVED else StageRunStatus.CANCELLED
        stage_run.completed_at = utc_now()
        stage_run.notebook_entry_ids.append(decision_entry.id)
        self.store.put(stage_run)

        if resolution.status == ApprovalStatus.APPROVED:
            for subject in checkpoint.subject_refs:
                if subject.entity_type == EntityKind.MERGE_PLAN:
                    merge_plan = self.store.snapshot.merge_plans[subject.entity_id]
                    merge_plan.approved_by_checkpoint_id = checkpoint.id
                    self.store.put(merge_plan)
        else:
            self.invalidate_downstream(
                branch_id=branch.id,
                anchor_stage=stage_run.stage,
                invalidated_by_stage_run_id=stage_run.id,
                reason="Approval rejected; downstream outputs are no longer valid.",
            )

        return self.get_state(branch.investigation_id, branch.id)

    def record_user_edit(self, request: UserEditRequest) -> WorkflowState:
        branch = self.store.snapshot.branches[request.branch_id]
        decision = UserDecision(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            actor_label=request.actor_label,
            decision_type=UserDecisionType.ADJUST_ANALYSIS,
            rationale=request.rationale,
            payload=request.payload | {"action": request.decision_action, "anchor_stage": request.anchor_stage.value},
        )
        self.store.put(decision)
        self.notebook_service.append_decision_entry(
            branch=branch,
            decision=decision,
            title=f"User edit at {request.anchor_stage.value}",
            summary=request.rationale or request.decision_action,
            related_refs=[],
        )
        self.invalidate_downstream(
            branch_id=branch.id,
            anchor_stage=request.anchor_stage,
            invalidated_by_stage_run_id=self._latest_effective_stage_run(branch.id, request.anchor_stage).id,
            reason=f"User edit: {request.decision_action}",
        )
        return self.get_state(branch.investigation_id, branch.id)

    async def edit_hypothesis(
        self,
        branch_id: UUID,
        hypothesis_id: UUID,
        actor_label: str,
        user_instruction: str,
    ) -> Hypothesis:
        hypothesis = self.store.snapshot.hypotheses[hypothesis_id]
        branch = self.store.snapshot.branches[branch_id]
        if hypothesis.branch_id != branch_id:
            raise RuntimeError("Hypothesis does not belong to the requested branch.")

        stage_run = self._latest_effective_stage_run(branch_id, WorkflowStage.GENERATE_HYPOTHESES)
        from llm.hypothesis_engine import HypothesisEngine
        from orchestration.contracts import HypothesisProposal, HypothesisRewriteProposal

        existing = HypothesisProposal(
            label=hypothesis.label,
            title=hypothesis.title or hypothesis.label,
            thesis=hypothesis.thesis,
            mechanism=hypothesis.mechanism,
            required_variables=hypothesis.required_variables,
            preferred_proxies=hypothesis.preferred_proxies,
            recommended_test_type=None,
            expected_direction=hypothesis.expected_direction,
            target_assets=hypothesis.target_assets,
            explanatory_variables=hypothesis.explanatory_variables,
            likely_caveats=[item.detail for item in hypothesis.caveats],
            confidence_level=hypothesis.confidence_level or 0.5,
            novelty_usefulness_note=hypothesis.novelty_usefulness_note or "Edited hypothesis",
        )
        hypothesis_engine = getattr(self.model_adapter, "hypothesis_engine", HypothesisEngine())
        rewrite: HypothesisRewriteProposal = await hypothesis_engine.rewrite(existing, user_instruction)

        edited = Hypothesis(
            investigation_id=hypothesis.investigation_id,
            branch_id=branch_id,
            research_question_id=hypothesis.research_question_id,
            stage_run_id=stage_run.id,
            label=hypothesis.label,
            title=rewrite.title,
            thesis=rewrite.thesis,
            mechanism=rewrite.mechanism,
            required_variables=rewrite.required_variables,
            preferred_proxies=rewrite.preferred_proxies,
            recommended_test_type=rewrite.recommended_test_type.value if rewrite.recommended_test_type is not None else hypothesis.recommended_test_type,
            expected_direction=rewrite.expected_direction,
            target_assets=hypothesis.target_assets,
            explanatory_variables=rewrite.explanatory_variables,
            confidence_level=rewrite.confidence_level,
            novelty_usefulness_note=rewrite.novelty_usefulness_note,
            caveats=[CaveatRecord(label="hypothesis_edit", detail=item) for item in rewrite.likely_caveats],
            status=hypothesis.status,
        )
        self.store.put(edited)

        decision = UserDecision(
            investigation_id=branch.investigation_id,
            branch_id=branch_id,
            actor_label=actor_label,
            decision_type=UserDecisionType.EDIT_HYPOTHESIS,
            stage_run_id=stage_run.id,
            rationale=user_instruction,
            selected_refs=[
                EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=hypothesis.id),
                EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=edited.id),
            ],
            payload={"instruction": user_instruction},
            affects_stage_run_ids=[stage_run.id],
        )
        self.store.put(decision)
        self.notebook_service.append_decision_entry(
            branch=branch,
            decision=decision,
            title=f"Hypothesis edited: {edited.title or edited.label}",
            summary=user_instruction,
            related_refs=[
                EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=hypothesis.id),
                EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=edited.id),
            ],
        )
        hypothesis.status = hypothesis.status
        self.store.put(hypothesis)
        self.invalidate_downstream(
            branch_id=branch_id,
            anchor_stage=WorkflowStage.GENERATE_HYPOTHESES,
            invalidated_by_stage_run_id=stage_run.id,
            reason=f"Hypothesis edited: {edited.title or edited.label}",
        )
        return edited

    def edit_merge_plan(
        self,
        branch_id: UUID,
        merge_plan_id: UUID,
        actor_label: str,
        mapping_overrides: list[dict[str, Any]] | None = None,
        join_overrides: list[dict[str, Any]] | None = None,
        lag_policy_override: str | None = None,
        rationale: str | None = None,
    ) -> MergePlan:
        merge_plan = self.store.snapshot.merge_plans[merge_plan_id]
        branch = self.store.snapshot.branches[branch_id]
        if merge_plan.branch_id != branch_id:
            raise RuntimeError("Merge plan does not belong to the requested branch.")

        stage_run = self._latest_effective_stage_run(branch_id, WorkflowStage.PROPOSE_MERGE_PLAN)
        override_map = {
            (item.get("source_dataset_source_id"), item.get("source_column")): item
            for item in (mapping_overrides or [])
        }
        updated_mappings: list[MergeMapping] = []
        for mapping in merge_plan.mappings:
            key = (str(mapping.source_dataset_source_id) if mapping.source_dataset_source_id else None, mapping.left_column)
            override = override_map.get(key)
            if override is None:
                updated_mappings.append(mapping)
                continue
            updated_mappings.append(
                mapping.model_copy(
                    update={
                        "semantic_role": override.get("semantic_role", mapping.semantic_role),
                        "right_column": override.get("target_column", mapping.right_column),
                        "confidence": float(override.get("confidence", mapping.confidence)),
                        "notes": override.get("notes", mapping.notes),
                        "include_in_output": bool(override.get("include_in_output", mapping.include_in_output)),
                        "drop_reason": override.get("drop_reason", mapping.drop_reason),
                        "lag_rule": override.get("lag_rule", mapping.lag_rule),
                        "frequency_rule": override.get("frequency_rule", mapping.frequency_rule),
                        "date_normalization_rule": override.get("date_normalization_rule", mapping.date_normalization_rule),
                        "user_overridden": True,
                    }
                )
            )

        updated_join_graph = merge_plan.join_graph
        if join_overrides:
            updated_join_graph = []
            for item in join_overrides:
                updated_join_graph.append(
                    JoinEdge(
                        left_dataset_source_id=UUID(item["left_dataset_source_id"]),
                        right_dataset_source_id=UUID(item["right_dataset_source_id"]),
                        join_type=item.get("join_type", merge_plan.join_type),
                        join_keys=item.get("join_keys", []),
                        left_time_column=item.get("left_time_column"),
                        right_time_column=item.get("right_time_column"),
                        confidence=float(item.get("confidence", merge_plan.confidence)),
                        rationale=item.get("rationale", "User override"),
                    )
                )

        edited_plan = merge_plan.model_copy(
            update={
                "id": uuid4(),
                "created_at": utc_now(),
                "mappings": updated_mappings,
                "join_graph": updated_join_graph,
                "lag_policy": lag_policy_override or merge_plan.lag_policy,
                "lag_assumption": lag_policy_override or merge_plan.lag_assumption,
                "approved_by_checkpoint_id": None,
                "confidence": min(merge_plan.confidence, 0.9),
            },
            deep=True,
        )
        self.store.put(edited_plan)
        for checkpoint in self.store.snapshot.approval_checkpoints.values():
            if (
                checkpoint.branch_id == branch_id
                and checkpoint.stage == WorkflowStage.AWAIT_USER_MERGE_APPROVAL
                and checkpoint.status == ApprovalStatus.PENDING
            ):
                checkpoint.subject_refs = [EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=edited_plan.id)]
                self.store.put(checkpoint)

        decision_payload = {
            "merge_plan_id": str(merge_plan.id),
            "edited_merge_plan_id": str(edited_plan.id),
            "mapping_overrides": mapping_overrides or [],
            "join_overrides": join_overrides or [],
            "lag_policy_override": lag_policy_override,
        }
        decision = UserDecision(
            investigation_id=branch.investigation_id,
            branch_id=branch_id,
            actor_label=actor_label,
            decision_type=UserDecisionType.OVERRIDE_MERGE_PLAN,
            stage_run_id=stage_run.id,
            rationale=rationale,
            payload=decision_payload,
            selected_refs=[
                EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id),
                EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=edited_plan.id),
            ],
            affects_stage_run_ids=[stage_run.id],
        )
        self.store.put(decision)
        self.notebook_service.append_decision_entry(
            branch=branch,
            decision=decision,
            title="Merge plan overridden by user",
            summary=rationale or "User adjusted mapping/join rules before approval.",
            related_refs=[
                EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id),
                EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=edited_plan.id),
            ],
        )
        self.invalidate_downstream(
            branch_id=branch_id,
            anchor_stage=WorkflowStage.PROPOSE_MERGE_PLAN,
            invalidated_by_stage_run_id=stage_run.id,
            reason="Merge plan overrides changed downstream dataset build assumptions.",
        )
        return edited_plan

    def fork_from_hypothesis(
        self,
        source_branch_id: UUID,
        hypothesis_id: UUID,
        actor_label: str,
        new_branch_name: str,
        rationale: str | None = None,
    ) -> WorkflowState:
        hypothesis = self.store.snapshot.hypotheses[hypothesis_id]
        if hypothesis.branch_id != source_branch_id:
            raise RuntimeError("Hypothesis does not belong to the requested source branch.")

        state = self.fork_branch(
            BranchForkRequest(
                source_branch_id=source_branch_id,
                anchor_stage_run_id=hypothesis.stage_run_id,
                actor_label=actor_label,
                new_branch_name=new_branch_name,
                rationale=rationale or f"Forked from hypothesis: {hypothesis.title or hypothesis.label}",
            )
        )
        child_branch = self.store.snapshot.branches[state.current_branch_id]
        decision = UserDecision(
            investigation_id=child_branch.investigation_id,
            branch_id=child_branch.id,
            actor_label=actor_label,
            decision_type=UserDecisionType.SELECT_HYPOTHESIS,
            stage_run_id=hypothesis.stage_run_id,
            rationale=rationale,
            selected_refs=[EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=hypothesis.id)],
            payload={"fork_origin": "hypothesis_card"},
        )
        self.store.put(decision)
        self.notebook_service.append_decision_entry(
            branch=child_branch,
            decision=decision,
            title=f"Hypothesis selected: {hypothesis.title or hypothesis.label}",
            summary=rationale or "Forked branch to pursue this hypothesis.",
            related_refs=[EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=hypothesis.id)],
        )
        return self.get_state(child_branch.investigation_id, child_branch.id)

    def fork_branch(self, request: BranchForkRequest) -> WorkflowState:
        source_branch = self.store.snapshot.branches[request.source_branch_id]
        anchor_run = self.store.snapshot.stage_runs[request.anchor_stage_run_id]
        fork_entry = next(
            (entry for entry in self.store.snapshot.notebook_entries.values() if entry.stage_run_id == anchor_run.id),
            None,
        )
        child_branch = Branch(
            investigation_id=source_branch.investigation_id,
            name=request.new_branch_name,
            parent_branch_id=source_branch.id,
            forked_from_stage_run_id=anchor_run.id,
            forked_from_notebook_entry_id=fork_entry.id if fork_entry is not None else None,
            fork_point_notebook_version=fork_entry.notebook_version if fork_entry is not None else source_branch.head_notebook_version,
            lineage_branch_ids=source_branch.lineage_branch_ids + [source_branch.id],
            lineage_depth=source_branch.lineage_depth + 1,
        )
        self.store.put(child_branch)

        decision = UserDecision(
            investigation_id=child_branch.investigation_id,
            branch_id=child_branch.id,
            actor_label=request.actor_label,
            decision_type=UserDecisionType.FORK_BRANCH,
            stage_run_id=anchor_run.id,
            rationale=request.rationale,
            selected_refs=[EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=anchor_run.id)],
        )
        self.store.put(decision)
        self.notebook_service.append_decision_entry(
            branch=child_branch,
            decision=decision,
            title=f"Branch forked from {anchor_run.stage.value}",
            summary=request.rationale or "Fork created for alternative investigation path.",
            related_refs=[EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=anchor_run.id)],
        )

        investigation = self.store.snapshot.investigations[source_branch.investigation_id]
        investigation.current_branch_id = child_branch.id
        investigation.updated_at = utc_now()
        self.store.put(investigation)
        return self.get_state(investigation.id, child_branch.id)

    def invalidate_downstream(
        self,
        branch_id: UUID,
        anchor_stage: WorkflowStage,
        invalidated_by_stage_run_id: UUID,
        reason: str,
    ) -> list[UUID]:
        invalidated_ids: list[UUID] = []
        downstream = set(self.workflow.downstream_stages(anchor_stage))
        for run in self.store.stage_runs_for_branch(branch_id):
            if run.stage in downstream and not run.invalidation.is_invalidated:
                run.invalidation.is_invalidated = True
                run.invalidation.invalidated_at = utc_now()
                run.invalidation.invalidated_by_stage_run_id = invalidated_by_stage_run_id
                run.invalidation.invalidation_reason = reason
                run.status = StageRunStatus.INVALIDATED
                run.completed_at = run.completed_at or utc_now()
                self.store.put(run)
                invalidated_ids.append(run.id)
        return invalidated_ids

    def export_snapshot(self) -> str:
        return self.store.to_json()

    @classmethod
    def from_snapshot_json(cls, payload: str) -> "WorkflowEngine":
        return cls(store=WorkflowStore.from_json(payload))

    def _branch_runtime_state(self, branch_id: UUID) -> BranchRuntimeState:
        effective = self._effective_stage_runs(branch_id)
        pending_checkpoint_id: UUID | None = None
        blocked_reason: str | None = None
        next_stage: WorkflowStage | None = None
        latest_completed_stage: WorkflowStage | None = None
        active_stage_run_id: UUID | None = None
        recovery_options: list[RecoveryOption] = []

        for descriptor in self.workflow.ordered_stages():
            run = effective.get(descriptor.stage)
            if run is None:
                next_stage = descriptor.stage
                break
            if run.status == StageRunStatus.AWAITING_APPROVAL:
                next_stage = descriptor.stage
                pending_checkpoint_id = run.approval_checkpoint_id
                blocked_reason = "awaiting_approval"
                active_stage_run_id = run.id
                break
            if run.status == StageRunStatus.CANCELLED:
                next_stage = descriptor.stage
                blocked_reason = "cancelled"
                active_stage_run_id = run.id
                break
            if run.status == StageRunStatus.FAILED:
                next_stage = descriptor.stage
                blocked_reason = "stage_failed"
                active_stage_run_id = run.id
                recovery_options = [
                    RecoveryOption(
                        action="retry_stage",
                        label=label,
                        description=label,
                        target_stage=descriptor.stage,
                    )
                    for label in run.recovery_options
                ]
                break
            if run.status == StageRunStatus.INVALIDATED:
                next_stage = descriptor.stage
                blocked_reason = "invalidated"
                active_stage_run_id = run.id
                break
            if run.status == StageRunStatus.COMPLETED:
                latest_completed_stage = descriptor.stage
                active_stage_run_id = run.id
                continue

        completed_stage_runs = {
            stage: run.id
            for stage, run in effective.items()
            if run.status == StageRunStatus.COMPLETED and not run.invalidation.is_invalidated
        }
        invalidated_stage_run_ids = [
            run.id for run in effective.values() if run.invalidation.is_invalidated
        ]

        return BranchRuntimeState(
            branch_id=branch_id,
            next_stage=next_stage,
            latest_completed_stage=latest_completed_stage,
            active_stage_run_id=active_stage_run_id,
            pending_approval_checkpoint_id=pending_checkpoint_id,
            blocked_reason=blocked_reason,
            resumable=blocked_reason != "awaiting_approval",
            completed_stage_runs=completed_stage_runs,
            invalidated_stage_run_ids=invalidated_stage_run_ids,
            recovery_options=recovery_options,
        )

    def _effective_stage_runs(self, branch_id: UUID) -> dict[WorkflowStage, StageRun]:
        branch = self.store.snapshot.branches[branch_id]
        effective: dict[WorkflowStage, StageRun] = {}
        if branch.parent_branch_id is not None:
            effective = self._effective_stage_runs(branch.parent_branch_id)
            if branch.forked_from_stage_run_id is not None:
                anchor_run = self.store.snapshot.stage_runs[branch.forked_from_stage_run_id]
                cutoff = self.workflow.stage_index(anchor_run.stage)
                effective = {
                    stage: run
                    for stage, run in effective.items()
                    if self.workflow.stage_index(stage) <= cutoff
                }
                effective[anchor_run.stage] = anchor_run

        own_runs = sorted(
            self.store.stage_runs_for_branch(branch_id),
            key=lambda run: (self.workflow.stage_index(run.stage), run.attempt, run.started_at),
        )
        for run in own_runs:
            if run.invalidation.is_invalidated:
                continue
            effective[run.stage] = run
        return effective

    def _latest_effective_stage_run(self, branch_id: UUID, stage: WorkflowStage) -> StageRun:
        run = self._effective_stage_runs(branch_id).get(stage)
        if run is None:
            raise RuntimeError(f"No effective stage run for {stage.value} on branch {branch_id}.")
        return run

    def _start_stage_run(self, branch: Branch, stage: WorkflowStage) -> StageRun:
        prior = self.store.latest_branch_stage_run(branch.id, stage)
        input_refs = self._input_refs_for_stage(branch.id, stage)
        stage_run = StageRun(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage=stage,
            attempt=(prior.attempt + 1) if prior is not None else 1,
            status=StageRunStatus.RUNNING,
            input_refs=input_refs,
            upstream_stage_run_ids=[
                self._latest_effective_stage_run(branch.id, dep).id
                for dep in self.workflow.dependencies(stage)
                if dep in self._effective_stage_runs(branch.id)
            ],
            supersedes_stage_run_id=prior.id if prior is not None else None,
            output_reproducibility=ReproducibilityMetadata(
                workflow_version="1.0",
                executor_name="workflow_engine",
                environment=get_settings().env,
                model_name=get_settings().openai_model if self.workflow.descriptor(stage).model_mediated else None,
                input_fingerprint=fingerprint(
                    {
                        "stage": stage.value,
                        "branch_id": str(branch.id),
                        "input_refs": [ref.model_dump(mode="json") for ref in input_refs],
                    }
                ),
            ),
        )
        self.store.put(stage_run)
        return stage_run

    def _input_refs_for_stage(self, branch_id: UUID, stage: WorkflowStage) -> list[EntityRef]:
        refs: list[EntityRef] = []
        effective = self._effective_stage_runs(branch_id)
        for dependency in self.workflow.dependencies(stage):
            dependency_run = effective.get(dependency)
            if dependency_run is not None:
                refs.extend(dependency_run.output_refs)
        return refs

    async def _execute_stage(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        handler_map = {
            WorkflowStage.INTAKE: self._handle_intake,
            WorkflowStage.PARSE_RESEARCH_QUESTION: self._handle_parse_research_question,
            WorkflowStage.GENERATE_HYPOTHESES: self._handle_generate_hypotheses,
            WorkflowStage.RETRIEVE_EVIDENCE: self._handle_retrieve_evidence,
            WorkflowStage.DISCOVER_DATASETS: self._handle_discover_datasets,
            WorkflowStage.PROFILE_DATASETS: self._handle_profile_datasets,
            WorkflowStage.PROPOSE_MERGE_PLAN: self._handle_propose_merge_plan,
            WorkflowStage.AWAIT_USER_MERGE_APPROVAL: self._handle_await_merge_approval,
            WorkflowStage.BUILD_CANONICAL_DATASET: self._handle_build_canonical_dataset,
            WorkflowStage.PROPOSE_TEST_PLAN: self._handle_propose_test_plan,
            WorkflowStage.AWAIT_USER_TEST_APPROVAL: self._handle_await_test_approval,
            WorkflowStage.MATERIALIZE_ANALYSIS_DATASET: self._handle_materialize_analysis_dataset,
            WorkflowStage.EXECUTE_ANALYSIS: self._handle_execute_analysis,
            WorkflowStage.SUMMARIZE_RESULTS: self._handle_summarize_results,
            WorkflowStage.PROPOSE_NEXT_STEPS: self._handle_propose_next_steps,
        }
        return await handler_map[stage_run.stage](branch, stage_run)

    async def _handle_intake(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        investigation = self.store.snapshot.investigations[branch.investigation_id]
        artifact = self._write_json_artifact(
            branch=branch,
            stage_run=stage_run,
            artifact_kind=ArtifactKind.MODEL_OUTPUT,
            role="intake_prompt",
            payload={"title": investigation.title, "raw_prompt": investigation.raw_prompt},
        )
        warning = self._create_warning(
            branch,
            stage_run,
            code="intake_recorded",
            message="Investigation intake recorded.",
            severity=WarningSeverity.INFO,
        )
        provenance = self._create_provenance(
            branch=branch,
            stage_run=stage_run,
            subject_ref=EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=stage_run.id),
            source_type=ProvenanceSourceType.USER,
            source_label="user_prompt",
            source_uri=None,
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Investigation Intake",
            summary="Captured the initial research prompt and created the root workflow context.",
            related_refs=[EntityRef(entity_type=EntityKind.INVESTIGATION, entity_id=investigation.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_parse_research_question(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        investigation = self.store.snapshot.investigations[branch.investigation_id]
        parsed = await self.model_adapter.parse_research_question(investigation.raw_prompt)
        question = ResearchQuestion(
            investigation_id=investigation.id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            prompt_text=investigation.raw_prompt,
            canonical_question=parsed.canonical_question,
            market_universe=parsed.market_universe,
            benchmark=parsed.benchmark,
            horizon=parsed.horizon,
            frequency=parsed.frequency,
            unit_of_analysis=parsed.unit_of_analysis,
            success_criteria=parsed.success_criteria,
            caveats=parsed.caveats,
        )
        self.store.put(question)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "research_question", parsed.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "planner_fallback", "Structured research question generated.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.RESEARCH_QUESTION, entity_id=question.id),
            ProvenanceSourceType.LLM,
            "parse_research_question",
            source_uri=None,
            prompt_name="parse_research_question",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Research Question Parsed",
            summary=parsed.canonical_question,
            related_refs=[EntityRef(entity_type=EntityKind.RESEARCH_QUESTION, entity_id=question.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
            model_payload=ModelStagePayload(
                prompt_name="parse_research_question",
                raw_output=parsed.model_dump(mode="json"),
                output_refs=[EntityRef(entity_type=EntityKind.RESEARCH_QUESTION, entity_id=question.id)],
            ),
        )

    async def _handle_generate_hypotheses(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        proposal_set = await self.model_adapter.generate_hypotheses(question.canonical_question)
        refs: list[EntityRef] = []
        for proposal in proposal_set.hypotheses:
            hypothesis = Hypothesis(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                research_question_id=question.id,
                stage_run_id=stage_run.id,
                label=proposal.label,
                title=proposal.title,
                thesis=proposal.thesis,
                mechanism=proposal.mechanism,
                required_variables=proposal.required_variables,
                preferred_proxies=proposal.preferred_proxies,
                recommended_test_type=proposal.recommended_test_type.value if proposal.recommended_test_type is not None else None,
                expected_direction=proposal.expected_direction,
                target_assets=proposal.target_assets,
                explanatory_variables=proposal.explanatory_variables,
                caveats=[CaveatRecord(label="hypothesis_caveat", detail=item) for item in proposal.likely_caveats],
                confidence_level=proposal.confidence_level,
                novelty_usefulness_note=proposal.novelty_usefulness_note,
            )
            self.store.put(hypothesis)
            refs.append(EntityRef(entity_type=EntityKind.HYPOTHESIS, entity_id=hypothesis.id))
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "hypotheses", proposal_set.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "hypotheses_generated", "Hypotheses generated for user steering.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            refs[0],
            ProvenanceSourceType.LLM,
            "generate_hypotheses",
            None,
            prompt_name="generate_hypotheses",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Hypotheses Generated",
            summary=f"Generated {len(refs)} structured hypothesis cards.",
            related_refs=refs,
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_retrieve_evidence(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        evidence_set = await self.model_adapter.retrieve_evidence(question.canonical_question)
        refs: list[EntityRef] = []
        for proposal in evidence_set.evidence_items:
            evidence = EvidenceSource(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                stage_run_id=stage_run.id,
                provider=proposal.provider,
                source_type=ProvenanceSourceType.PUBLIC_RESEARCH,
                title=proposal.title,
                source=proposal.source,
                citation=proposal.citation,
                published_at=datetime.fromisoformat(f"{proposal.date}T00:00:00+00:00") if proposal.date else None,
                summary=proposal.short_claim_summary,
                methodology_summary=proposal.methodology_summary,
                data_used=proposal.data_used,
                relevance_to_hypothesis=proposal.relevance_to_hypothesis,
                evidence_stance=proposal.evidence_stance,
                extracted_claims=proposal.extracted_claims,
            )
            self.store.put(evidence)
            refs.append(EntityRef(entity_type=EntityKind.EVIDENCE_SOURCE, entity_id=evidence.id))
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "evidence", evidence_set.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "evidence_summary", "Evidence retrieval used structured stage output.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            refs[0],
            ProvenanceSourceType.LLM,
            "retrieve_evidence",
            None,
            prompt_name="retrieve_evidence",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Evidence Retrieved",
            summary=f"Retrieved {len(refs)} evidence items.",
            related_refs=refs,
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_discover_datasets(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        discovery = await self.model_adapter.discover_datasets(question.canonical_question)
        refs: list[EntityRef] = []
        for candidate in discovery.datasets:
            dataset = DatasetSource(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                stage_run_id=stage_run.id,
                provider=candidate.provider,
                external_id=candidate.external_id,
                name=candidate.name,
                description=candidate.description,
                dataset_kind=candidate.dataset_kind,
                entity_grain=candidate.entity_grain,
                time_grain=candidate.time_grain,
                frequency=candidate.frequency,
            )
            self.store.put(dataset)
            refs.append(EntityRef(entity_type=EntityKind.DATASET_SOURCE, entity_id=dataset.id))
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "datasets", discovery.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "dataset_discovery", "Candidate datasets discovered.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            refs[0],
            ProvenanceSourceType.LLM,
            "discover_datasets",
            None,
            prompt_name="discover_datasets",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Datasets Discovered",
            summary=f"Discovered {len(refs)} candidate datasets.",
            related_refs=refs,
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_profile_datasets(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        dataset_sources = self._entities_from_stage(branch.id, WorkflowStage.DISCOVER_DATASETS, DatasetSource)
        if not dataset_sources:
            raise StageFailure(
                "No datasets available to profile.",
                recovery_options=[RecoveryOption(action="rerun_stage", label="Rerun dataset discovery", description="Discover datasets before profiling.", target_stage=WorkflowStage.DISCOVER_DATASETS)],
            )
        profiles = await self.model_adapter.profile_datasets([item.external_id for item in dataset_sources])
        refs: list[EntityRef] = []
        for source in dataset_sources:
            matching = next((item for item in profiles.profiles if item.dataset_external_id == source.external_id), None)
            profile = DatasetProfile(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                dataset_source_id=source.id,
                stage_run_id=stage_run.id,
                row_count=matching.row_count if matching is not None else None,
                columns=matching.columns if matching is not None else [],
                key_candidates=matching.key_candidates if matching is not None else ["date"],
                quality_flags=matching.quality_flags if matching is not None else ["profile_missing"],
            )
            self.store.put(profile)
            refs.append(EntityRef(entity_type=EntityKind.DATASET_PROFILE, entity_id=profile.id))
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.DATASET_PREVIEW, "dataset_profiles", profiles.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "profiling_complete", "Dataset profiling completed.", WarningSeverity.INFO)
        provenance = self._create_provenance(branch, stage_run, refs[0], ProvenanceSourceType.SYSTEM, "profile_datasets", None)
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Datasets Profiled",
            summary=f"Profiled {len(refs)} datasets.",
            related_refs=refs,
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_propose_merge_plan(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        datasets = self._entities_from_stage(branch.id, WorkflowStage.DISCOVER_DATASETS, DatasetSource)
        profiles = self._entities_from_stage(branch.id, WorkflowStage.PROFILE_DATASETS, DatasetProfile)
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        hypotheses = self._entities_from_stage(branch.id, WorkflowStage.GENERATE_HYPOTHESES, Hypothesis)
        prior_test_plans = [item for item in self.store.snapshot.test_plans.values() if item.branch_id == branch.id]
        if len(datasets) < 2:
            raise StageFailure(
                "At least two datasets are required to propose a merge plan.",
                recovery_options=[RecoveryOption(action="edit_prompt", label="Add another dataset", description="Discover more datasets before merge planning.", target_stage=WorkflowStage.DISCOVER_DATASETS)],
            )
        dataset_by_id = {item.id: item for item in datasets}
        profile_by_dataset_id = {item.dataset_source_id: item for item in profiles}
        planner_profiles: list[MergePlannerDatasetProfile] = []
        for source in datasets:
            profile = profile_by_dataset_id.get(source.id)
            profile_columns = [item.name for item in (profile.columns if profile else [])]
            if not profile_columns:
                profile_columns = ["date", "value"]
                if source.provider == "yahoo_finance":
                    profile_columns = ["date", "symbol", "adjusted_close", "volume"]
                if source.provider == "sec_edgar":
                    profile_columns = ["filing_date", "accession_number", "cik"]
            planner_profiles.append(
                MergePlannerDatasetProfile(
                    dataset_external_id=source.external_id,
                    dataset_name=source.name,
                    provider=source.provider,
                    dataset_kind=source.dataset_kind,
                    frequency=source.frequency,
                    columns=profile_columns,
                    key_candidates=profile.key_candidates if profile else [],
                    quality_flags=profile.quality_flags if profile else [],
                )
            )
        planner_input = MergePlannerInput(
            research_question=self._research_question_to_plan(question),
            hypotheses=[item.thesis for item in hypotheses],
            requested_tests=[analysis.analysis_type.value for plan in prior_test_plans for analysis in plan.analyses],
            dataset_profiles=planner_profiles,
        )
        proposal = await self.model_adapter.propose_merge_plan(planner_input)
        source_id_by_external = {item.external_id: item.id for item in datasets}
        mappings = [
            MergeMapping(
                merge_plan_id=UUID(int=0),
                stage_run_id=stage_run.id,
                source_dataset_source_id=source_id_by_external.get(item.source_dataset_external_id),
                left_column=item.source_column,
                right_column=item.target_column,
                semantic_role=item.semantic_role,
                semantic_match_explanation=item.match_explanation,
                date_normalization_rule=item.date_normalization_rule,
                frequency_rule=item.frequency_rule,
                lag_rule=item.lag_rule,
                include_in_output=item.include_in_output,
                drop_reason=item.drop_reason,
                leakage_risk=item.leakage_risk,
                ambiguity_note=item.ambiguity_note,
                transforms=item.transforms,
                confidence=item.confidence,
                notes=item.notes,
            )
            for item in proposal.mappings
        ]
        selected_ids = [
            source_id_by_external[item.dataset_external_id]
            for item in proposal.chosen_datasets
            if item.dataset_external_id in source_id_by_external
        ]
        if len(selected_ids) < 2:
            selected_ids = [item.id for item in datasets[:2]]
        join_graph = [
            JoinEdge(
                left_dataset_source_id=source_id_by_external[item.left_dataset_external_id],
                right_dataset_source_id=source_id_by_external[item.right_dataset_external_id],
                join_type=item.join_type,
                join_keys=item.join_keys,
                left_time_column=item.left_time_column,
                right_time_column=item.right_time_column,
                confidence=item.confidence,
                rationale=item.rationale,
            )
            for item in proposal.join_graph
            if item.left_dataset_external_id in source_id_by_external
            and item.right_dataset_external_id in source_id_by_external
        ]
        merge_plan = MergePlan(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            left_dataset_source_id=selected_ids[0],
            right_dataset_source_id=selected_ids[1],
            output_name=proposal.output_name,
            chosen_dataset_source_ids=selected_ids,
            join_type=proposal.join_type,
            join_graph=join_graph,
            mappings=[],
            time_alignment_policy=proposal.time_alignment_policy,
            date_alignment_strategy=proposal.date_alignment_strategy,
            frequency_conversion_strategy=proposal.frequency_conversion_strategy,
            lag_policy=proposal.lag_policy,
            lag_assumption=proposal.lag_assumption,
            dropped_columns=[
                DroppedColumn(
                    dataset_source_id=source_id_by_external[item.dataset_external_id],
                    column=item.column,
                    reason=item.reason,
                    confidence=item.confidence,
                )
                for item in proposal.dropped_columns
                if item.dataset_external_id in source_id_by_external
            ],
            validation_checks=proposal.validation_checks,
            unresolved_ambiguities=proposal.unresolved_ambiguities,
            planner_warnings=proposal.warnings,
            ambiguity_notes=proposal.ambiguity_notes,
            confidence=proposal.confidence,
        )
        self.store.put(merge_plan)
        for mapping in mappings:
            mapping.merge_plan_id = merge_plan.id
        merge_plan.mappings = mappings
        self.store.put(merge_plan)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "merge_plan", proposal.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "merge_plan_proposed", "Merge plan proposed and awaiting review.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id),
            ProvenanceSourceType.LLM,
            "propose_merge_plan",
            None,
            prompt_name="propose_merge_plan",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Merge Plan Proposed",
            summary=proposal.lag_assumption,
            related_refs=[EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_await_merge_approval(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        merge_plan = self._entity_from_stage(branch.id, WorkflowStage.PROPOSE_MERGE_PLAN, MergePlan)
        warning = self._create_warning(branch, stage_run, "approval_required", "Merge plan approval required before canonical dataset build.", WarningSeverity.INFO)
        self.store.put(warning)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "merge_approval_request", {"merge_plan_id": str(merge_plan.id)})
        self.store.put(artifact)
        checkpoint = ApprovalCheckpoint(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            stage=stage_run.stage,
            requested_by="workflow_engine",
            request_notebook_entry_id=UUID(int=0),
            subject_refs=[EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id)],
            artifact_ref_ids=[artifact.id],
            notes="Review time alignment, lag assumption, and mappings.",
        )
        self.store.put(checkpoint)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id),
            ProvenanceSourceType.SYSTEM,
            "await_user_merge_approval",
            None,
        )
        self.store.put(provenance)
        return StageExecutionResult(
            title="Merge Approval Required",
            summary="Workflow paused for merge-plan approval.",
            related_refs=[EntityRef(entity_type=EntityKind.MERGE_PLAN, entity_id=merge_plan.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
            active_checkpoint_id=checkpoint.id,
        )

    async def _handle_build_canonical_dataset(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        merge_plan = self._approved_merge_plan(branch.id)
        hypotheses = self._entities_from_stage(branch.id, WorkflowStage.GENERATE_HYPOTHESES, Hypothesis)
        selected_hypothesis = self._select_hypothesis_for_canonical(hypotheses)
        included_mappings = [item for item in merge_plan.mappings if item.include_in_output]
        build_plan = await self.model_adapter.plan_canonical_dataset_build(
            CanonicalBuildInput(
                approved_merge_plan=self._merge_plan_to_proposal(merge_plan),
                selected_hypothesis=selected_hypothesis.thesis if selected_hypothesis else "No explicit hypothesis selected.",
                canonical_frequency="monthly" if "monthly" in (merge_plan.frequency_conversion_strategy or "").lower() else "daily",
                source_dataset_external_ids=[
                    self.store.snapshot.dataset_sources[source_id].external_id
                    for source_id in (merge_plan.chosen_dataset_source_ids or [merge_plan.left_dataset_source_id, merge_plan.right_dataset_source_id])
                    if source_id in self.store.snapshot.dataset_sources
                ],
                validation_requirements=merge_plan.validation_checks,
            )
        )
        mapped_rows, fetch_provenance, build_warnings, quality_report = await self._materialize_canonical_rows(
            branch=branch,
            stage_run=stage_run,
            merge_plan=merge_plan,
            build_plan=build_plan,
        )
        if not mapped_rows:
            raise StageFailure(
                "Canonical dataset build produced zero rows after merge/alignment.",
                recovery_options=[
                    RecoveryOption(
                        action="edit_merge_plan",
                        label="Adjust merge mappings",
                        description="Review join keys, lag policy, and source mappings to recover rows.",
                        target_stage=WorkflowStage.PROPOSE_MERGE_PLAN,
                    )
                ],
            )
        time_mappings = [item for item in included_mappings if item.semantic_role == "time_key"]
        entity_mappings = [item for item in included_mappings if item.semantic_role == "entity_key"]
        measure_mappings = [item for item in included_mappings if item.semantic_role == "measure"]
        attribute_mappings = [item for item in included_mappings if item.semantic_role == "attribute"]
        inferred_frequency = "monthly" if "monthly" in (build_plan.frequency_alignment or "").lower() else "daily"
        dataset = AnalysisDataset(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            merge_plan_id=merge_plan.id,
            dataset_kind=AnalysisDatasetKind.CANONICAL,
            name=merge_plan.output_name,
            grain="date",
            frequency=inferred_frequency,
            feature_columns=[item.right_column for item in measure_mappings + attribute_mappings],
            target_columns=[item.right_column for item in measure_mappings[:1]] or ["target_measure"],
            identifier_columns=[item.right_column for item in entity_mappings] or ["entity_id"],
            time_column=time_mappings[0].right_column if time_mappings else "timestamp",
            upstream_dataset_source_ids=merge_plan.chosen_dataset_source_ids or [merge_plan.left_dataset_source_id, merge_plan.right_dataset_source_id],
            row_count=len(mapped_rows),
        )
        artifact = self._write_json_artifact(
            branch,
            stage_run,
            ArtifactKind.DATASET_FILE,
            "canonical_dataset",
            {
                "dataset_name": dataset.name,
                "kind": dataset.dataset_kind.value,
                "merge_plan_id": str(merge_plan.id),
                "selected_hypothesis_id": str(selected_hypothesis.id) if selected_hypothesis else None,
                "join_graph": [edge.model_dump(mode="json") for edge in merge_plan.join_graph],
                "included_mappings": [item.model_dump(mode="json") for item in included_mappings],
                "dropped_columns": [item.model_dump(mode="json") for item in merge_plan.dropped_columns],
                "date_alignment_strategy": build_plan.timestamp_normalization,
                "frequency_conversion_strategy": build_plan.frequency_alignment,
                "lag_policy": build_plan.lag_policy,
                "derived_fields": [item.model_dump(mode="json") for item in build_plan.derived_fields],
                "rows": mapped_rows,
            },
        )
        preview_artifact = self._write_json_artifact(
            branch,
            stage_run,
            ArtifactKind.DATASET_PREVIEW,
            "canonical_dataset_preview",
            {"dataset_name": dataset.name, "row_count": len(mapped_rows), "rows": mapped_rows[:25]},
        )
        quality_artifact = self._write_json_artifact(
            branch,
            stage_run,
            ArtifactKind.REPORT,
            "canonical_dataset_quality_report",
            quality_report,
        )
        provenance_bundle_artifact = self._write_json_artifact(
            branch,
            stage_run,
            ArtifactKind.LOG,
            "canonical_dataset_provenance_bundle",
            {
                "dataset_name": dataset.name,
                "selected_hypothesis": selected_hypothesis.model_dump(mode="json") if selected_hypothesis else None,
                "fetch_provenance": fetch_provenance,
                "build_plan": build_plan.model_dump(mode="json"),
                "quality_report_summary": quality_report,
            },
        )
        dataset.materialized_artifact_ref_id = artifact.id
        for artifact_ref in [artifact, preview_artifact, quality_artifact, provenance_bundle_artifact]:
            self.store.put(artifact_ref)
        self.store.put(dataset)
        warning_text = "Canonical dataset materialized from approved merge plan."
        if merge_plan.planner_warnings:
            warning_text += f" Planner warnings: {'; '.join(merge_plan.planner_warnings[:2])}."
        if build_warnings:
            warning_text += f" Build warnings: {'; '.join(build_warnings[:2])}."
        warning = self._create_warning(branch, stage_run, "canonical_dataset_built", warning_text, WarningSeverity.INFO)
        system_provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id),
            ProvenanceSourceType.SYSTEM,
            "build_canonical_dataset",
            None,
        )
        planning_provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id),
            ProvenanceSourceType.LLM,
            "canonical_dataset_builder_plan",
            None,
            prompt_name="canonical_dataset_builder",
            prompt_version="v1",
        )
        for record in [warning, system_provenance, planning_provenance]:
            self.store.put(record)
        provenance_ids = [system_provenance.id, planning_provenance.id]
        for record in fetch_provenance:
            provenance = self._create_provenance(
                branch=branch,
                stage_run=stage_run,
                subject_ref=EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id),
                source_type=ProvenanceSourceType.DATA_API,
                source_label=record["source_label"],
                source_uri=record.get("source_uri"),
            )
            self.store.put(provenance)
            provenance_ids.append(provenance.id)
        return StageExecutionResult(
            title="Canonical Dataset Built",
            summary=f"Built canonical dataset `{dataset.name}`.",
            related_refs=[EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id)],
            warning_ids=[warning.id],
            provenance_ids=provenance_ids,
            artifact_ref_ids=[artifact.id, preview_artifact.id, quality_artifact.id, provenance_bundle_artifact.id],
        )

    async def _handle_propose_test_plan(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        canonical_dataset = self._entity_from_stage(branch.id, WorkflowStage.BUILD_CANONICAL_DATASET, AnalysisDataset)
        hypotheses = self._entities_from_stage(branch.id, WorkflowStage.GENERATE_HYPOTHESES, Hypothesis)
        proposal = await self.model_adapter.propose_test_plan(question.canonical_question)
        analyses = [
            AnalysisSpec(
                analysis_type=item.analysis_type,
                title=item.title,
                objective=item.objective,
                dependent_variable=item.dependent_variable,
                independent_variables=item.independent_variables,
                parameters=item.parameters,
            )
            for item in proposal.analyses
        ]
        test_plan = TestPlan(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            canonical_dataset_id=canonical_dataset.id,
            selected_hypothesis_ids=[item.id for item in hypotheses],
            title=proposal.title,
            objective=proposal.objective,
            analyses=analyses,
            caveats=proposal.caveats,
        )
        self.store.put(test_plan)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "test_plan", proposal.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "test_plan_proposed", "Test plan proposed and awaiting analyst approval.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.TEST_PLAN, entity_id=test_plan.id),
            ProvenanceSourceType.LLM,
            "propose_test_plan",
            None,
            prompt_name="propose_test_plan",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Test Plan Proposed",
            summary=test_plan.objective,
            related_refs=[EntityRef(entity_type=EntityKind.TEST_PLAN, entity_id=test_plan.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_await_test_approval(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        test_plan = self._entity_from_stage(branch.id, WorkflowStage.PROPOSE_TEST_PLAN, TestPlan)
        warning = self._create_warning(branch, stage_run, "approval_required", "Test plan approval required before materialization and execution.", WarningSeverity.INFO)
        self.store.put(warning)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.MODEL_OUTPUT, "test_approval_request", {"test_plan_id": str(test_plan.id)})
        self.store.put(artifact)
        checkpoint = ApprovalCheckpoint(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            stage=stage_run.stage,
            requested_by="workflow_engine",
            request_notebook_entry_id=UUID(int=0),
            subject_refs=[EntityRef(entity_type=EntityKind.TEST_PLAN, entity_id=test_plan.id)],
            artifact_ref_ids=[artifact.id],
            notes="Confirm analyses, parameters, and caveats.",
        )
        self.store.put(checkpoint)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.TEST_PLAN, entity_id=test_plan.id),
            ProvenanceSourceType.SYSTEM,
            "await_user_test_approval",
            None,
        )
        self.store.put(provenance)
        return StageExecutionResult(
            title="Test Approval Required",
            summary="Workflow paused for test-plan approval.",
            related_refs=[EntityRef(entity_type=EntityKind.TEST_PLAN, entity_id=test_plan.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
            active_checkpoint_id=checkpoint.id,
        )

    async def _handle_materialize_analysis_dataset(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        canonical_dataset = self._entity_from_stage(branch.id, WorkflowStage.BUILD_CANONICAL_DATASET, AnalysisDataset)
        test_plan = self._entity_from_stage(branch.id, WorkflowStage.PROPOSE_TEST_PLAN, TestPlan)
        dataset = AnalysisDataset(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            merge_plan_id=canonical_dataset.merge_plan_id,
            dataset_kind=AnalysisDatasetKind.MATERIALIZED,
            test_plan_id=test_plan.id,
            name=f"{canonical_dataset.name}_materialized",
            grain=canonical_dataset.grain,
            frequency=canonical_dataset.frequency,
            feature_columns=canonical_dataset.feature_columns,
            target_columns=canonical_dataset.target_columns,
            identifier_columns=canonical_dataset.identifier_columns,
            time_column=canonical_dataset.time_column,
            upstream_dataset_source_ids=canonical_dataset.upstream_dataset_source_ids,
        )
        artifact = self._write_json_artifact(
            branch,
            stage_run,
            ArtifactKind.DATASET_FILE,
            "analysis_dataset",
            {"dataset_name": dataset.name, "test_plan_id": str(test_plan.id)},
        )
        dataset.materialized_artifact_ref_id = artifact.id
        self.store.put(artifact)
        self.store.put(dataset)
        warning = self._create_warning(branch, stage_run, "analysis_dataset_materialized", "Materialized the approved analysis dataset.", WarningSeverity.INFO)
        provenance = self._create_provenance(branch, stage_run, EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id), ProvenanceSourceType.SYSTEM, "materialize_analysis_dataset", None)
        for record in [warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Analysis Dataset Materialized",
            summary=f"Materialized dataset `{dataset.name}` for execution.",
            related_refs=[EntityRef(entity_type=EntityKind.ANALYSIS_DATASET, entity_id=dataset.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    async def _handle_execute_analysis(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        dataset = self._entity_from_stage(branch.id, WorkflowStage.MATERIALIZE_ANALYSIS_DATASET, AnalysisDataset)
        test_plan = self._entity_from_stage(branch.id, WorkflowStage.PROPOSE_TEST_PLAN, TestPlan)
        result_refs: list[EntityRef] = []
        artifact_refs: list[UUID] = []
        warning_ids: list[UUID] = []
        provenance_ids: list[UUID] = []

        for analysis in test_plan.analyses:
            run = AnalysisRun(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                stage_run_id=stage_run.id,
                test_plan_id=test_plan.id,
                analysis_dataset_id=dataset.id,
                analysis_type=analysis.analysis_type,
                config=analysis.parameters,
                status=StageRunStatus.COMPLETED,
                completed_at=utc_now(),
                reproducibility=ReproducibilityMetadata(
                    workflow_version="1.0",
                    executor_name="analysis_engine",
                    environment=get_settings().env,
                ),
            )
            self.store.put(run)
            artifact = self._write_json_artifact(
                branch,
                stage_run,
                ArtifactKind.RESULT_BUNDLE,
                f"analysis_{analysis.analysis_type.value}",
                {
                    "analysis_type": analysis.analysis_type.value,
                    "objective": analysis.objective,
                    "dataset_id": str(dataset.id),
                },
            )
            result = ResultArtifact(
                investigation_id=branch.investigation_id,
                branch_id=branch.id,
                stage_run_id=stage_run.id,
                analysis_run_id=run.id,
                artifact_ref_id=artifact.id,
                artifact_type=self._artifact_type_for_analysis(analysis.analysis_type),
                title=analysis.title,
                description=analysis.objective,
                metric_summary={"status": "placeholder_complete"},
            )
            run.result_artifact_ids.append(result.id)
            self.store.put(artifact)
            self.store.put(result)
            self.store.put(run)

            warning = self._create_warning(branch, stage_run, "analysis_complete", f"Completed {analysis.analysis_type.value}.", WarningSeverity.INFO)
            provenance = self._create_provenance(branch, stage_run, EntityRef(entity_type=EntityKind.ANALYSIS_RUN, entity_id=run.id), ProvenanceSourceType.ANALYSIS_ENGINE, analysis.analysis_type.value, None)
            self.store.put(warning)
            self.store.put(provenance)

            result_refs.extend(
                [
                    EntityRef(entity_type=EntityKind.ANALYSIS_RUN, entity_id=run.id),
                    EntityRef(entity_type=EntityKind.RESULT_ARTIFACT, entity_id=result.id),
                ]
            )
            artifact_refs.append(artifact.id)
            warning_ids.append(warning.id)
            provenance_ids.append(provenance.id)

        if not test_plan.analyses:
            raise StageFailure(
                "No analyses were defined in the approved test plan.",
                recovery_options=[RecoveryOption(action="edit_test_plan", label="Edit test plan", description="Add at least one analysis to the test plan.", target_stage=WorkflowStage.PROPOSE_TEST_PLAN)],
            )
        return StageExecutionResult(
            title="Analysis Executed",
            summary=f"Executed {len(test_plan.analyses)} approved analyses.",
            related_refs=result_refs,
            warning_ids=warning_ids,
            provenance_ids=provenance_ids,
            artifact_ref_ids=artifact_refs,
        )

    async def _handle_summarize_results(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        summary = await self.model_adapter.summarize_results(question.canonical_question)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.REPORT, "result_summary", summary.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "results_summarized", "Results summary generated.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=stage_run.id),
            ProvenanceSourceType.LLM,
            "summarize_results",
            None,
            prompt_name="summarize_results",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Results Summarized",
            summary=summary.summary,
            related_refs=[EntityRef(entity_type=EntityKind.ARTIFACT_REF, entity_id=artifact.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
            model_payload=ModelStagePayload(
                prompt_name="summarize_results",
                raw_output=summary.model_dump(mode="json"),
                output_refs=[EntityRef(entity_type=EntityKind.ARTIFACT_REF, entity_id=artifact.id)],
            ),
        )

    async def _handle_propose_next_steps(self, branch: Branch, stage_run: StageRun) -> StageExecutionResult:
        question = self._entity_from_stage(branch.id, WorkflowStage.PARSE_RESEARCH_QUESTION, ResearchQuestion)
        next_steps = await self.model_adapter.propose_next_steps(question.canonical_question)
        artifact = self._write_json_artifact(branch, stage_run, ArtifactKind.REPORT, "next_steps", next_steps.model_dump(mode="json"))
        warning = self._create_warning(branch, stage_run, "next_steps_ready", "Next-step proposals generated for branch steering.", WarningSeverity.INFO)
        provenance = self._create_provenance(
            branch,
            stage_run,
            EntityRef(entity_type=EntityKind.ARTIFACT_REF, entity_id=artifact.id),
            ProvenanceSourceType.LLM,
            "propose_next_steps",
            None,
            prompt_name="propose_next_steps",
            prompt_version="v1",
        )
        for record in [artifact, warning, provenance]:
            self.store.put(record)
        return StageExecutionResult(
            title="Next Steps Proposed",
            summary=next_steps.summary,
            related_refs=[EntityRef(entity_type=EntityKind.ARTIFACT_REF, entity_id=artifact.id)],
            warning_ids=[warning.id],
            provenance_ids=[provenance.id],
            artifact_ref_ids=[artifact.id],
        )

    def _approved_merge_plan(self, branch_id: UUID) -> MergePlan:
        merge_plan = self._entity_from_stage(branch_id, WorkflowStage.PROPOSE_MERGE_PLAN, MergePlan)
        if merge_plan.approved_by_checkpoint_id is None:
            raise StageFailure(
                "Merge plan has not been approved.",
                recovery_options=[RecoveryOption(action="approve_merge_plan", label="Resolve merge approval", description="Approve or reject the pending merge checkpoint.", target_stage=WorkflowStage.AWAIT_USER_MERGE_APPROVAL)],
            )
        checkpoint = self.store.snapshot.approval_checkpoints[merge_plan.approved_by_checkpoint_id]
        if checkpoint.status != ApprovalStatus.APPROVED:
            raise StageFailure(
                "Merge plan approval is not in approved state.",
                recovery_options=[RecoveryOption(action="resolve_merge_approval", label="Resolve merge approval", description="Approve the merge plan before continuing.", target_stage=WorkflowStage.AWAIT_USER_MERGE_APPROVAL)],
            )
        return merge_plan

    def _research_question_to_plan(self, question: ResearchQuestion) -> ResearchQuestionPlan:
        return ResearchQuestionPlan(
            canonical_question=question.canonical_question,
            market_universe=question.market_universe,
            benchmark=question.benchmark,
            horizon=question.horizon,
            frequency=question.frequency,
            unit_of_analysis=question.unit_of_analysis,
            success_criteria=question.success_criteria,
            caveats=question.caveats,
        )

    def _entity_from_stage(self, branch_id: UUID, stage: WorkflowStage, expected_type: type[Any]) -> Any:
        entities = self._entities_from_stage(branch_id, stage, expected_type)
        if not entities:
            raise StageFailure(
                f"No {expected_type.__name__} output found from stage {stage.value}.",
                recovery_options=[RecoveryOption(action="rerun_stage", label=f"Rerun {stage.value}", description="Regenerate the missing upstream outputs.", target_stage=stage)],
            )
        return entities[0]

    def _entities_from_stage(self, branch_id: UUID, stage: WorkflowStage, expected_type: type[Any]) -> list[Any]:
        run = self._latest_effective_stage_run(branch_id, stage)
        values: list[Any] = []
        mapping_by_kind = {
            EntityKind.RESEARCH_QUESTION: self.store.snapshot.research_questions,
            EntityKind.HYPOTHESIS: self.store.snapshot.hypotheses,
            EntityKind.EVIDENCE_SOURCE: self.store.snapshot.evidence_sources,
            EntityKind.DATASET_SOURCE: self.store.snapshot.dataset_sources,
            EntityKind.DATASET_PROFILE: self.store.snapshot.dataset_profiles,
            EntityKind.MERGE_PLAN: self.store.snapshot.merge_plans,
            EntityKind.ANALYSIS_DATASET: self.store.snapshot.analysis_datasets,
            EntityKind.TEST_PLAN: self.store.snapshot.test_plans,
            EntityKind.ANALYSIS_RUN: self.store.snapshot.analysis_runs,
            EntityKind.RESULT_ARTIFACT: self.store.snapshot.result_artifacts,
            EntityKind.ARTIFACT_REF: self.store.snapshot.artifact_refs,
        }
        for ref in run.output_refs:
            mapping = mapping_by_kind.get(ref.entity_type)
            if mapping is None:
                continue
            value = mapping.get(ref.entity_id)
            if value is not None and isinstance(value, expected_type):
                values.append(value)
        return values

    def _select_hypothesis_for_canonical(self, hypotheses: list[Hypothesis]) -> Hypothesis | None:
        if not hypotheses:
            return None
        selected = [item for item in hypotheses if item.status.value == "selected"]
        pool = selected or hypotheses
        return sorted(
            pool,
            key=lambda item: (item.priority_score or 0.0, item.confidence_level or 0.0),
            reverse=True,
        )[0]

    def _merge_plan_to_proposal(self, merge_plan: MergePlan) -> MergePlanProposal:
        return MergePlanProposal(
            output_name=merge_plan.output_name,
            chosen_datasets=[
                {
                    "dataset_external_id": self.store.snapshot.dataset_sources[source_id].external_id,
                    "role": "selected",
                    "reason": "Approved merge-plan dataset.",
                    "confidence": merge_plan.confidence,
                }
                for source_id in (merge_plan.chosen_dataset_source_ids or [merge_plan.left_dataset_source_id, merge_plan.right_dataset_source_id])
                if source_id in self.store.snapshot.dataset_sources
            ],
            join_graph=[
                {
                    "left_dataset_external_id": self.store.snapshot.dataset_sources[edge.left_dataset_source_id].external_id,
                    "right_dataset_external_id": self.store.snapshot.dataset_sources[edge.right_dataset_source_id].external_id,
                    "join_type": edge.join_type.value,
                    "join_keys": edge.join_keys,
                    "left_time_column": edge.left_time_column,
                    "right_time_column": edge.right_time_column,
                    "confidence": edge.confidence,
                    "rationale": edge.rationale,
                }
                for edge in merge_plan.join_graph
                if edge.left_dataset_source_id in self.store.snapshot.dataset_sources
                and edge.right_dataset_source_id in self.store.snapshot.dataset_sources
            ],
            join_type=merge_plan.join_type,
            time_alignment_policy=merge_plan.time_alignment_policy,
            date_alignment_strategy=merge_plan.date_alignment_strategy or "",
            frequency_conversion_strategy=merge_plan.frequency_conversion_strategy or "",
            lag_policy=merge_plan.lag_policy or merge_plan.lag_assumption,
            lag_assumption=merge_plan.lag_assumption,
            mappings=[
                {
                    "source_dataset_external_id": self.store.snapshot.dataset_sources[item.source_dataset_source_id].external_id
                    if item.source_dataset_source_id in self.store.snapshot.dataset_sources
                    else "unknown",
                    "source_column": item.left_column,
                    "target_column": item.right_column,
                    "semantic_role": item.semantic_role,
                    "match_explanation": item.semantic_match_explanation or "Approved mapping.",
                    "date_normalization_rule": item.date_normalization_rule,
                    "frequency_rule": item.frequency_rule,
                    "lag_rule": item.lag_rule,
                    "include_in_output": item.include_in_output,
                    "drop_reason": item.drop_reason,
                    "leakage_risk": item.leakage_risk,
                    "ambiguity_note": item.ambiguity_note,
                    "transforms": [transform.model_dump(mode="json") for transform in item.transforms],
                    "confidence": item.confidence,
                    "notes": item.notes,
                }
                for item in merge_plan.mappings
            ],
            dropped_columns=[
                {
                    "dataset_external_id": self.store.snapshot.dataset_sources[item.dataset_source_id].external_id,
                    "column": item.column,
                    "reason": item.reason,
                    "confidence": item.confidence,
                }
                for item in merge_plan.dropped_columns
                if item.dataset_source_id in self.store.snapshot.dataset_sources
            ],
            unresolved_ambiguities=merge_plan.unresolved_ambiguities,
            warnings=merge_plan.planner_warnings,
            ambiguity_notes=merge_plan.ambiguity_notes,
            validation_checks=merge_plan.validation_checks,
            confidence=merge_plan.confidence,
        )

    async def _materialize_canonical_rows(
        self,
        branch: Branch,
        stage_run: StageRun,
        merge_plan: MergePlan,
        build_plan: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str], dict[str, Any]]:
        source_ids = merge_plan.chosen_dataset_source_ids or [merge_plan.left_dataset_source_id, merge_plan.right_dataset_source_id]
        source_records = [self.store.snapshot.dataset_sources[item] for item in source_ids if item in self.store.snapshot.dataset_sources]
        registry = AdapterRegistry()
        source_rows: dict[UUID, list[dict[str, Any]]] = {}
        fetch_provenance: list[dict[str, str]] = []
        build_warnings: list[str] = []
        for source in source_records:
            adapter = registry.get(source.provider)
            try:
                result = await adapter.fetch(source.external_id)
                rows = self._rows_from_fetch_result(result)
                source_rows[source.id] = rows
                if not rows:
                    build_warnings.append(f"No rows fetched for {source.provider}:{source.external_id}.")
                fetch_provenance.append({"source_label": f"{source.provider}:{source.external_id}", "source_uri": source.source_url or ""})
            except Exception as exc:
                build_warnings.append(f"Fetch failed for {source.provider}:{source.external_id} ({exc}).")
                source_rows[source.id] = []
        if not source_rows:
            return [], fetch_provenance, build_warnings, {"row_count": 0, "error": "no_sources_loaded"}

        mapped_by_source = {source_id: self._apply_mapping_bundle(rows, merge_plan, source_id) for source_id, rows in source_rows.items()}
        base_source_id = source_ids[0]
        joined_rows = mapped_by_source.get(base_source_id, [])
        for edge in merge_plan.join_graph:
            if edge.left_dataset_source_id != base_source_id:
                continue
            right_rows = mapped_by_source.get(edge.right_dataset_source_id, [])
            joined_rows = self._join_rows(joined_rows, right_rows, edge.join_keys, edge.join_type.value)
        normalized_rows = [self._normalize_row_time(item, build_plan.timestamp_normalization) for item in joined_rows]
        lagged_rows = [self._apply_row_lag(item, build_plan.lag_policy) for item in normalized_rows]
        aligned_rows = self._align_frequency(lagged_rows, build_plan.frequency_alignment)
        enriched_rows = self._apply_derived_fields(aligned_rows, build_plan.derived_fields)
        if not enriched_rows:
            enriched_rows = [self._placeholder_row_from_mappings(merge_plan)]
            build_warnings.append("Fell back to placeholder canonical row because raw fetch/join returned no rows.")
        quality_report = self._build_quality_report(enriched_rows, merge_plan, build_plan)
        return enriched_rows, fetch_provenance, build_warnings, quality_report

    def _rows_from_fetch_result(self, result: Any) -> list[dict[str, Any]]:
        if result.dataset is not None:
            rows = []
            for observation in result.dataset.observations:
                row = observation.model_dump(mode="json", exclude_none=True)
                extra = row.pop("extra_fields", {}) or {}
                row.update(extra)
                rows.append(row)
            return rows
        if result.filing is not None:
            filing = result.filing
            return [{"accession_number": filing.accession_number, "form": filing.form, "filing_date": filing.filing_date.isoformat(), "cik": filing.cik}]
        return []

    def _apply_mapping_bundle(self, rows: list[dict[str, Any]], merge_plan: MergePlan, source_id: UUID) -> list[dict[str, Any]]:
        mappings = [item for item in merge_plan.mappings if item.include_in_output and item.source_dataset_source_id == source_id]
        output: list[dict[str, Any]] = []
        for row in rows:
            updated = dict(row)
            for mapping in mappings:
                if mapping.left_column in row:
                    updated[mapping.right_column] = row.get(mapping.left_column)
            output.append(updated)
        return output

    def _join_rows(self, left_rows: list[dict[str, Any]], right_rows: list[dict[str, Any]], join_keys: list[str], join_type: str) -> list[dict[str, Any]]:
        if not left_rows:
            return []
        if not right_rows:
            return left_rows if join_type in {"left", "asof"} else []
        time_keys = [item for item in join_keys if "date" in item.lower() or "time" in item.lower()]
        entity_keys = [item for item in join_keys if item not in time_keys]
        right_index: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in right_rows:
            key = tuple(row.get(item) for item in entity_keys)
            right_index.setdefault(key, []).append(row)
        for values in right_index.values():
            values.sort(key=lambda item: self._row_date(item, time_keys[0] if time_keys else None) or date.min)
        output: list[dict[str, Any]] = []
        for left in left_rows:
            key = tuple(left.get(item) for item in entity_keys)
            candidates = right_index.get(key, [])
            selected: dict[str, Any] | None = None
            if join_type == "asof" and time_keys:
                left_time = self._row_date(left, time_keys[0])
                if left_time is not None:
                    prior = [item for item in candidates if (self._row_date(item, time_keys[0]) or date.min) <= left_time]
                    selected = prior[-1] if prior else None
            elif time_keys:
                selected = next((item for item in candidates if self._row_date(item, time_keys[0]) == self._row_date(left, time_keys[0])), None)
            elif candidates:
                selected = candidates[0]
            if selected is None and join_type == "inner":
                continue
            merged = dict(left)
            if selected is not None:
                for col, value in selected.items():
                    merged.setdefault(col, value)
            output.append(merged)
        return output

    def _row_date(self, row: dict[str, Any], key: str | None) -> date | None:
        value = row.get(key) if key else row.get("date") or row.get("timestamp")
        if value is None:
            return None
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
        except ValueError:
            return None

    def _normalize_row_time(self, row: dict[str, Any], _rule: str) -> dict[str, Any]:
        normalized = dict(row)
        for key in ["timestamp", "date", "filing_date"]:
            parsed = self._row_date(normalized, key)
            if parsed is not None:
                normalized["timestamp"] = parsed.isoformat()
                break
        return normalized

    def _apply_row_lag(self, row: dict[str, Any], lag_policy: str) -> dict[str, Any]:
        if "lag" not in lag_policy.lower():
            return row
        timestamp = row.get("timestamp")
        if timestamp is None:
            return row
        try:
            lagged = datetime.fromisoformat(str(timestamp)).date() + timedelta(days=1)
        except ValueError:
            return row
        output = dict(row)
        output["timestamp"] = lagged.isoformat()
        return output

    def _align_frequency(self, rows: list[dict[str, Any]], frequency_alignment: str) -> list[dict[str, Any]]:
        if "monthly" not in frequency_alignment.lower():
            return rows
        by_month: dict[str, dict[str, Any]] = {}
        for row in rows:
            timestamp = row.get("timestamp") or row.get("date")
            if timestamp is None:
                continue
            by_month[str(timestamp)[:7]] = row
        return [by_month[item] for item in sorted(by_month.keys())]

    def _apply_derived_fields(self, rows: list[dict[str, Any]], derived_fields: list[TransformSpec]) -> list[dict[str, Any]]:
        if not derived_fields:
            return rows
        output = [dict(item) for item in rows]
        for transform in derived_fields:
            if not transform.source_columns:
                continue
            source_col = transform.source_columns[0]
            for row in output:
                value = row.get(source_col)
                if value is None:
                    continue
                target_col = str(transform.parameters.get("target_column", f"{source_col}_{transform.operation}"))
                if transform.operation == "copy":
                    row[target_col] = value
                if transform.operation == "scale" and isinstance(value, (int, float)):
                    factor = float(transform.parameters.get("factor", 1.0))
                    row[target_col] = float(value) * factor
        return output

    def _build_quality_report(self, rows: list[dict[str, Any]], merge_plan: MergePlan, build_plan: Any) -> dict[str, Any]:
        columns = sorted({key for row in rows for key in row.keys()})
        null_fraction = {
            column: (sum(1 for row in rows if row.get(column) is None) / len(rows)) if rows else 1.0
            for column in columns
        }
        return {
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": columns,
            "null_fraction_by_column": null_fraction,
            "leakage_warnings": [item.leakage_risk for item in merge_plan.mappings if item.leakage_risk],
            "checks_requested": {"leakage": list(build_plan.leakage_checks), "quality": list(build_plan.quality_checks)},
        }

    def _placeholder_row_from_mappings(self, merge_plan: MergePlan) -> dict[str, Any]:
        row: dict[str, Any] = {"timestamp": utc_now().date().isoformat()}
        for mapping in merge_plan.mappings:
            if not mapping.include_in_output:
                continue
            if mapping.semantic_role == "time_key":
                row[mapping.right_column] = row["timestamp"]
            elif mapping.semantic_role == "entity_key":
                row[mapping.right_column] = "placeholder_entity"
            else:
                row[mapping.right_column] = 0.0
        return row

    def _write_json_artifact(
        self,
        branch: Branch,
        stage_run: StageRun,
        artifact_kind: ArtifactKind,
        role: str,
        payload: dict[str, Any],
    ) -> ArtifactRef:
        relative_path = f"workflow/{branch.id}/{stage_run.stage.value}/attempt_{stage_run.attempt}_{role}.json"
        path, checksum, byte_size = self.artifact_store.write_json(relative_path, payload)
        artifact = ArtifactRef(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            artifact_kind=artifact_kind,
            role=role,
            uri=str(path),
            storage_backend="local",
            mime_type="application/json",
            checksum_sha256=checksum,
            byte_size=byte_size,
        )
        self.store.put(artifact)
        return artifact

    def _create_warning(
        self,
        branch: Branch,
        stage_run: StageRun,
        code: str,
        message: str,
        severity: WarningSeverity,
        mitigation: str | None = None,
    ) -> Warning:
        return Warning(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            subject_ref=EntityRef(entity_type=EntityKind.STAGE_RUN, entity_id=stage_run.id),
            severity=severity,
            code=code,
            message=message,
            mitigation=mitigation,
        )

    def _create_provenance(
        self,
        branch: Branch,
        stage_run: StageRun,
        subject_ref: EntityRef,
        source_type: ProvenanceSourceType,
        source_label: str,
        source_uri: str | None,
        prompt_name: str | None = None,
        prompt_version: str | None = None,
    ) -> ProvenanceRecord:
        return ProvenanceRecord(
            investigation_id=branch.investigation_id,
            branch_id=branch.id,
            stage_run_id=stage_run.id,
            subject_ref=subject_ref,
            source_type=source_type,
            source_label=source_label,
            source_uri=source_uri,
            model_name=get_settings().openai_model if prompt_name is not None else None,
            prompt_name=prompt_name,
            prompt_version=prompt_version,
            input_fingerprint=stage_run.output_reproducibility.input_fingerprint,
        )

    def _artifact_type_for_analysis(self, analysis_type) -> Any:
        if analysis_type.value in {"correlation_summary", "linear_regression", "rolling_regression", "regime_split"}:
            from domain.enums import ResultArtifactType

            return ResultArtifactType.TABLE
        from domain.enums import ResultArtifactType

        return ResultArtifactType.CHART

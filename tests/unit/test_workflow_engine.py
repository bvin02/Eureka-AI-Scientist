import asyncio

from domain.enums import ApprovalStatus, StageRunStatus, WorkflowStage
from data.models import AdapterFetchResult, CanonicalDataset, CanonicalDatasetMetadata, CanonicalObservation, ProvenancePayload
from domain.models import TimeCoverage
from orchestration.contracts import DatasetCandidate, DatasetDiscoverySet
import orchestration.engine as engine_module
from orchestration.engine import WorkflowEngine
from orchestration.model_adapter import DeterministicWorkflowModelAdapter
from orchestration.models import ApprovalResolution, BranchForkRequest, UserEditRequest


class SingleDatasetAdapter(DeterministicWorkflowModelAdapter):
    async def discover_datasets(self, canonical_question: str) -> DatasetDiscoverySet:
        return DatasetDiscoverySet(
            datasets=[
                DatasetCandidate(
                    provider="fred",
                    external_id="DFII10",
                    name="10Y Real Yield",
                    description=f"Only one dataset for {canonical_question}",
                    dataset_kind="macro_series",
                    entity_grain="market",
                    time_grain="date",
                    frequency="daily",
                )
            ]
        )


def create_engine(model_adapter=None) -> WorkflowEngine:
    return WorkflowEngine(model_adapter=model_adapter or DeterministicWorkflowModelAdapter())


def test_workflow_runs_until_first_approval_and_serializes() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    result = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))

    branch_state = next(item for item in result.state.branch_states if item.branch_id == result.state.current_branch_id)
    assert result.state.current_stage == WorkflowStage.AWAIT_USER_MERGE_APPROVAL
    assert branch_state.pending_approval_checkpoint_id is not None
    assert len(result.executed_stage_runs) == 8
    assert len(result.emitted_notebook_entry_ids) == 8

    for stage_run_id in result.executed_stage_runs:
        stage_run = engine.store.snapshot.stage_runs[stage_run_id]
        assert stage_run.warning_ids
        assert stage_run.provenance_ids
        assert stage_run.artifact_ref_ids
        assert stage_run.notebook_entry_ids
        assert stage_run.output_reproducibility.input_fingerprint is not None

    restored = WorkflowEngine.from_snapshot_json(engine.export_snapshot())
    restored_state = restored.get_state(state.investigation_id, state.current_branch_id)
    restored_branch_state = next(item for item in restored_state.branch_states if item.branch_id == restored_state.current_branch_id)
    assert restored_branch_state.pending_approval_checkpoint_id == branch_state.pending_approval_checkpoint_id


def test_approval_resolution_resumes_and_advances_to_test_approval() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    first_block = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    first_branch_state = next(item for item in first_block.state.branch_states if item.branch_id == first_block.state.current_branch_id)

    approved_state = engine.resolve_approval(
        ApprovalResolution(
            checkpoint_id=first_branch_state.pending_approval_checkpoint_id,
            status=ApprovalStatus.APPROVED,
            actor_label="analyst",
            rationale="Merge assumptions accepted.",
        )
    )

    resumed = asyncio.run(engine.run_until_blocked(approved_state.investigation_id, approved_state.current_branch_id))
    resumed_branch_state = next(item for item in resumed.state.branch_states if item.branch_id == resumed.state.current_branch_id)
    assert resumed.state.current_stage == WorkflowStage.AWAIT_USER_TEST_APPROVAL
    assert resumed_branch_state.pending_approval_checkpoint_id is not None


def test_user_edit_invalidates_only_downstream_stage_runs() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    first_block = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    first_checkpoint = next(item for item in first_block.state.branch_states if item.branch_id == state.current_branch_id).pending_approval_checkpoint_id
    state = engine.resolve_approval(
        ApprovalResolution(
            checkpoint_id=first_checkpoint,
            status=ApprovalStatus.APPROVED,
            actor_label="analyst",
        )
    )
    second_block = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    second_checkpoint = next(item for item in second_block.state.branch_states if item.branch_id == state.current_branch_id).pending_approval_checkpoint_id
    state = engine.resolve_approval(
        ApprovalResolution(
            checkpoint_id=second_checkpoint,
            status=ApprovalStatus.APPROVED,
            actor_label="analyst",
        )
    )
    asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))

    updated_state = engine.record_user_edit(
        UserEditRequest(
            branch_id=state.current_branch_id,
            anchor_stage=WorkflowStage.PROPOSE_TEST_PLAN,
            decision_action="adjust window length",
            actor_label="analyst",
            rationale="Change downstream analysis assumptions only.",
        )
    )

    branch_runs = engine.store.stage_runs_for_branch(updated_state.current_branch_id)
    invalidated_stages = {run.stage for run in branch_runs if run.status == StageRunStatus.INVALIDATED}
    assert WorkflowStage.MATERIALIZE_ANALYSIS_DATASET in invalidated_stages
    assert WorkflowStage.EXECUTE_ANALYSIS in invalidated_stages
    assert WorkflowStage.SUMMARIZE_RESULTS in invalidated_stages
    assert WorkflowStage.PROPOSE_NEXT_STEPS in invalidated_stages
    assert WorkflowStage.BUILD_CANONICAL_DATASET not in invalidated_stages


def test_branch_can_fork_from_any_completed_stage_anchor() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    parse_stage_run = engine.store.latest_branch_stage_run(state.current_branch_id, WorkflowStage.PARSE_RESEARCH_QUESTION)

    child_state = engine.fork_branch(
        BranchForkRequest(
            source_branch_id=state.current_branch_id,
            anchor_stage_run_id=parse_stage_run.id,
            actor_label="analyst",
            new_branch_name="alt-lag-path",
            rationale="Explore an alternative branch from parsed question.",
        )
    )

    branch_state = next(item for item in child_state.branch_states if item.branch_id == child_state.current_branch_id)
    child_branch = engine.store.snapshot.branches[child_state.current_branch_id]
    assert child_branch.parent_branch_id == state.current_branch_id
    assert child_branch.lineage_depth == 1
    assert child_state.current_stage == WorkflowStage.GENERATE_HYPOTHESES
    assert branch_state.latest_completed_stage == WorkflowStage.PARSE_RESEARCH_QUESTION


def test_failed_stage_exposes_recovery_options() -> None:
    engine = create_engine(model_adapter=SingleDatasetAdapter())
    state = asyncio.run(engine.create_investigation("Failure demo", "Need a merge plan."))
    result = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    branch_state = next(item for item in result.state.branch_states if item.branch_id == result.state.current_branch_id)

    assert result.state.current_stage == WorkflowStage.PROPOSE_MERGE_PLAN
    assert branch_state.blocked_reason == "stage_failed"
    assert branch_state.recovery_options
    failed_run = engine.store.latest_branch_stage_run(state.current_branch_id, WorkflowStage.PROPOSE_MERGE_PLAN)
    assert failed_run.status == StageRunStatus.FAILED
    assert failed_run.failure_message is not None


def test_hypothesis_can_be_edited_and_invalidates_downstream() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))

    hypothesis_id = next(iter(engine.store.snapshot.hypotheses.values())).id
    edited = asyncio.run(
        engine.edit_hypothesis(
            branch_id=state.current_branch_id,
            hypothesis_id=hypothesis_id,
            actor_label="analyst",
            user_instruction="Rewrite this around semis versus defensives.",
        )
    )

    assert edited.title is not None
    assert edited.required_variables
    branch_state = next(item for item in engine.get_state(state.investigation_id, state.current_branch_id).branch_states if item.branch_id == state.current_branch_id)
    assert branch_state.next_stage == WorkflowStage.RETRIEVE_EVIDENCE


def test_branch_can_fork_from_hypothesis_card() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))
    asyncio.run(engine.run_next_stage(state.investigation_id, state.current_branch_id))

    hypothesis = next(iter(engine.store.snapshot.hypotheses.values()))
    child_state = engine.fork_from_hypothesis(
        source_branch_id=state.current_branch_id,
        hypothesis_id=hypothesis.id,
        actor_label="analyst",
        new_branch_name="hypothesis-branch",
        rationale="Pursue this specific hypothesis path.",
    )

    child_branch = engine.store.snapshot.branches[child_state.current_branch_id]
    assert child_branch.parent_branch_id == state.current_branch_id
    assert child_state.current_stage == WorkflowStage.RETRIEVE_EVIDENCE


def test_merge_plan_can_be_overridden_and_used_for_canonical_dataset_build() -> None:
    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    blocked = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    merge_plan = next(
        item for item in engine.store.snapshot.merge_plans.values() if item.branch_id == state.current_branch_id
    )
    first_mapping = merge_plan.mappings[0]
    edited = engine.edit_merge_plan(
        branch_id=state.current_branch_id,
        merge_plan_id=merge_plan.id,
        actor_label="analyst",
        mapping_overrides=[
            {
                "source_dataset_source_id": str(first_mapping.source_dataset_source_id),
                "source_column": first_mapping.left_column,
                "target_column": "aligned_timestamp",
                "semantic_role": "time_key",
                "include_in_output": True,
            }
        ],
        lag_policy_override="Apply one-period lag for macro features before merge.",
        rationale="Manual override for review.",
    )
    assert edited.id != merge_plan.id
    assert any(item.user_overridden for item in edited.mappings)

    branch_state = next(item for item in blocked.state.branch_states if item.branch_id == state.current_branch_id)
    approved_state = engine.resolve_approval(
        ApprovalResolution(
            checkpoint_id=branch_state.pending_approval_checkpoint_id,
            status=ApprovalStatus.APPROVED,
            actor_label="analyst",
            rationale="Approve edited merge plan.",
        )
    )
    approved_merge_plan = next(
        item
        for item in engine.store.snapshot.merge_plans.values()
        if item.branch_id == approved_state.current_branch_id and item.approved_by_checkpoint_id is not None
    )
    assert approved_merge_plan.id == edited.id
    assert approved_merge_plan.lag_policy == "Apply one-period lag for macro features before merge."


def test_canonical_dataset_build_emits_preview_quality_and_provenance_bundle(monkeypatch) -> None:
    class _StubAdapter:
        async def fetch(self, external_id: str, **kwargs):
            dataset = CanonicalDataset(
                metadata=CanonicalDatasetMetadata(
                    provider="stub",
                    external_id=external_id,
                    name=f"dataset-{external_id}",
                    description="stub dataset",
                    dataset_kind="stub",
                    coverage=TimeCoverage(),
                    provenance=ProvenancePayload(provider="stub", endpoint="fetch"),
                ),
                observations=[
                    CanonicalObservation(date="2024-01-01", value=1.0),
                    CanonicalObservation(date="2024-01-02", value=2.0),
                ],
            )
            return AdapterFetchResult(dataset=dataset)

    class _StubRegistry:
        def get(self, name: str):
            return _StubAdapter()

    monkeypatch.setattr(engine_module, "AdapterRegistry", lambda: _StubRegistry())

    engine = create_engine()
    state = asyncio.run(engine.create_investigation("Demo", "Investigate semis versus defensives."))
    blocked = asyncio.run(engine.run_until_blocked(state.investigation_id, state.current_branch_id))
    checkpoint_id = next(
        item for item in blocked.state.branch_states if item.branch_id == state.current_branch_id
    ).pending_approval_checkpoint_id
    approved = engine.resolve_approval(
        ApprovalResolution(checkpoint_id=checkpoint_id, status=ApprovalStatus.APPROVED, actor_label="analyst")
    )
    stage_run, _ = asyncio.run(engine.run_next_stage(approved.investigation_id, approved.current_branch_id))
    assert stage_run.stage == WorkflowStage.BUILD_CANONICAL_DATASET
    assert len(stage_run.artifact_ref_ids) == 4

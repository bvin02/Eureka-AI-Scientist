# Eureka Workflow Engine

## Workflow design
Eureka uses an explicit state machine with a linear stage order and branch-aware inheritance of completed upstream runs.

Canonical stages:
1. `intake`
2. `parse_research_question`
3. `generate_hypotheses`
4. `retrieve_evidence`
5. `discover_datasets`
6. `profile_datasets`
7. `propose_merge_plan`
8. `await_user_merge_approval`
9. `build_canonical_dataset`
10. `propose_test_plan`
11. `await_user_test_approval`
12. `materialize_analysis_dataset`
13. `execute_analysis`
14. `summarize_results`
15. `propose_next_steps`

Execution model:
- Every stage produces a `StageRun`.
- Every stage writes a notebook entry during stage execution, not after the workflow ends.
- Every stage produces at least one artifact reference, warning, and provenance record.
- Approval stages create `ApprovalCheckpoint` records that block further progress until explicitly resolved.
- Branches inherit valid upstream stage runs from their parent up to the fork anchor stage, then compute downstream results independently.

Model-mediated stages:
- `parse_research_question`
- `generate_hypotheses`
- `retrieve_evidence`
- `discover_datasets`
- `propose_merge_plan`
- `propose_test_plan`
- `summarize_results`
- `propose_next_steps`

These stages route through a model adapter that can use the OpenAI Responses API with typed schemas, while notebook writes, branching, invalidation, and artifact creation remain deterministic backend logic.

## State object
Primary runtime state is represented by:
- `WorkflowSnapshot` in `orchestration/models.py`
- `WorkflowState` in `orchestration/models.py`
- `BranchRuntimeState` in `orchestration/models.py`

`WorkflowSnapshot` is the serializable source of truth. It contains all investigations, branches, stage runs, approvals, notebook entries, artifacts, warnings, provenance records, and domain outputs.

`WorkflowState` is the computed view:
- current branch
- next runnable stage
- pending approval checkpoint
- completed and invalidated runs
- recovery options for failed stages

## Transition rules
- A stage can run only when all dependency stages have an effective, non-invalidated completed run.
- `run_until_blocked` advances until one of these conditions occurs:
  - workflow completes
  - a stage enters `awaiting_approval`
  - a stage fails
- Approval resolution completes the approval stage run and unblocks the next dependent stage.
- Branch forks create a new branch anchored to a specific stage run and inherit only the valid upstream path up to that anchor.

## Partial recomputation strategy
- Invalidations are branch-local.
- User edits and rejected approvals call `invalidate_downstream`.
- Only downstream stages after the anchor stage are invalidated.
- Upstream stage runs remain valid and reusable.
- New reruns create new stage runs with incremented `attempt` and `supersedes_stage_run_id`.

This keeps recomputation scoped and inspectable while preserving immutable historical outputs.

## Failure and retry semantics
- A failing stage produces a terminal `StageRun` with:
  - `status = failed`
  - `failure_message`
  - `recovery_options`
  - warning and provenance records
- Failures do not silently advance the workflow.
- Recovery is explicit:
  - retry the failed stage
  - edit upstream assumptions
  - branch from the failing or prior stage
- Rejected approvals invalidate downstream work and require a new approved path before execution continues.

## Implementation notes
- Engine: `orchestration/engine.py`
- Stage graph: `orchestration/state_machine.py`
- Typed model-stage contracts: `orchestration/contracts.py`
- Model adapter boundary: `orchestration/model_adapter.py`
- Serializable store: `orchestration/store.py`
- Notebook integration: `notebook/service.py`

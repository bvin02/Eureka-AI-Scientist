from domain.enums import WorkflowStage
from orchestration.state_machine import WorkflowDefinition


def test_workflow_definition_contains_expected_stage_count() -> None:
    workflow = WorkflowDefinition()
    assert len(workflow.ordered_stages()) == 15


def test_next_stage_progression_for_merge_plan() -> None:
    workflow = WorkflowDefinition()
    assert workflow.next_stage(WorkflowStage.PROPOSE_MERGE_PLAN) == WorkflowStage.AWAIT_USER_MERGE_APPROVAL


def test_approval_stages_are_explicit() -> None:
    workflow = WorkflowDefinition()
    assert workflow.approval_stages() == [
        WorkflowStage.AWAIT_USER_MERGE_APPROVAL,
        WorkflowStage.AWAIT_USER_TEST_APPROVAL,
    ]


def test_workflow_ends_with_next_steps_not_notebook_commit() -> None:
    workflow = WorkflowDefinition()
    assert workflow.ordered_stages()[-1].stage == WorkflowStage.PROPOSE_NEXT_STEPS

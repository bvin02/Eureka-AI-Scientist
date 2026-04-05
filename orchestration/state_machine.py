from domain.enums import WorkflowStage
from domain.models import StageDescriptor


class WorkflowDefinition:
    def __init__(self) -> None:
        self._stages = [
            StageDescriptor(stage=WorkflowStage.INTAKE, label="Intake"),
            StageDescriptor(stage=WorkflowStage.PARSE_RESEARCH_QUESTION, label="Parse Research Question"),
            StageDescriptor(stage=WorkflowStage.GENERATE_HYPOTHESES, label="Generate Hypotheses"),
            StageDescriptor(stage=WorkflowStage.RETRIEVE_EVIDENCE, label="Retrieve Evidence"),
            StageDescriptor(stage=WorkflowStage.DISCOVER_DATASETS, label="Discover Datasets"),
            StageDescriptor(stage=WorkflowStage.PROFILE_DATASETS, label="Profile Datasets"),
            StageDescriptor(stage=WorkflowStage.PROPOSE_MERGE_PLAN, label="Propose Merge Plan"),
            StageDescriptor(
                stage=WorkflowStage.AWAIT_USER_MERGE_APPROVAL,
                label="Await User Merge Approval",
                requires_approval=True,
            ),
            StageDescriptor(stage=WorkflowStage.BUILD_CANONICAL_DATASET, label="Build Canonical Dataset"),
            StageDescriptor(stage=WorkflowStage.PROPOSE_TEST_PLAN, label="Propose Test Plan"),
            StageDescriptor(
                stage=WorkflowStage.AWAIT_USER_TEST_APPROVAL,
                label="Await User Test Approval",
                requires_approval=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.MATERIALIZE_ANALYSIS_DATASET,
                label="Materialize Analysis Dataset",
            ),
            StageDescriptor(stage=WorkflowStage.EXECUTE_ANALYSIS, label="Execute Analysis"),
            StageDescriptor(stage=WorkflowStage.SUMMARIZE_RESULTS, label="Summarize Results"),
            StageDescriptor(stage=WorkflowStage.PROPOSE_NEXT_STEPS, label="Propose Next Steps"),
        ]

    def ordered_stages(self) -> list[StageDescriptor]:
        return list(self._stages)

    def approval_stages(self) -> list[WorkflowStage]:
        return [stage.stage for stage in self._stages if stage.requires_approval]

    def next_stage(self, current: WorkflowStage) -> WorkflowStage | None:
        for index, stage in enumerate(self._stages):
            if stage.stage == current:
                if index == len(self._stages) - 1:
                    return None
                return self._stages[index + 1].stage
        raise ValueError(f"Unknown workflow stage: {current}")

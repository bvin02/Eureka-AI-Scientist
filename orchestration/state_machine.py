from __future__ import annotations

from domain.enums import WorkflowStage
from domain.models import StageDescriptor


class WorkflowDefinition:
    def __init__(self) -> None:
        self._stages = [
            StageDescriptor(stage=WorkflowStage.INTAKE, label="Intake"),
            StageDescriptor(
                stage=WorkflowStage.PARSE_RESEARCH_QUESTION,
                label="Parse Research Question",
                depends_on=[WorkflowStage.INTAKE],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.GENERATE_HYPOTHESES,
                label="Generate Hypotheses",
                depends_on=[WorkflowStage.PARSE_RESEARCH_QUESTION],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.RETRIEVE_EVIDENCE,
                label="Retrieve Evidence",
                depends_on=[WorkflowStage.PARSE_RESEARCH_QUESTION],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.DISCOVER_DATASETS,
                label="Discover Datasets",
                depends_on=[WorkflowStage.PARSE_RESEARCH_QUESTION],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.PROFILE_DATASETS,
                label="Profile Datasets",
                depends_on=[WorkflowStage.DISCOVER_DATASETS],
            ),
            StageDescriptor(
                stage=WorkflowStage.PROPOSE_MERGE_PLAN,
                label="Propose Merge Plan",
                depends_on=[WorkflowStage.PROFILE_DATASETS],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.AWAIT_USER_MERGE_APPROVAL,
                label="Await User Merge Approval",
                depends_on=[WorkflowStage.PROPOSE_MERGE_PLAN],
                requires_approval=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.BUILD_CANONICAL_DATASET,
                label="Build Canonical Dataset",
                depends_on=[WorkflowStage.AWAIT_USER_MERGE_APPROVAL],
            ),
            StageDescriptor(
                stage=WorkflowStage.PROPOSE_TEST_PLAN,
                label="Propose Test Plan",
                depends_on=[WorkflowStage.BUILD_CANONICAL_DATASET],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.AWAIT_USER_TEST_APPROVAL,
                label="Await User Test Approval",
                depends_on=[WorkflowStage.PROPOSE_TEST_PLAN],
                requires_approval=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.MATERIALIZE_ANALYSIS_DATASET,
                label="Materialize Analysis Dataset",
                depends_on=[WorkflowStage.AWAIT_USER_TEST_APPROVAL],
            ),
            StageDescriptor(
                stage=WorkflowStage.EXECUTE_ANALYSIS,
                label="Execute Analysis",
                depends_on=[WorkflowStage.MATERIALIZE_ANALYSIS_DATASET],
            ),
            StageDescriptor(
                stage=WorkflowStage.SUMMARIZE_RESULTS,
                label="Summarize Results",
                depends_on=[WorkflowStage.EXECUTE_ANALYSIS],
                model_mediated=True,
            ),
            StageDescriptor(
                stage=WorkflowStage.PROPOSE_NEXT_STEPS,
                label="Propose Next Steps",
                depends_on=[WorkflowStage.SUMMARIZE_RESULTS],
                model_mediated=True,
            ),
        ]
        self._by_stage = {descriptor.stage: descriptor for descriptor in self._stages}

    def ordered_stages(self) -> list[StageDescriptor]:
        return list(self._stages)

    def approval_stages(self) -> list[WorkflowStage]:
        return [stage.stage for stage in self._stages if stage.requires_approval]

    def descriptor(self, stage: WorkflowStage) -> StageDescriptor:
        return self._by_stage[stage]

    def dependencies(self, stage: WorkflowStage) -> list[WorkflowStage]:
        return list(self._by_stage[stage].depends_on)

    def next_stage(self, current: WorkflowStage) -> WorkflowStage | None:
        for index, stage in enumerate(self._stages):
            if stage.stage == current:
                if index == len(self._stages) - 1:
                    return None
                return self._stages[index + 1].stage
        raise ValueError(f"Unknown workflow stage: {current}")

    def stage_index(self, stage: WorkflowStage) -> int:
        for index, descriptor in enumerate(self._stages):
            if descriptor.stage == stage:
                return index
        raise ValueError(f"Unknown workflow stage: {stage}")

    def downstream_stages(self, anchor: WorkflowStage, include_anchor: bool = False) -> list[WorkflowStage]:
        anchor_index = self.stage_index(anchor)
        if not include_anchor:
            anchor_index += 1
        return [descriptor.stage for descriptor in self._stages[anchor_index:]]

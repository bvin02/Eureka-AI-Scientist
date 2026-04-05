from __future__ import annotations

from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.prompts.registry import PromptRegistry
from orchestration.contracts import CanonicalBuildInput, CanonicalBuildPlanProposal


class DeterministicCanonicalDatasetBuilder:
    def build_plan(self, build_input: CanonicalBuildInput) -> CanonicalBuildPlanProposal:
        frequency = build_input.canonical_frequency or "daily"
        return CanonicalBuildPlanProposal(
            dataset_name="canonical_research_dataset",
            selected_hypothesis_summary=build_input.selected_hypothesis,
            timestamp_normalization="Normalize all source time keys to UTC ISO-8601 dates before joining.",
            lag_policy="Apply conservative as-of previous timestamp lag for non-synchronous sources.",
            frequency_alignment=f"Align all sources to canonical {frequency} grain before final emit.",
            leakage_checks=[
                "assert_no_future_join_keys",
                "assert_no_future_timestamp_alignment",
                "assert_no_target_in_feature_columns",
            ],
            quality_checks=[
                "row_count_non_zero",
                "null_fraction_threshold",
                "timestamp_monotonicity",
            ],
            notes=[
                "Canonical dataset is intentionally test-plan agnostic.",
                "Test-specific filtering and feature selection belong in materialize_analysis_dataset.",
            ],
        )


class CanonicalDatasetBuilder:
    def __init__(
        self,
        gateway: OpenAIResponsesGateway | None = None,
        prompt_registry: PromptRegistry | None = None,
        fallback: DeterministicCanonicalDatasetBuilder | None = None,
    ) -> None:
        self.gateway = gateway or OpenAIResponsesGateway()
        self.prompt_registry = prompt_registry or PromptRegistry()
        self.fallback = fallback or DeterministicCanonicalDatasetBuilder()

    async def build_plan(self, build_input: CanonicalBuildInput) -> CanonicalBuildPlanProposal:
        if self.gateway.client is None:
            return self.fallback.build_plan(build_input)
        request = PromptRequest(
            prompt_name="canonical_dataset_builder",
            system_prompt=self.prompt_registry.get_prompt_text("canonical_dataset_builder"),
            user_prompt=build_input.model_dump_json(indent=2),
        )
        try:
            return await self.gateway.generate_structured(
                request=request,
                schema=CanonicalBuildPlanProposal,
            )
        except RuntimeError:
            return self.fallback.build_plan(build_input)

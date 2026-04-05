import asyncio

from llm.prompts.registry import PromptRegistry
from llm.research_planner import (
    DeterministicResearchPlanner,
    ResearchPlanner,
    ResearchPlannerOutput,
    planner_output_to_research_question_plan,
)


def test_prompt_registry_includes_research_planner_prompt() -> None:
    registry = PromptRegistry()
    assert "research_planner" in registry.list_prompts()


def test_deterministic_planner_handles_quant_prompt_with_paths_and_entities() -> None:
    planner = DeterministicResearchPlanner()
    output = planner.plan(
        "Will cooling inflation and falling real yields rotate leadership into semiconductors and growth stocks?"
    )

    assert isinstance(output, ResearchPlannerOutput)
    assert output.target_variable
    assert output.explanatory_variables
    assert len(output.path_options) == 3
    assert [item.label for item in output.path_options] == ["conservative", "recommended", "aggressive"]
    assert any(entity.canonical_name == "inflation" for entity in output.structured_entities)
    assert any(entity.canonical_name == "semiconductors" for entity in output.structured_entities)
    assert output.ambiguity_notes is not None


def test_planner_output_maps_to_workflow_question_plan() -> None:
    planner = DeterministicResearchPlanner()
    output = planner.plan("Does recession risk hurt cyclicals?")
    question_plan = planner_output_to_research_question_plan(output)

    assert question_plan.canonical_question == output.normalized_question
    assert question_plan.horizon == output.likely_time_horizon
    assert question_plan.frequency == output.likely_data_frequency
    assert question_plan.success_criteria


def test_research_planner_falls_back_without_openai_client() -> None:
    planner = ResearchPlanner()
    output = asyncio.run(planner.plan("Do soft CPI prints help long-duration tech?"))
    assert output.normalized_question.startswith("Investigate whether")
    assert len(output.path_options) == 3


def test_deterministic_planner_handles_ambiguous_prompt_gracefully() -> None:
    planner = DeterministicResearchPlanner()
    output = planner.plan("What happens to markets when things get weird?")
    assert output.ambiguity_notes
    assert output.path_options[1].label == "recommended"

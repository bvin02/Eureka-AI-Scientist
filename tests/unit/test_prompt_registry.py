from llm.prompts.registry import PromptRegistry


def test_prompt_registry_exposes_expected_prompts() -> None:
    registry = PromptRegistry()
    assert registry.list_prompts() == [
        "canonical_dataset_builder",
        "evidence_retrieval",
        "hypothesis_engine",
        "master",
        "merge_planner",
        "research_planner",
        "scaffold",
    ]

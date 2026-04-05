from llm.prompts.registry import PromptRegistry


def test_prompt_registry_exposes_expected_prompts() -> None:
    registry = PromptRegistry()
    assert registry.list_prompts() == [
        "evidence_retrieval",
        "hypothesis_engine",
        "master",
        "research_planner",
        "scaffold",
    ]

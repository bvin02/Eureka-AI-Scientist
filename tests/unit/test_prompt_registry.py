from llm.prompts.registry import PromptRegistry


def test_prompt_registry_exposes_master_and_scaffold_prompts() -> None:
    registry = PromptRegistry()
    assert registry.list_prompts() == ["master", "scaffold"]

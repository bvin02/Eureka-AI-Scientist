from pathlib import Path


class PromptRegistry:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or Path.cwd()
        self._prompts = {
            "master": Path("prompts/prompt1_master.md"),
            "scaffold": Path("prompts/prompt2_scaffold.md"),
            "research_planner": Path("llm/prompts/research_planner.md"),
        }

    def list_prompts(self) -> list[str]:
        return sorted(self._prompts.keys())

    def get_prompt_text(self, name: str) -> str:
        relative_path = self._prompts[name]
        return (self.root / relative_path).read_text(encoding="utf-8")

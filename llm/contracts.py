from typing import Any, Literal

from pydantic import BaseModel


class PromptRequest(BaseModel):
    prompt_name: str
    system_prompt: str
    user_prompt: str


class ToolDefinition(BaseModel):
    type: Literal["function"] = "function"
    name: str
    description: str
    parameters: dict[str, Any]

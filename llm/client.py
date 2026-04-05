from collections.abc import Sequence
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from infra.logging import get_logger
from infra.settings import get_settings
from llm.contracts import PromptRequest, ToolDefinition

try:
    from openai import AsyncOpenAI
except ImportError:  # pragma: no cover - handled by dependency installation
    AsyncOpenAI = None  # type: ignore[assignment]

T = TypeVar("T", bound=BaseModel)


class OpenAIResponsesGateway:
    def __init__(self, client: Any | None = None) -> None:
        self.settings = get_settings()
        self.logger = get_logger("eureka.llm")

        if client is not None:
            self.client = client
        elif AsyncOpenAI is not None and self.settings.openai_api_key:
            self.client = AsyncOpenAI(api_key=self.settings.openai_api_key)
        else:
            self.client = None

    async def generate_structured(
        self,
        request: PromptRequest,
        schema: type[T],
        tools: Sequence[ToolDefinition] | None = None,
        model: str | None = None,
    ) -> T:
        if self.client is None:
            raise RuntimeError("OpenAI client is not configured. Set EUREKA_OPENAI_API_KEY first.")

        response = await self.client.responses.create(
            model=model or self.settings.openai_model,
            input=self._build_input(request),
            tools=[tool.model_dump(exclude_none=True) for tool in tools or []],
            text={"format": self._build_json_schema_format(schema)},
            store=False,
        )

        output_text = getattr(response, "output_text", None)
        if not output_text:
            raise RuntimeError("Responses API returned no structured output text.")

        try:
            return schema.model_validate_json(output_text)
        except ValidationError as exc:
            self.logger.exception("structured output validation failed")
            raise RuntimeError("Structured response validation failed.") from exc

    def _build_input(self, request: PromptRequest) -> list[dict[str, Any]]:
        return [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": request.system_prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": request.user_prompt}],
            },
        ]

    def _build_json_schema_format(self, schema: type[T]) -> dict[str, Any]:
        return {
            "type": "json_schema",
            "name": schema.__name__.lower(),
            "strict": True,
            "schema": schema.model_json_schema(),
        }

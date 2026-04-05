from fastapi import APIRouter

from analysis.registry import supported_analyses
from domain.models import ArchitectureDecision, StageDescriptor, SystemHealth
from infra.settings import get_settings
from orchestration.state_machine import WorkflowDefinition

router = APIRouter(tags=["system"])


@router.get("/health", response_model=SystemHealth)
def health() -> SystemHealth:
    settings = get_settings()
    return SystemHealth(
        status="ok",
        environment=settings.env,
        app_name=settings.app_name,
        openai_model=settings.openai_model,
    )


@router.get("/architecture", response_model=list[ArchitectureDecision])
def architecture() -> list[ArchitectureDecision]:
    return [
        ArchitectureDecision(
            component="backend_api",
            selection="FastAPI",
            rationale="Fast iteration, explicit Python contracts, and clean integration with the worker and shared domain code.",
        ),
        ArchitectureDecision(
            component="workflow",
            selection="Explicit persisted state machine",
            rationale="Research stages, approvals, retries, and recomputation need inspectable transitions instead of implicit agent loops.",
        ),
        ArchitectureDecision(
            component="llm_gateway",
            selection="OpenAI Responses API wrapper",
            rationale="All model-mediated stages must use GPT-5.4 with schema-validated structured outputs from one centralized boundary.",
        ),
        ArchitectureDecision(
            component="frontend",
            selection="React + TypeScript + Vite",
            rationale="A fast workspace shell keeps backend ownership in Python while allowing a responsive research UI.",
        ),
    ]


@router.get("/workflow/stages", response_model=list[StageDescriptor])
def workflow_stages() -> list[StageDescriptor]:
    return WorkflowDefinition().ordered_stages()


@router.get("/analysis/methods")
def analysis_methods() -> dict[str, list[dict[str, str]]]:
    methods = [method.model_dump(mode="json") for method in supported_analyses()]
    return {"methods": methods}

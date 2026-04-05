from datetime import datetime, timezone
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from domain.enums import InvestigationStatus, WorkflowStage


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class SystemHealth(BaseModel):
    status: str
    environment: str
    app_name: str
    openai_model: str


class ArchitectureDecision(BaseModel):
    component: str
    selection: str
    rationale: str


class StageDescriptor(BaseModel):
    stage: WorkflowStage
    label: str
    requires_approval: bool = False
    notebook_emission_required: bool = True


class AnalysisMethodDescriptor(BaseModel):
    key: str
    label: str
    description: str


class InvestigationSummary(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    status: InvestigationStatus = InvestigationStatus.ACTIVE
    active_stage: WorkflowStage = WorkflowStage.INTAKE
    created_at: datetime = Field(default_factory=utc_now)


class BranchSummary(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    investigation_id: UUID
    name: str
    parent_branch_id: UUID | None = None
    created_at: datetime = Field(default_factory=utc_now)


class NotebookEntrySummary(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    branch_id: UUID
    stage: WorkflowStage
    title: str
    created_at: datetime = Field(default_factory=utc_now)

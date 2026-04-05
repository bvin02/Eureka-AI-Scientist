from typing import NewType
from uuid import UUID

InvestigationId = NewType("InvestigationId", UUID)
BranchId = NewType("BranchId", UUID)
NotebookEntryId = NewType("NotebookEntryId", UUID)

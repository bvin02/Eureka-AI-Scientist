from typing import Protocol

from pydantic import BaseModel


class DatasetDiscoveryResult(BaseModel):
    provider: str
    external_id: str
    name: str
    description: str


class DataSourceAdapter(Protocol):
    name: str

    def describe(self) -> str:
        ...

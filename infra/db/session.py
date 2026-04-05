from functools import lru_cache
from typing import Any

try:
    from sqlmodel import Session, SQLModel, create_engine
except ImportError:  # pragma: no cover - handled after dependency install
    Session = Any  # type: ignore[misc,assignment]

    class _FallbackMetadata:
        @staticmethod
        def create_all(_: Any) -> None:
            return None

    class _FallbackSQLModel:
        metadata = _FallbackMetadata()

    SQLModel = _FallbackSQLModel  # type: ignore[assignment]

    def create_engine(*_: Any, **__: Any) -> None:
        return None

from infra.settings import get_settings


@lru_cache(maxsize=1)
def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, echo=False)


def init_db() -> None:
    SQLModel.metadata.create_all(get_engine())


def get_session() -> Session:
    engine = get_engine()
    if engine is None:
        raise RuntimeError("sqlmodel is not installed. Run `uv sync --dev` to enable DB-backed features.")
    return Session(engine)

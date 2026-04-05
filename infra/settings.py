from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="EUREKA_",
        extra="ignore",
    )

    app_name: str = "Eureka"
    env: str = "development"
    openai_model: str = "gpt-5.4"
    openai_api_key: str | None = None
    fred_api_key: str | None = None
    database_url: str = "sqlite:///./eureka.db"
    artifacts_dir: Path = Path("./artifacts")
    log_level: str = "INFO"
    worker_poll_seconds: int = 2


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

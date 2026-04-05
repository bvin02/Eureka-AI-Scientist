from contextlib import asynccontextmanager

from fastapi import FastAPI

from apps.api.routers.system import router as system_router
from infra.db.session import init_db
from infra.logging import configure_logging, get_logger
from infra.settings import get_settings


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    init_db()
    logger = get_logger("eureka.api")
    logger.info("api startup complete")
    yield
    logger.info("api shutdown complete")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version="0.1.0",
        description="Workflow-first quant research engine for macro and market investigations.",
        lifespan=lifespan,
    )
    app.include_router(system_router, prefix="/api/v1")
    return app


app = create_app()

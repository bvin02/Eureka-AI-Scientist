# Eureka

Eureka is a conversational quant research engine for macro and market investigations.
The product is a workflow-first research workspace, not a chat wrapper.

## Architecture
- Backend API: FastAPI
- Workflow orchestration: explicit Python state machine
- Background execution: Python worker using shared orchestration code
- Model gateway: OpenAI Responses API via a centralized client wrapper
- Primary model: GPT-5.4
- Typed contracts: Pydantic v2
- Persistence: SQLModel/SQLAlchemy
- Analytical data layer: DuckDB + Parquet
- Analysis stack: pandas + statsmodels + scipy
- Frontend: React + TypeScript + Vite

## Repo layout
```text
apps/
  api/
  worker/
  web/
analysis/
data/
domain/
infra/
llm/
notebook/
orchestration/
tests/
```

## Local development
Python uses `uv` and the frontend uses `npm`.

1. Create local environment variables from `.env.example`.
2. Install Python dependencies:
```bash
uv sync --dev
```
3. Start the API:
```bash
uv run uvicorn apps.api.main:app --reload
```
4. Start the worker:
```bash
uv run python -m apps.worker.main
```
5. Start the frontend:
```bash
cd apps/web
npm install
npm run dev
```

## Quality checks
```bash
uv run pytest
uv run ruff check .
uv run mypy .
python -m compileall apps analysis data domain infra llm notebook orchestration tests
```

## Phase 0 scope
This scaffold establishes:
- package layout and tooling
- FastAPI application shell
- worker entrypoint
- workflow stage definitions
- OpenAI Responses API boundary
- settings, logging, and DB wiring
- React workspace shell
- smoke tests and documentation

Secrets must not remain in source-controlled files. Load keys from environment variables or a real secret manager.

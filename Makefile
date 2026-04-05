.PHONY: api worker test lint typecheck compile web

api:
	uv run uvicorn apps.api.main:app --reload

worker:
	uv run python -m apps.worker.main

test:
	uv run pytest

lint:
	uv run ruff check .

typecheck:
	uv run mypy .

compile:
	python -m compileall apps analysis data domain infra llm notebook orchestration tests

web:
	cd apps/web && npm run dev

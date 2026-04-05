# Eureka Build Status

## Current phase
1. Domain model and persistence contracts
Status: ready to begin

## Architecture baseline
- Backend API: FastAPI
- Workflow engine: explicit persisted Python state machine
- Background execution: worker process using shared orchestration code
- LLM interface: centralized OpenAI Responses API client
- Primary model: GPT-5.4
- Schema layer: Pydantic v2
- Persistence: SQLAlchemy/SQLModel
- Analytical data layer: DuckDB + Parquet artifacts
- Analysis stack: pandas + statsmodels + scipy
- Frontend: React + TypeScript research workspace

## Architecture tightening applied
- Replaced the cyclic `build_analysis_dataset -> propose_test_plan` design with `build_canonical_dataset -> propose_test_plan -> materialize_analysis_dataset`.
- Removed `notebook_commit` as a standalone workflow stage; notebook persistence is an event emitted by every stage.
- Added explicit architecture requirements for immutable stage runs, approval checkpoints, and artifact references.
- Tightened review prompts to look for branch invalidation, race conditions, and notebook/orchestration coupling.

## Repo structure baseline
```text
eureka/
  apps/
    api/
    worker/
    web/
  domain/
  orchestration/
  llm/
  data/
  analysis/
  notebook/
  infra/
  tests/
```

## Phase plan
- [x] 0. Architecture and scaffold
- [ ] 1. Domain model and persistence contracts
- [ ] 2. OpenAI gateway and prompt registry
- [ ] 3. Workflow engine and stage persistence
- [ ] 4. Source adapters and dataset profiling
- [ ] 5. Merge planning and analysis dataset builder
- [ ] 6. Analysis runtime and result artifacts
- [ ] 7. Notebook system and user steering APIs
- [ ] 8. Frontend workspace and visualization
- [ ] 9. Export, demo path, reliability hardening, QA, and final polish

## Phase 0 completed
- Architecture decisions locked in `PROJECT_RULES.md`
- Canonical repo structure created
- Backend API, worker, web shell, and shared Python packages scaffolded
- Dependency baseline and developer commands added
- Smoke tests added for workflow definition and API shell

## Open issues
- Plaintext API keys are currently checked into `api/openai.txt` and `api/fred.txt`; rotate and move to environment-based secret management before any shared use.
- Domain and persistence implementation still need concrete `StageRun`, `ApprovalCheckpoint`, and artifact-link models beyond the current scaffold summaries.

## Decisions
- Eureka is a workflow product, not a chat wrapper.
- The notebook is a first-class system of record for execution history, provenance, and branching.
- All important model-mediated outputs must be schema-validated typed payloads.
- Human approvals are required at major control points such as merge plans and test plans.
- Reproducibility and inspectability take priority over breadth.
- The canonical dataset is built before test planning; test-specific dataset materialization happens only after test approval.
- Notebook entries are append-only stage events, not a separate final workflow stage.

# Eureka Build Status

## Current phase
4. Source adapters and dataset profiling
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
- [x] 1. Domain model and persistence contracts
- [x] 2. OpenAI gateway and prompt registry
- [x] 3. Workflow engine and stage persistence
- [ ] 4. Source adapters and dataset profiling
- [ ] 5. Merge planning and analysis dataset builder
- [ ] 6. Analysis runtime and result artifacts
- [ ] 7. Notebook system and user steering APIs
- [ ] 8. Frontend workspace and visualization
- [ ] 9. Export, demo path, reliability hardening, QA, and final polish

## Phase 3 completed
- Explicit workflow state machine implemented with dependency-aware stage descriptors
- Serializable workflow snapshot and resumable runtime state implemented
- Immutable stage runs now carry attempt numbers, fingerprints, warnings, provenance, notebook entries, and artifact references
- Approval checkpoints, branch forking, downstream invalidation, retry/failure semantics, and per-stage notebook writes implemented
- Deterministic model-adapter boundary added so model-mediated stages can use Responses API while orchestration stays inspectable

## Phase 2 completed
- Centralized model-stage adapter boundary now routes structured stage generation through one orchestrator-facing interface
- Responses-backed adapter and deterministic fallback adapter both implement the same typed contracts

## Phase 1 completed
- Canonical domain entities implemented for investigations, questions, hypotheses, evidence, datasets, merges, tests, results, notebook lineage, provenance, decisions, stage runs, approvals, and artifact references
- Domain invariants added for invalidation, approvals, notebook anchors, and stage-run terminal states
- Canonical domain model documentation added with purpose, lifecycle, persistence strategy, and UI/notebook relations for each entity
- Workflow and stage naming aligned to the tightened architecture baseline

## Phase 0 completed
- Architecture decisions locked in `PROJECT_RULES.md`
- Canonical repo structure created
- Backend API, worker, web shell, and shared Python packages scaffolded
- Dependency baseline and developer commands added
- Smoke tests added for workflow definition and API shell

## Open issues
- Plaintext API keys are currently checked into `api/openai.txt` and `api/fred.txt`; rotate and move to environment-based secret management before any shared use.

## Decisions
- Eureka is a workflow product, not a chat wrapper.
- The notebook is a first-class system of record for execution history, provenance, and branching.
- All important model-mediated outputs must be schema-validated typed payloads.
- Human approvals are required at major control points such as merge plans and test plans.
- Reproducibility and inspectability take priority over breadth.
- The canonical dataset is built before test planning; test-specific dataset materialization happens only after test approval.
- Notebook entries are append-only stage events, not a separate final workflow stage.

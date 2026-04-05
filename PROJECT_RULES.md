# Eureka Project Rules

## Product
Eureka is a conversational quant research engine for markets, macro, and economics.
It turns vague research ideas into structured hypotheses, finds and joins datasets, runs real analyses and backtests, and records the full investigation in a transparent notebook with branching and user steering.

## Product principles
1. Execution and transparent workflow are the moat, not chat.
2. Inspectability beats magic.
3. Reproducibility beats cleverness.
4. Workflow clarity beats gimmicks.
5. Narrow excellence beats broad mediocrity.
6. Quant rigor is mandatory: no silent leakage, hidden assumptions, or vague provenance.

## Build priorities
1. Rock-solid architecture and clean abstractions
2. Excellent UX for research workflows
3. Deterministic, inspectable, debuggable pipelines
4. Transparent provenance and notebook history
5. Strong quant hygiene and visible caveats
6. Working end-to-end product over breadth

## Hard constraints
- Python-first backend and orchestration
- Python-first data integration and analysis stack
- OpenAI Responses API for all model-mediated workflow stages
- GPT-5.4 as the primary reasoning model
- Structured outputs and typed schemas for important model outputs
- Human-in-the-loop workflow with explicit approvals
- Professional light institutional UI
- No dark gradient startup aesthetic
- No fake hardcoded flows pretending to be dynamic

## Final architecture baseline
- Backend API: FastAPI
- Workflow execution: explicit Python state machine with persisted stage state and resumable runs
- Background execution: in-process worker or lightweight queue-backed worker behind the same Python codebase
- Schema layer: Pydantic v2
- Persistence: SQLAlchemy/SQLModel with PostgreSQL in deployed environments and SQLite acceptable for local dev
- Analytical data layer: DuckDB plus Parquet artifacts
- Analysis libraries: pandas, statsmodels, scipy, and light use of polars only if it materially helps
- Frontend: React plus TypeScript workspace application
- Charts: declarative charting with a clean institutional presentation
- Model gateway: a centralized OpenAI client abstraction wrapping the Responses API, structured outputs, validation, retries, timeouts, and logging

## System boundaries
- `apps/api` owns HTTP APIs, SSE/event streaming, dependency wiring, and read models
- `apps/worker` owns background workflow execution
- `apps/web` owns the research workspace UI
- `domain` owns canonical entities, enums, IDs, and schema contracts
- `orchestration` owns workflow states, transitions, invalidation, retries, and human approval gates
- `llm` owns prompts, typed model contracts, and OpenAI integration
- `data` owns source adapters, profiling, cataloging, joins, and dataset materialization
- `analysis` owns statistical methods, backtests, and result artifacts
- `notebook` owns notebook entries, branch lineage, compare logic, and memo export
- `infra` owns settings, DB wiring, artifact storage, and structured logging

## Canonical repo structure
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

## Workflow requirements
The workflow engine is a first-class product primitive.
It must be explicit, inspectable, serializable, resumable, and branch-aware.

Canonical stages:
1. intake
2. parse_research_question
3. generate_hypotheses
4. retrieve_evidence
5. discover_datasets
6. profile_datasets
7. propose_merge_plan
8. await_user_merge_approval
9. build_analysis_dataset
10. propose_test_plan
11. await_user_test_approval
12. execute_analysis
13. summarize_results
14. propose_next_steps
15. notebook_commit

Rules:
- every stage emits notebook entries
- every stage emits warnings and provenance
- user can branch from any notebook entry
- downstream recomputation is scoped to affected stages only
- failed stages produce visible recovery options
- all important state is reproducible from persisted notebook and artifact state

## Required v1 capabilities
- intake natural language research prompts
- parse prompts into a structured research plan
- generate multiple hypothesis cards
- allow user path selection and steering
- retrieve literature or evidence summaries
- discover candidate datasets
- profile datasets
- propose schema mappings and merge plans
- allow user approval or override of merge plans
- execute correlation, summary, linear regression, rolling regression, event study, simple backtest, and regime split analyses
- maintain notebook timeline with branch and fork support
- compare branches
- show provenance, assumptions, warnings, and exact transformations
- export a concise research memo

## v1 data sources
- FRED
- Yahoo Finance or equivalent simple market data source
- SEC EDGAR
- Optional evidence retrieval layer if time allows

## Engineering requirements
- Strongly typed Python contracts across service boundaries
- Pydantic-style schemas for model I/O and API payloads
- Explicit separation between orchestration, data access, analysis, notebook logic, and UI
- Structured logging and traceable stage execution
- Deterministic artifact generation where practical
- No ad hoc model calls scattered through the codebase
- No raw prose parsing when schema-based outputs are viable
- Everything material must be reproducible from notebook state, artifacts, and inputs

## Notebook and provenance rules
- The notebook is a product surface, not a debug log
- Every major automated or human decision creates a notebook entry
- Each notebook entry can carry summary text, structured payloads, source refs, warnings, approvals, artifacts, and provenance
- Branch lineage must be visible and comparable
- Prompt version, model name, tool calls, input fingerprint, and output fingerprint must be captured for model-mediated stages

## UI rules
- Workspace-first layout, not a full-screen chat UI
- Left rail: notebook timeline and branch tree
- Center: active research workspace
- Right panel: provenance, warnings, assumptions, and inspector
- Every major automated step must be inspectable and overridable
- Methodology and caveats must appear near results

## Non-goals
- Universal domain support
- Real-time collaboration
- Auth complexity
- Enterprise permissions model
- Fully autonomous hidden reasoning
- Unnecessary ML model training

# Eureka Canonical Domain Model

This document defines the canonical domain entities for Eureka.
All entities use explicit UUIDs, explicit timestamps, typed nested schemas, and explicit references to stage runs and artifacts where applicable.

Persistence baseline:
- Primary store: relational tables via SQLModel/SQLAlchemy
- Nested structures: JSON/JSONB columns for assumptions, caveats, configs, metrics, and nested typed records
- Large payloads and files: artifact store referenced by `ArtifactRef`
- Append-only records: notebook entries, stage runs, approvals, provenance, warnings, result artifacts
- Mutable records: investigations and branches for head pointers and status, with immutable downstream outputs referenced by ID

## Investigation
- Purpose: Root object for a research project, tying prompt, active branch, notebook head, and reproducibility posture together.
- Schema/type definition: `Investigation` in `domain/models.py`
- Lifecycle: Created at intake, updated when active branch or top-level status changes, completed or failed at investigation end.
- Persistence strategy: One row per investigation, with JSON columns for assumptions, caveats, and reproducibility metadata.
- Relation to notebook and UI: Powers the workspace shell, investigation header, and branch switcher; notebook rail is always scoped to an investigation and branch.

## ResearchQuestion
- Purpose: Canonical structured restatement of the user prompt for downstream planning and analysis.
- Schema/type definition: `ResearchQuestion`
- Lifecycle: Produced by the parse question stage, superseded by later branch-specific restatements rather than mutated.
- Persistence strategy: Append-only row keyed by branch and stage run; provenance linked back to the raw prompt and planner output.
- Relation to notebook and UI: Displayed in the plan review workspace and linked from the stage notebook entry.

## Hypothesis
- Purpose: Represents one testable thesis card with mechanism, expected direction, and falsifiers.
- Schema/type definition: `Hypothesis`
- Lifecycle: Generated during hypothesis stage; user selection changes status through new branch decisions rather than in-place edits.
- Persistence strategy: Append-only rows per branch and stage run.
- Relation to notebook and UI: Rendered as selectable hypothesis cards and referenced in steering decisions and later test plans.

## EvidenceSource
- Purpose: Captures literature, filings, or retrieved evidence items supporting or challenging hypotheses.
- Schema/type definition: `EvidenceSource`
- Lifecycle: Created by evidence retrieval runs and remains immutable as a cited source record.
- Persistence strategy: Append-only rows; raw excerpts or cached snapshots live in artifact storage and are referenced by `artifact_ref_ids`.
- Relation to notebook and UI: Shown as evidence cards with source links and provenance in the inspector.

## DatasetSource
- Purpose: Canonical catalog entry for an external dataset candidate discovered for the investigation.
- Schema/type definition: `DatasetSource`
- Lifecycle: Created during dataset discovery, then reused by profiling, merge planning, and materialization stages.
- Persistence strategy: Append-only rows plus provider metadata in JSON columns.
- Relation to notebook and UI: Drives dataset cards, source metadata views, and dataset selection workflows.

## DatasetProfile
- Purpose: Stores dataset shape, quality profile, keys, time coverage, and caveats discovered during profiling.
- Schema/type definition: `DatasetProfile`
- Lifecycle: Produced from a specific profiling run; newer profiles are appended when data or configuration changes.
- Persistence strategy: Append-only row with nested column profile JSON and optional preview artifacts.
- Relation to notebook and UI: Supports dataset preview tables, schema inspection, and warning surfaces before merge approval.

## MergePlan
- Purpose: Declares how two datasets should be joined, aligned in time, validated, and transformed.
- Schema/type definition: `MergePlan`
- Lifecycle: Proposed by merge planning, optionally approved via an approval checkpoint, and superseded by new plans on branch changes.
- Persistence strategy: Append-only row with nested `MergeMapping` records and transform specs stored as JSON.
- Relation to notebook and UI: Central object for merge review tables, approval UI, and provenance explanation.

## MergeMapping
- Purpose: Describes one semantic join mapping between source columns, including transforms and confidence.
- Schema/type definition: `MergeMapping`
- Lifecycle: Created only as part of a merge plan and remains immutable with it.
- Persistence strategy: Child table or embedded JSON rows keyed to `merge_plan_id`.
- Relation to notebook and UI: Rendered in merge mapping tables and ambiguity inspector panels.

## AnalysisDataset
- Purpose: Represents a materialized canonical or test-specific dataset prepared for analysis.
- Schema/type definition: `AnalysisDataset`
- Lifecycle: Produced after canonical dataset build or test-specific materialization; invalidated when upstream merge plans or assumptions change.
- Persistence strategy: Append-only metadata row plus Parquet or DuckDB artifact referenced through `materialized_artifact_ref_id`.
- Relation to notebook and UI: Powers dataset preview, analysis builder defaults, and reproducibility inspection.

## TestPlan
- Purpose: Encodes the approved or proposed set of analyses to run for a selected hypothesis path.
- Schema/type definition: `TestPlan`
- Lifecycle: Proposed from the canonical dataset, optionally approved, and superseded by new versions instead of edited.
- Persistence strategy: Append-only row with nested `AnalysisSpec` JSON.
- Relation to notebook and UI: Rendered as test plan cards and approval checkpoints before execution.

## AnalysisRun
- Purpose: Execution record for one concrete analysis invocation against one analysis dataset.
- Schema/type definition: `AnalysisRun`
- Lifecycle: Created when execution starts, completed or failed as runtime finishes, never overwritten.
- Persistence strategy: Append-only row keyed to test plan and stage run, with JSON config and reproducibility metadata.
- Relation to notebook and UI: Drives results tables, execution status badges, and rerun controls.

## ResultArtifact
- Purpose: Domain-level description of an analysis output such as a chart, table, or metric bundle.
- Schema/type definition: `ResultArtifact`
- Lifecycle: Produced from a completed analysis run and linked to an underlying artifact reference.
- Persistence strategy: Append-only metadata row with `artifact_ref_id` pointing at storage.
- Relation to notebook and UI: Rendered directly in the results workspace and notebook timeline.

## NotebookEntry
- Purpose: Append-only event in the research notebook timeline, linking human and automated events to stage runs, approvals, decisions, and artifacts.
- Schema/type definition: `NotebookEntry`
- Lifecycle: Created for every meaningful stage event, approval, decision, warning, branch action, or result publication.
- Persistence strategy: Append-only row with monotonically increasing `notebook_version` per branch; optional notebook snapshot artifact for reconstruction.
- Relation to notebook and UI: Core left-rail timeline primitive and branch comparison anchor.

## Branch
- Purpose: Represents a forkable research path with lineage, head pointers, and notebook version state.
- Schema/type definition: `Branch`
- Lifecycle: Created at investigation start or via fork, updated only for head/version pointers and archival state.
- Persistence strategy: One row per branch, mutable for head pointers and archived status.
- Relation to notebook and UI: Powers branch tree visualization, comparisons, and rerun-from-here flows.

## Warning
- Purpose: Typed, visible warning record for quality, methodology, or execution risks.
- Schema/type definition: `Warning`
- Lifecycle: Emitted by any stage or analysis, never silently deleted; superseding logic should reference new warnings rather than mutate old ones.
- Persistence strategy: Append-only row linked to subject entity or stage run.
- Relation to notebook and UI: Displayed in the right-side inspector and attached to relevant notebook entries and results.

## ProvenanceRecord
- Purpose: Canonical provenance entry describing where a piece of information came from and how it was produced.
- Schema/type definition: `ProvenanceRecord`
- Lifecycle: Created whenever a model, user, system, API, or analysis engine contributes a durable output.
- Persistence strategy: Append-only row; tool call payloads and fingerprints stored in JSON columns.
- Relation to notebook and UI: Fuels provenance inspector panels, citation drawers, and reproducibility exports.

## UserDecision
- Purpose: Captures explicit user steering events such as selecting a hypothesis, approving a plan, or forking a branch.
- Schema/type definition: `UserDecision`
- Lifecycle: Created on every user steering action; later actions supersede but do not mutate prior decisions.
- Persistence strategy: Append-only row with selected entity references and optional payload overrides.
- Relation to notebook and UI: Displayed as decision entries in the notebook and used to reconstruct workflow state.

## StageRun
- Purpose: Immutable execution record for one workflow stage attempt, including inputs, outputs, warnings, provenance, artifacts, and invalidation metadata.
- Schema/type definition: `StageRun`
- Lifecycle: Created at stage start, closed with a terminal status, possibly invalidated later by downstream changes.
- Persistence strategy: Append-only row with JSON references plus explicit invalidation fields; no in-place mutation of outputs.
- Relation to notebook and UI: Backbone of inspectability; every stage workspace and notebook event resolves back to a stage run.

## ApprovalCheckpoint
- Purpose: Human-in-the-loop checkpoint for merge plans, test plans, and future approval gates.
- Schema/type definition: `ApprovalCheckpoint`
- Lifecycle: Opened by a stage run, resolved by a user decision, possibly superseded by a later checkpoint.
- Persistence strategy: Append-only row with explicit request and resolution notebook entry IDs.
- Relation to notebook and UI: Drives approval banners, pending tasks, and review history.

## ArtifactRef
- Purpose: Storage-layer pointer to any durable file or blob used by the notebook, analysis engine, or exports.
- Schema/type definition: `ArtifactRef`
- Lifecycle: Created when a durable payload is written; later artifacts can point to parent artifacts for lineage.
- Persistence strategy: One row per artifact pointer with path, checksum, MIME type, and version metadata.
- Relation to notebook and UI: Enables preview/download links and exact reproduction of stage outputs.

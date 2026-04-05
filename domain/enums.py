from enum import StrEnum


class InvestigationStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class BranchStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    SUPERSEDED = "superseded"


class WorkflowStage(StrEnum):
    INTAKE = "intake"
    PARSE_RESEARCH_QUESTION = "parse_research_question"
    GENERATE_HYPOTHESES = "generate_hypotheses"
    RETRIEVE_EVIDENCE = "retrieve_evidence"
    DISCOVER_DATASETS = "discover_datasets"
    PROFILE_DATASETS = "profile_datasets"
    PROPOSE_MERGE_PLAN = "propose_merge_plan"
    AWAIT_USER_MERGE_APPROVAL = "await_user_merge_approval"
    BUILD_CANONICAL_DATASET = "build_canonical_dataset"
    PROPOSE_TEST_PLAN = "propose_test_plan"
    AWAIT_USER_TEST_APPROVAL = "await_user_test_approval"
    MATERIALIZE_ANALYSIS_DATASET = "materialize_analysis_dataset"
    EXECUTE_ANALYSIS = "execute_analysis"
    SUMMARIZE_RESULTS = "summarize_results"
    PROPOSE_NEXT_STEPS = "propose_next_steps"


class StageRunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INVALIDATED = "invalidated"


class ApprovalStatus(StrEnum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class NotebookEntryKind(StrEnum):
    STAGE = "stage"
    APPROVAL = "approval"
    DECISION = "decision"
    WARNING = "warning"
    RESULT = "result"
    BRANCH = "branch"


class WarningSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class HypothesisStatus(StrEnum):
    PROPOSED = "proposed"
    SELECTED = "selected"
    REJECTED = "rejected"
    SUPERSEDED = "superseded"


class MergeJoinType(StrEnum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    ASOF = "asof"


class TimeAlignmentPolicy(StrEnum):
    SAME_TIMESTAMP = "same_timestamp"
    SAME_DAY = "same_day"
    MONTH_END = "month_end"
    ASOF_PREVIOUS = "asof_previous"
    PUBLICATION_LAG = "publication_lag"


class AnalysisType(StrEnum):
    CORRELATION_SUMMARY = "correlation_summary"
    LINEAR_REGRESSION = "linear_regression"
    ROLLING_REGRESSION = "rolling_regression"
    EVENT_STUDY = "event_study"
    SIMPLE_BACKTEST = "simple_backtest"
    REGIME_SPLIT = "regime_split"


class AnalysisDatasetKind(StrEnum):
    CANONICAL = "canonical"
    MATERIALIZED = "materialized"


class ArtifactKind(StrEnum):
    NOTEBOOK_SNAPSHOT = "notebook_snapshot"
    DATASET_FILE = "dataset_file"
    DATASET_PREVIEW = "dataset_preview"
    TABLE = "table"
    CHART_SPEC = "chart_spec"
    CHART_IMAGE = "chart_image"
    RESULT_BUNDLE = "result_bundle"
    REPORT = "report"
    LOG = "log"
    MODEL_OUTPUT = "model_output"


class ResultArtifactType(StrEnum):
    TABLE = "table"
    CHART = "chart"
    METRIC_BUNDLE = "metric_bundle"
    MEMO = "memo"
    DATASET_PREVIEW = "dataset_preview"


class ProvenanceSourceType(StrEnum):
    USER = "user"
    LLM = "llm"
    SYSTEM = "system"
    DATA_API = "data_api"
    ANALYSIS_ENGINE = "analysis_engine"
    FILING = "filing"
    ARTICLE = "article"


class UserDecisionType(StrEnum):
    SELECT_HYPOTHESIS = "select_hypothesis"
    EDIT_HYPOTHESIS = "edit_hypothesis"
    APPROVE_MERGE_PLAN = "approve_merge_plan"
    OVERRIDE_MERGE_PLAN = "override_merge_plan"
    APPROVE_TEST_PLAN = "approve_test_plan"
    REJECT_TEST_PLAN = "reject_test_plan"
    FORK_BRANCH = "fork_branch"
    RERUN_STAGE = "rerun_stage"
    ADJUST_ANALYSIS = "adjust_analysis"


class EntityKind(StrEnum):
    INVESTIGATION = "investigation"
    RESEARCH_QUESTION = "research_question"
    HYPOTHESIS = "hypothesis"
    EVIDENCE_SOURCE = "evidence_source"
    DATASET_SOURCE = "dataset_source"
    DATASET_PROFILE = "dataset_profile"
    MERGE_PLAN = "merge_plan"
    MERGE_MAPPING = "merge_mapping"
    ANALYSIS_DATASET = "analysis_dataset"
    TEST_PLAN = "test_plan"
    ANALYSIS_RUN = "analysis_run"
    RESULT_ARTIFACT = "result_artifact"
    NOTEBOOK_ENTRY = "notebook_entry"
    BRANCH = "branch"
    WARNING = "warning"
    PROVENANCE_RECORD = "provenance_record"
    USER_DECISION = "user_decision"
    STAGE_RUN = "stage_run"
    APPROVAL_CHECKPOINT = "approval_checkpoint"
    ARTIFACT_REF = "artifact_ref"

from enum import StrEnum


class InvestigationStatus(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkflowStage(StrEnum):
    INTAKE = "intake"
    PARSE_RESEARCH_QUESTION = "parse_research_question"
    GENERATE_HYPOTHESES = "generate_hypotheses"
    RETRIEVE_EVIDENCE = "retrieve_evidence"
    DISCOVER_DATASETS = "discover_datasets"
    PROFILE_DATASETS = "profile_datasets"
    PROPOSE_MERGE_PLAN = "propose_merge_plan"
    AWAIT_USER_MERGE_APPROVAL = "await_user_merge_approval"
    BUILD_ANALYSIS_DATASET = "build_analysis_dataset"
    PROPOSE_TEST_PLAN = "propose_test_plan"
    AWAIT_USER_TEST_APPROVAL = "await_user_test_approval"
    EXECUTE_ANALYSIS = "execute_analysis"
    SUMMARIZE_RESULTS = "summarize_results"
    PROPOSE_NEXT_STEPS = "propose_next_steps"
    NOTEBOOK_COMMIT = "notebook_commit"

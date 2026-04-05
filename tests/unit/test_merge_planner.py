import asyncio

from llm.merge_planner import DeterministicMergePlanner, MergePlanner, MergePlannerDatasetProfile, MergePlannerInput
from orchestration.contracts import ResearchQuestionPlan


def _input() -> MergePlannerInput:
    return MergePlannerInput(
        research_question=ResearchQuestionPlan(
            canonical_question="Investigate whether cooling inflation and falling real yields rotate performance into semiconductors",
            market_universe=["semiconductors", "macro"],
            benchmark="SPY",
            horizon="1-6 months",
            frequency="monthly",
            unit_of_analysis="time series",
        ),
        hypotheses=["Falling real yields should support semiconductors versus defensives."],
        requested_tests=["linear_regression", "rolling_regression"],
        dataset_profiles=[
            MergePlannerDatasetProfile(
                dataset_external_id="SOXX",
                dataset_name="Semiconductor ETF prices",
                provider="yahoo_finance",
                dataset_kind="market_series",
                frequency="daily",
                columns=["date", "symbol", "adjusted_close", "volume"],
                key_candidates=["date", "symbol"],
            ),
            MergePlannerDatasetProfile(
                dataset_external_id="DFII10",
                dataset_name="10Y real yield",
                provider="fred",
                dataset_kind="macro_series",
                frequency="daily",
                columns=["date", "value", "realtime_start", "realtime_end"],
                key_candidates=["date"],
            ),
            MergePlannerDatasetProfile(
                dataset_external_id="CPILFESL",
                dataset_name="Core CPI",
                provider="fred",
                dataset_kind="macro_series",
                frequency="monthly",
                columns=["date", "value"],
                key_candidates=["date"],
            ),
        ],
    )


def test_deterministic_merge_planner_emits_reviewable_plan() -> None:
    planner = DeterministicMergePlanner()
    plan = planner.plan(_input())

    assert len(plan.chosen_datasets) >= 2
    assert plan.join_graph
    assert any(mapping.semantic_role == "time_key" for mapping in plan.mappings)
    assert any(mapping.target_column == "price_adjusted" for mapping in plan.mappings)
    assert plan.date_alignment_strategy
    assert plan.frequency_conversion_strategy
    assert plan.lag_policy
    assert plan.dropped_columns
    assert plan.warnings
    assert 0.0 < plan.confidence <= 1.0


def test_merge_planner_wrapper_uses_fallback_without_openai_client() -> None:
    planner = MergePlanner()
    plan = asyncio.run(planner.plan(_input()))
    assert plan.join_graph
    assert plan.validation_checks

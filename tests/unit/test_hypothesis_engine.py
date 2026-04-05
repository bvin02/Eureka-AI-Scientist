import asyncio

from llm.hypothesis_engine import DeterministicHypothesisEngine, HypothesisEngine
from orchestration.contracts import ResearchQuestionPlan


def sample_question() -> ResearchQuestionPlan:
    return ResearchQuestionPlan(
        canonical_question="Investigate whether cooling inflation and falling real yields rotate leadership into semiconductors.",
        market_universe=["semiconductors", "macro"],
        benchmark="SPY",
        horizon="1-6 months",
        frequency="monthly",
        unit_of_analysis="time series",
    )


def test_deterministic_hypothesis_engine_generates_card_ready_hypotheses() -> None:
    engine = DeterministicHypothesisEngine()
    result = engine.generate(sample_question())

    assert 3 <= len(result.hypotheses) <= 6
    for hypothesis in result.hypotheses:
        assert hypothesis.title
        assert hypothesis.thesis
        assert hypothesis.mechanism
        assert hypothesis.required_variables
        assert hypothesis.preferred_proxies
        assert hypothesis.recommended_test_type is not None
        assert hypothesis.expected_direction
        assert hypothesis.likely_caveats
        assert 0 <= hypothesis.confidence_level <= 1
        assert hypothesis.novelty_usefulness_note


def test_rewrite_preserves_testability_shape() -> None:
    engine = DeterministicHypothesisEngine()
    original = engine.generate(sample_question()).hypotheses[0]
    rewritten = engine.rewrite(original, "Make it more focused on semis versus defensives.")

    assert rewritten.title.endswith("(Edited)")
    assert rewritten.required_variables
    assert rewritten.preferred_proxies
    assert rewritten.novelty_usefulness_note


def test_hypothesis_engine_falls_back_without_openai_client() -> None:
    engine = HypothesisEngine()
    result = asyncio.run(engine.generate(sample_question()))
    assert 3 <= len(result.hypotheses) <= 6

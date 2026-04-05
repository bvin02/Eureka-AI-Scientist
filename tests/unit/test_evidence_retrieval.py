import asyncio

from domain.enums import EvidenceStance
from llm.evidence_retrieval import DeterministicEvidenceRetriever, EvidenceInput, EvidenceRetriever


def test_retrieval_strategy_is_explicit() -> None:
    retriever = DeterministicEvidenceRetriever()
    strategy = retriever.strategy()
    assert strategy.freshness_bias
    assert strategy.source_mix
    assert strategy.ranking_rules
    assert strategy.summarization_rules


def test_deterministic_retriever_returns_ui_ready_cards() -> None:
    retriever = DeterministicEvidenceRetriever()
    result = retriever.retrieve(
        EvidenceInput(
            research_question="Investigate whether cooling inflation and falling real yields rotate leadership into semiconductors.",
            hypothesis_statement="Falling real yields help semiconductors outperform.",
        )
    )

    assert result.evidence_items
    for card in result.evidence_items:
        assert card.title
        assert card.source
        assert card.short_claim_summary
        assert card.relevance_to_hypothesis
        assert card.evidence_stance in {
            EvidenceStance.SUPPORTS,
            EvidenceStance.WEAKLY_SUPPORTS,
            EvidenceStance.CONTRADICTS,
            EvidenceStance.ADJACENT,
        }
        assert card.citation


def test_evidence_retriever_falls_back_without_openai_client() -> None:
    retriever = EvidenceRetriever()
    result = asyncio.run(
        retriever.retrieve(EvidenceInput(research_question="Does labor cooling help small caps?"))
    )
    assert result.evidence_items

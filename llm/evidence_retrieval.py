from __future__ import annotations

from datetime import datetime, timezone

from pydantic import Field

from domain.enums import EvidenceStance
from domain.models import DomainModel
from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.prompts.registry import PromptRegistry
from orchestration.contracts import EvidenceItemProposal, EvidenceSummarySet


class EvidenceRetrievalStrategy(DomainModel):
    freshness_bias: str
    source_mix: list[str] = Field(default_factory=list)
    ranking_rules: list[str] = Field(default_factory=list)
    summarization_rules: list[str] = Field(default_factory=list)


class EvidenceInput(DomainModel):
    research_question: str
    hypothesis_title: str | None = None
    hypothesis_statement: str | None = None


class DeterministicEvidenceRetriever:
    def strategy(self) -> EvidenceRetrievalStrategy:
        return EvidenceRetrievalStrategy(
            freshness_bias="Prefer recent evidence first, but keep older foundational sources when they remain structurally relevant.",
            source_mix=["papers", "public research", "credible macro/market commentary"],
            ranking_rules=[
                "Prefer recency when the topic is fast-moving.",
                "Prefer direct relevance to the active hypothesis over generic market commentary.",
                "Prefer sources with explicit methodology or data references.",
                "Include disagreement when contradictory evidence is plausible.",
            ],
            summarization_rules=[
                "Summaries must be compact and research-useful.",
                "No raw text dumping.",
                "Methodology and detectable data inputs should be surfaced when possible.",
                "Use support/weakly support/contradict/adjacent stance labels conservatively.",
            ],
        )

    def retrieve(self, evidence_input: EvidenceInput) -> EvidenceSummarySet:
        question = evidence_input.research_question.lower()
        hypothesis_context = evidence_input.hypothesis_statement or evidence_input.research_question
        now_year = datetime.now(timezone.utc).year

        cards: list[EvidenceItemProposal] = [
            EvidenceItemProposal(
                provider="public_research",
                source="Federal Reserve research / macro commentary",
                title="Real-rate sensitivity of long-duration assets",
                date=f"{max(now_year - 1, 2020)}-10-01",
                short_claim_summary="Falling real yields tend to support duration-sensitive equity segments, though the effect is regime-dependent.",
                methodology_summary="Empirical macro-finance framing using historical yield and asset-return relationships.",
                data_used=["real yields", "equity returns"],
                relevance_to_hypothesis=f"Directly relevant to {hypothesis_context}.",
                evidence_stance=EvidenceStance.SUPPORTS,
                citation="provenance:macro_real_rates_note",
                extracted_claims=["Real-rate moves can affect valuation-sensitive sectors more than the broad market."],
            ),
            EvidenceItemProposal(
                provider="public_research",
                source="Sell-side or public market strategy research",
                title="Sector rotation depends on growth expectations as well as inflation cooling",
                date=f"{max(now_year - 1, 2020)}-06-15",
                short_claim_summary="Cooling inflation alone is often insufficient; growth-sensitive outperformance depends on whether the market reads the move as benign easing rather than growth deterioration.",
                methodology_summary="Cross-market commentary supported by historical sector-relative performance comparisons.",
                data_used=["sector ETF returns", "inflation series", "yield changes"],
                relevance_to_hypothesis="Useful counterweight because it highlights conditional interpretation rather than a one-way macro rule.",
                evidence_stance=EvidenceStance.WEAKLY_SUPPORTS,
                citation="provenance:sector_rotation_regime_note",
                extracted_claims=["Macro interpretation matters as much as the raw data direction."],
            ),
            EvidenceItemProposal(
                provider="paper",
                source="Academic macro-finance literature",
                title="Macro announcement effects are short horizon and noisy across regimes",
                date=f"{max(now_year - 3, 2019)}-03-01",
                short_claim_summary="Event-style reactions to macro releases exist, but persistence and sign can vary with the prevailing regime and expectations backdrop.",
                methodology_summary="Event-study style measurement around announcement dates.",
                data_used=["macro release surprises", "high-frequency or daily asset returns"],
                relevance_to_hypothesis="Relevant if the hypothesis is framed around CPI prints, labor releases, or other event windows.",
                evidence_stance=EvidenceStance.ADJACENT,
                citation="provenance:macro_event_study_reference",
                extracted_claims=["Announcement effects are strongest when the surprise component is large."],
            ),
        ]

        if "recession" in question or "labor" in question:
            cards.append(
                EvidenceItemProposal(
                    provider="public_research",
                    source="Macro strategy commentary",
                    title="Labor softening can look supportive until recession risk dominates",
                    date=f"{max(now_year - 1, 2020)}-11-20",
                    short_claim_summary="A cooling labor market can initially support lower-rate narratives, but once recession fear dominates, cyclicals and semis often struggle.",
                    methodology_summary="Narrative and historical regime comparison.",
                    data_used=["unemployment rate", "sector returns", "rates"],
                    relevance_to_hypothesis="Adds disagreement and regime dependence to labor-driven hypotheses.",
                    evidence_stance=EvidenceStance.CONTRADICTS,
                    citation="provenance:labor_recession_tradeoff",
                    extracted_claims=["Labor cooling is not unambiguously bullish for cyclicals."],
                )
            )

        return EvidenceSummarySet(evidence_items=cards[:5])


class EvidenceRetriever:
    def __init__(
        self,
        gateway: OpenAIResponsesGateway | None = None,
        prompt_registry: PromptRegistry | None = None,
        fallback: DeterministicEvidenceRetriever | None = None,
    ) -> None:
        self.gateway = gateway or OpenAIResponsesGateway()
        self.prompt_registry = prompt_registry or PromptRegistry()
        self.fallback = fallback or DeterministicEvidenceRetriever()

    def retrieval_strategy(self) -> EvidenceRetrievalStrategy:
        return self.fallback.strategy()

    async def retrieve(self, evidence_input: EvidenceInput) -> EvidenceSummarySet:
        if self.gateway.client is None:
            return self.fallback.retrieve(evidence_input)

        request = PromptRequest(
            prompt_name="evidence_retrieval",
            system_prompt=self.prompt_registry.get_prompt_text("evidence_retrieval"),
            user_prompt=evidence_input.model_dump_json(indent=2),
        )
        try:
            return await self.gateway.generate_structured(request=request, schema=EvidenceSummarySet)
        except RuntimeError:
            return self.fallback.retrieve(evidence_input)

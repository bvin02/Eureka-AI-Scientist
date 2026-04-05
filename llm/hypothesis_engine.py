from __future__ import annotations

from pydantic import Field

from domain.enums import AnalysisType
from domain.models import CaveatRecord, DomainModel
from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.prompts.registry import PromptRegistry
from orchestration.contracts import (
    HypothesisProposal,
    HypothesisProposalSet,
    HypothesisRewriteProposal,
    ResearchQuestionPlan,
)


class HypothesisCardPayload(DomainModel):
    normalized_question: str
    hypotheses: list[HypothesisProposal] = Field(default_factory=list)


class DeterministicHypothesisEngine:
    def generate(self, research_question: ResearchQuestionPlan) -> HypothesisProposalSet:
        question = research_question.canonical_question
        lower = question.lower()
        cards: list[HypothesisProposal] = []

        base_test = AnalysisType.LINEAR_REGRESSION
        if "release" in lower or "cpi" in lower:
            base_test = AnalysisType.EVENT_STUDY
        elif "regime" in lower or "recession" in lower:
            base_test = AnalysisType.REGIME_SPLIT

        cards.append(
            HypothesisProposal(
                label="primary",
                title="Primary macro transmission",
                thesis=f"{question} through a measurable macro-to-market transmission channel.",
                mechanism="Changes in macro expectations alter discount rates and sector-relative earnings expectations.",
                required_variables=["target returns", "core macro driver", "benchmark returns"],
                preferred_proxies=["SOXX or XLK returns", "DFII10", "CPILFESL"],
                recommended_test_type=base_test,
                expected_direction="direction depends on the sign of the macro move, but should be stable in the proposed specification",
                target_assets=["SOXX", "XLK"],
                explanatory_variables=["real yields", "inflation"],
                likely_caveats=[
                    "Proxy selection may dominate the result.",
                    "Macro releases require explicit publication-lag handling.",
                ],
                confidence_level=0.74,
                novelty_usefulness_note="Good default card for a first-pass quant investigation.",
            )
        )

        cards.append(
            HypothesisProposal(
                label="counterfactual",
                title="Counterfactual defensive rotation",
                thesis="If the primary thesis is wrong, defensive sectors should benefit more than growth-sensitive assets under the same macro shift.",
                mechanism="Investors may interpret the macro signal as slower growth rather than lower discount rates.",
                required_variables=["defensive sector returns", "growth sector returns", "macro driver"],
                preferred_proxies=["XLP or XLU returns", "XLK or SOXX returns", "DFII10", "CPILFESL"],
                recommended_test_type=AnalysisType.LINEAR_REGRESSION,
                expected_direction="defensives outperform when the macro signal is interpreted as growth-negative",
                target_assets=["XLP", "XLU", "XLK", "SOXX"],
                explanatory_variables=["real yields", "inflation"],
                likely_caveats=[
                    "Sector composition effects may confound style interpretation.",
                    "Relative-value framing can hide broad market beta effects.",
                ],
                confidence_level=0.61,
                novelty_usefulness_note="Useful falsification card that forces clearer interpretation.",
            )
        )

        cards.append(
            HypothesisProposal(
                label="regime",
                title="Regime-conditional sensitivity",
                thesis="The relationship should be stronger in specific macro regimes than in unconditional full-sample analysis.",
                mechanism="Transmission strength changes when inflation, policy, or recession risk dominates the narrative.",
                required_variables=["target returns", "macro driver", "regime classifier"],
                preferred_proxies=["sector ETF returns", "real yield changes", "UNRATE or yield-curve slope"],
                recommended_test_type=AnalysisType.REGIME_SPLIT,
                expected_direction="stronger and cleaner directional effect within selected regimes",
                target_assets=["SOXX", "XLK", "SPY"],
                explanatory_variables=["real yields", "inflation", "recession risk"],
                likely_caveats=[
                    "Regime definitions may be unstable or ex post.",
                    "Smaller sample sizes can weaken inference.",
                ],
                confidence_level=0.68,
                novelty_usefulness_note="High usefulness for separating unconditional noise from conditional signal.",
            )
        )

        if "rotation" in lower or "outperform" in lower or "leadership" in lower:
            cards.append(
                HypothesisProposal(
                    label="tradeable",
                    title="Tradeable rotation rule",
                    thesis="The macro signal can be turned into a simple sector-rotation rule with measurable out-of-sample value.",
                    mechanism="Macro information updates allocation preferences between growth-sensitive and defensive exposures.",
                    required_variables=["signal variable", "asset returns", "rebalance schedule"],
                    preferred_proxies=["macro release or monthly macro change", "SOXX", "XLP", "SPY"],
                    recommended_test_type=AnalysisType.SIMPLE_BACKTEST,
                    expected_direction="rule-based allocation outperforms a passive benchmark under realistic assumptions",
                    target_assets=["SOXX", "XLP", "SPY"],
                    explanatory_variables=["real yields", "inflation"],
                    likely_caveats=[
                        "Backtests are sensitive to timing and transaction assumptions.",
                        "Signal definitions may overfit narrative episodes.",
                    ],
                    confidence_level=0.55,
                    novelty_usefulness_note="Lower certainty, but useful for demo value and judge-facing storytelling.",
                )
            )

        cards = cards[:6]
        return HypothesisProposalSet(hypotheses=cards)

    def rewrite(
        self,
        existing: HypothesisProposal | HypothesisRewriteProposal,
        user_instruction: str,
    ) -> HypothesisRewriteProposal:
        note = user_instruction.strip() or "Edited by user."
        return HypothesisRewriteProposal(
            title=f"{existing.title} (Edited)",
            thesis=f"{existing.thesis} Revision note: {note}",
            mechanism=existing.mechanism,
            required_variables=list(existing.required_variables),
            preferred_proxies=list(existing.preferred_proxies),
            recommended_test_type=existing.recommended_test_type,
            expected_direction=existing.expected_direction,
            explanatory_variables=list(existing.explanatory_variables),
            likely_caveats=list(existing.likely_caveats),
            confidence_level=max(0.3, min(float(existing.confidence_level), 0.95)),
            novelty_usefulness_note=f"{existing.novelty_usefulness_note} Edited for a user-specific angle.",
        )


class HypothesisEngine:
    def __init__(
        self,
        gateway: OpenAIResponsesGateway | None = None,
        prompt_registry: PromptRegistry | None = None,
        fallback: DeterministicHypothesisEngine | None = None,
    ) -> None:
        self.gateway = gateway or OpenAIResponsesGateway()
        self.prompt_registry = prompt_registry or PromptRegistry()
        self.fallback = fallback or DeterministicHypothesisEngine()

    async def generate(self, research_question: ResearchQuestionPlan) -> HypothesisProposalSet:
        if self.gateway.client is None:
            return self.fallback.generate(research_question)

        request = PromptRequest(
            prompt_name="hypothesis_engine",
            system_prompt=self.prompt_registry.get_prompt_text("hypothesis_engine"),
            user_prompt=research_question.model_dump_json(indent=2),
        )
        try:
            return await self.gateway.generate_structured(request=request, schema=HypothesisProposalSet)
        except RuntimeError:
            return self.fallback.generate(research_question)

    async def rewrite(
        self,
        existing: HypothesisProposal | HypothesisRewriteProposal,
        user_instruction: str,
    ) -> HypothesisRewriteProposal:
        if self.gateway.client is None:
            return self.fallback.rewrite(existing, user_instruction)

        request = PromptRequest(
            prompt_name="hypothesis_engine_rewrite",
            system_prompt=(
                self.prompt_registry.get_prompt_text("hypothesis_engine")
                + "\nRewrite the existing hypothesis according to the user's instruction while keeping it testable."
            ),
            user_prompt=(
                "Existing hypothesis:\n"
                f"{existing.model_dump_json(indent=2)}\n\n"
                "Rewrite instruction:\n"
                f"{user_instruction}"
            ),
        )
        try:
            return await self.gateway.generate_structured(request=request, schema=HypothesisRewriteProposal)
        except RuntimeError:
            return self.fallback.rewrite(existing, user_instruction)

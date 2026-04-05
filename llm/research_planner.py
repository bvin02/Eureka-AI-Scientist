from __future__ import annotations

from collections.abc import Iterable

from pydantic import Field

from domain.models import CaveatRecord, DomainModel
from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.prompts.registry import PromptRegistry
from orchestration.contracts import ResearchQuestionPlan


class PlannerEntity(DomainModel):
    canonical_name: str
    entity_type: str
    original_text: str
    interpretation: str
    confidence: float


class CandidateProxy(DomainModel):
    concept: str
    proxy_name: str
    justification: str
    candidate_series: list[str] = Field(default_factory=list)
    confidence: float


class MethodologyOption(DomainModel):
    name: str
    rationale: str
    suitability: str


class CandidateDataSource(DomainModel):
    provider: str
    dataset_hint: str
    rationale: str


class PlannerPathOption(DomainModel):
    label: str
    summary: str
    target_variable: str
    explanatory_focus: list[str] = Field(default_factory=list)
    methodologies: list[str] = Field(default_factory=list)
    data_sources: list[str] = Field(default_factory=list)
    tradeoffs: list[str] = Field(default_factory=list)


class ResearchPlannerOutput(DomainModel):
    normalized_question: str
    target_variable: str
    explanatory_variables: list[str] = Field(default_factory=list)
    structured_entities: list[PlannerEntity] = Field(default_factory=list)
    candidate_proxies: list[CandidateProxy] = Field(default_factory=list)
    likely_time_horizon: str
    likely_data_frequency: str
    possible_methodologies: list[MethodologyOption] = Field(default_factory=list)
    confounders_caveats: list[str] = Field(default_factory=list)
    candidate_data_sources: list[CandidateDataSource] = Field(default_factory=list)
    path_options: list[PlannerPathOption] = Field(default_factory=list)
    ambiguity_notes: list[str] = Field(default_factory=list)


class DeterministicResearchPlanner:
    def plan(self, question: str) -> ResearchPlannerOutput:
        prompt = question.strip() or "Untitled quant research question"
        lower = prompt.lower()

        entities: list[PlannerEntity] = []
        proxies: list[CandidateProxy] = []
        explanatory_variables: list[str] = []
        confounders: list[str] = ["Correlation may reflect broader macro regime shifts rather than a clean causal effect."]
        methodologies = [
            MethodologyOption(
                name="correlation_summary",
                rationale="Establish directional co-movement before stronger claims.",
                suitability="good baseline",
            ),
            MethodologyOption(
                name="linear_regression",
                rationale="Estimate sensitivity of the target variable to explanatory drivers.",
                suitability="recommended",
            ),
            MethodologyOption(
                name="rolling_regression",
                rationale="Check stability across changing macro regimes.",
                suitability="recommended",
            ),
        ]
        data_sources = [
            CandidateDataSource(
                provider="FRED",
                dataset_hint="macro series relevant to the prompt",
                rationale="Primary source for inflation, yields, labor, and recession-sensitive macro data.",
            ),
            CandidateDataSource(
                provider="Yahoo Finance",
                dataset_hint="asset or sector price history",
                rationale="Simple source for equity, ETF, and benchmark return series.",
            ),
        ]

        target_variable = "forward sector or asset returns"
        likely_horizon = "1-6 months"
        likely_frequency = "monthly"
        ambiguity_notes: list[str] = []

        def add_entity(name: str, entity_type: str, original_text: str, interpretation: str, confidence: float = 0.85) -> None:
            entities.append(
                PlannerEntity(
                    canonical_name=name,
                    entity_type=entity_type,
                    original_text=original_text,
                    interpretation=interpretation,
                    confidence=confidence,
                )
            )

        def add_proxy(concept: str, proxy_name: str, justification: str, candidate_series: Iterable[str], confidence: float = 0.8) -> None:
            proxies.append(
                CandidateProxy(
                    concept=concept,
                    proxy_name=proxy_name,
                    justification=justification,
                    candidate_series=list(candidate_series),
                    confidence=confidence,
                )
            )

        if "inflation" in lower or "cpi" in lower:
            add_entity("inflation", "macro_factor", "inflation", "Price-level or inflation-trend driver.")
            explanatory_variables.append("inflation")
            add_proxy("inflation", "core CPI", "Monthly core inflation proxy.", ["CPILFESL"])
            confounders.append("Inflation data are released with publication lag and revision risk is limited but timing still matters.")

        if "yield" in lower or "rates" in lower or "real yield" in lower:
            add_entity("yields", "macro_factor", "yields", "Rate or discount-rate channel.")
            explanatory_variables.append("yields")
            add_proxy("real yields", "10Y real yield", "Real discount-rate proxy for long-duration assets.", ["DFII10"])
            likely_frequency = "daily or monthly"
            confounders.append("Yield changes may proxy for growth expectations, inflation expectations, or policy expectations.")

        if "growth" in lower or "growth stocks" in lower:
            add_entity("growth stocks", "asset_bucket", "growth stocks", "Long-duration equity style exposure.")
            target_variable = "relative performance of growth-oriented equities"
            add_proxy("growth stocks", "QQQ or XLK returns", "Liquid proxies for growth-heavy equity exposure.", ["QQQ", "XLK"])

        if "semi" in lower or "semis" in lower or "semiconductor" in lower:
            add_entity("semiconductors", "sector", "semis", "Cyclical long-duration tech subsector.")
            target_variable = "semiconductor sector returns or relative returns"
            add_proxy("semiconductors", "SOXX returns", "Liquid sector ETF proxy for semiconductors.", ["SOXX"])

        if "recession" in lower:
            add_entity("recession", "macro_regime", "recession", "Growth slowdown or downturn regime.")
            explanatory_variables.append("recession risk")
            add_proxy("recession risk", "yield curve slope and unemployment trend", "Practical recession-risk proxies.", ["T10Y2Y", "UNRATE"])
            confounders.append("Recession classification is often ex post, so real-time regime identification can be noisy.")

        if "labor" in lower or "employment" in lower or "jobs" in lower:
            add_entity("labor market", "macro_factor", "labor data", "Employment and wage conditions.")
            explanatory_variables.append("labor market")
            add_proxy("labor market", "unemployment rate and payroll growth", "Standard labor-condition proxies.", ["UNRATE", "PAYEMS"])

        if not explanatory_variables:
            explanatory_variables = ["macro regime", "market discount rate", "earnings sensitivity"]
            ambiguity_notes.append("The prompt leaves explanatory drivers partially implicit, so the planner proposed broad macro-market drivers.")

        if "daily" not in lower and likely_frequency == "daily or monthly":
            ambiguity_notes.append("Daily and monthly frequencies are both plausible; recommended path should default to monthly to simplify lag handling.")
            likely_frequency = "monthly"

        if "long term" in lower or "multi-year" in lower:
            likely_horizon = "6-24 months"
        elif "event" in lower or "release" in lower:
            likely_horizon = "1-10 trading days"
            likely_frequency = "daily"
            methodologies.append(
                MethodologyOption(
                    name="event_study",
                    rationale="Appropriate for release-date or catalyst framing.",
                    suitability="recommended when event timing is explicit",
                )
            )

        if "backtest" in lower or "rotation" in lower:
            methodologies.append(
                MethodologyOption(
                    name="simple_backtest",
                    rationale="Useful when the question implies a tradeable rotation rule.",
                    suitability="aggressive",
                )
            )

        if "regime" in lower or "recession" in lower:
            methodologies.append(
                MethodologyOption(
                    name="regime_split",
                    rationale="Useful when the question is conditional on macro state.",
                    suitability="recommended",
                )
            )

        normalized_question = prompt.rstrip("?")
        if not normalized_question.lower().startswith(("investigate", "test whether", "evaluate whether")):
            normalized_question = f"Investigate whether {normalized_question[0].lower() + normalized_question[1:]}" if normalized_question else "Investigate the research question"

        if len(entities) < 2:
            ambiguity_notes.append("The prompt is broad enough that several structured entities were inferred rather than explicitly stated.")

        path_options = [
            PlannerPathOption(
                label="conservative",
                summary="Use monthly data, a narrow variable set, and descriptive plus baseline regression methods.",
                target_variable=target_variable,
                explanatory_focus=explanatory_variables[:2],
                methodologies=["correlation_summary", "linear_regression"],
                data_sources=["FRED", "Yahoo Finance"],
                tradeoffs=["Higher interpretability", "Lower risk of overfitting", "May miss short-horizon timing effects"],
            ),
            PlannerPathOption(
                label="recommended",
                summary="Use a lag-aware monthly core specification with regression and rolling/regime diagnostics.",
                target_variable=target_variable,
                explanatory_focus=explanatory_variables,
                methodologies=[item.name for item in methodologies[:3]],
                data_sources=["FRED", "Yahoo Finance"],
                tradeoffs=["Balanced rigor and feasibility", "Good demo path", "Still sensitive to proxy choice"],
            ),
            PlannerPathOption(
                label="aggressive",
                summary="Add higher-frequency timing, event studies, and trade-style validation where the prompt supports it.",
                target_variable=target_variable,
                explanatory_focus=explanatory_variables,
                methodologies=[item.name for item in methodologies] + ["event_study"],
                data_sources=["FRED", "Yahoo Finance", "SEC EDGAR"],
                tradeoffs=["Richer story", "Higher implementation and leakage risk", "Requires stricter timing assumptions"],
            ),
        ]

        if "rotation" in lower or "sector" in lower or "style" in lower:
            data_sources.append(
                CandidateDataSource(
                    provider="Yahoo Finance",
                    dataset_hint="sector ETFs or style proxies",
                    rationale="Useful for relative performance or rotation questions.",
                )
            )

        return ResearchPlannerOutput(
            normalized_question=normalized_question,
            target_variable=target_variable,
            explanatory_variables=explanatory_variables,
            structured_entities=entities,
            candidate_proxies=proxies,
            likely_time_horizon=likely_horizon,
            likely_data_frequency=likely_frequency,
            possible_methodologies=methodologies,
            confounders_caveats=confounders,
            candidate_data_sources=data_sources,
            path_options=path_options,
            ambiguity_notes=ambiguity_notes,
        )


class ResearchPlanner:
    def __init__(
        self,
        gateway: OpenAIResponsesGateway | None = None,
        prompt_registry: PromptRegistry | None = None,
        fallback: DeterministicResearchPlanner | None = None,
    ) -> None:
        self.gateway = gateway or OpenAIResponsesGateway()
        self.prompt_registry = prompt_registry or PromptRegistry()
        self.fallback = fallback or DeterministicResearchPlanner()

    async def plan(self, question: str) -> ResearchPlannerOutput:
        if self.gateway.client is None:
            return self.fallback.plan(question)

        request = PromptRequest(
            prompt_name="research_planner",
            system_prompt=self.prompt_registry.get_prompt_text("research_planner"),
            user_prompt=question,
        )
        try:
            return await self.gateway.generate_structured(request=request, schema=ResearchPlannerOutput)
        except RuntimeError:
            return self.fallback.plan(question)


def planner_output_to_research_question_plan(output: ResearchPlannerOutput) -> ResearchQuestionPlan:
    return ResearchQuestionPlan(
        canonical_question=output.normalized_question,
        market_universe=[entity.canonical_name for entity in output.structured_entities if entity.entity_type in {"asset_bucket", "sector", "macro_factor", "macro_regime"}],
        benchmark="SPY",
        horizon=output.likely_time_horizon,
        frequency=output.likely_data_frequency,
        unit_of_analysis="time series",
        success_criteria=[path.summary for path in output.path_options if path.label == "recommended"] or ["quant research plan defined"],
        caveats=[
            CaveatRecord(
                label="planner_caveat",
                detail=item,
            )
            for item in output.confounders_caveats[:3]
        ],
    )

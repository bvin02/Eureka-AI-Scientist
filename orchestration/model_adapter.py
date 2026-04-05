from __future__ import annotations

from typing import Protocol

from domain.enums import AnalysisType
from llm.canonical_dataset_builder import CanonicalDatasetBuilder
from llm.client import OpenAIResponsesGateway
from llm.contracts import PromptRequest
from llm.evidence_retrieval import EvidenceInput, EvidenceRetriever
from llm.hypothesis_engine import HypothesisEngine
from llm.merge_planner import MergePlanner, MergePlannerInput
from llm.research_planner import ResearchPlanner, planner_output_to_research_question_plan
from orchestration.contracts import (
    DatasetCandidate,
    DatasetDiscoverySet,
    DatasetProfileProposal,
    DatasetProfileSet,
    EvidenceSummarySet,
    HypothesisProposalSet,
    MergePlanProposal,
    NextStepProposal,
    ResearchQuestionPlan,
    ResultSummaryProposal,
    TestPlanProposal,
    AnalysisSpecProposal,
    CanonicalBuildInput,
    CanonicalBuildPlanProposal,
)


class WorkflowModelAdapter(Protocol):
    async def parse_research_question(self, raw_prompt: str) -> ResearchQuestionPlan: ...
    async def generate_hypotheses(self, canonical_question: str) -> HypothesisProposalSet: ...
    async def retrieve_evidence(self, canonical_question: str) -> EvidenceSummarySet: ...
    async def discover_datasets(self, canonical_question: str) -> DatasetDiscoverySet: ...
    async def profile_datasets(self, dataset_names: list[str]) -> DatasetProfileSet: ...
    async def propose_merge_plan(self, planner_input: MergePlannerInput) -> MergePlanProposal: ...
    async def plan_canonical_dataset_build(self, build_input: CanonicalBuildInput) -> CanonicalBuildPlanProposal: ...
    async def propose_test_plan(self, question: str) -> TestPlanProposal: ...
    async def summarize_results(self, question: str) -> ResultSummaryProposal: ...
    async def propose_next_steps(self, question: str) -> NextStepProposal: ...


class DeterministicWorkflowModelAdapter:
    def __init__(
        self,
        planner: ResearchPlanner | None = None,
        hypothesis_engine: HypothesisEngine | None = None,
        evidence_retriever: EvidenceRetriever | None = None,
        merge_planner: MergePlanner | None = None,
    ) -> None:
        self.planner = planner or ResearchPlanner()
        self.hypothesis_engine = hypothesis_engine or HypothesisEngine()
        self.evidence_retriever = evidence_retriever or EvidenceRetriever()
        self.merge_planner = merge_planner or MergePlanner()
        self.canonical_builder = CanonicalDatasetBuilder()

    async def parse_research_question(self, raw_prompt: str) -> ResearchQuestionPlan:
        planner_output = self.planner.fallback.plan(raw_prompt)
        return planner_output_to_research_question_plan(planner_output)

    async def generate_hypotheses(self, canonical_question: str) -> HypothesisProposalSet:
        research_question = ResearchQuestionPlan(
            canonical_question=canonical_question,
            market_universe=["macro", "equities"],
            benchmark="SPY",
            horizon="1-6 months",
            frequency="monthly",
            unit_of_analysis="time series",
        )
        return await self.hypothesis_engine.generate(research_question)

    async def retrieve_evidence(self, canonical_question: str) -> EvidenceSummarySet:
        return await self.evidence_retriever.retrieve(
            EvidenceInput(research_question=canonical_question)
        )

    async def discover_datasets(self, canonical_question: str) -> DatasetDiscoverySet:
        question = canonical_question.lower()
        market_name = "SOXX prices" if "semi" in question or "growth" in question else "SPY prices"
        return DatasetDiscoverySet(
            datasets=[
                DatasetCandidate(
                    provider="fred",
                    external_id="DFII10",
                    name="10Y Treasury Inflation-Indexed Security, Constant Maturity",
                    description="Proxy for 10-year real yields.",
                    dataset_kind="macro_series",
                    entity_grain="market",
                    time_grain="date",
                    frequency="daily",
                ),
                DatasetCandidate(
                    provider="fred",
                    external_id="CPILFESL",
                    name="Core CPI",
                    description="Core CPI index.",
                    dataset_kind="macro_series",
                    entity_grain="market",
                    time_grain="date",
                    frequency="monthly",
                ),
                DatasetCandidate(
                    provider="yahoo_finance",
                    external_id="SOXX",
                    name=market_name,
                    description="Market price series for sector comparison.",
                    dataset_kind="market_series",
                    entity_grain="ticker",
                    time_grain="date",
                    frequency="daily",
                ),
            ]
        )

    async def profile_datasets(self, dataset_names: list[str]) -> DatasetProfileSet:
        profiles = []
        for name in dataset_names:
            profiles.append(
                DatasetProfileProposal(
                    dataset_external_id=name,
                    row_count=120,
                    key_candidates=["date"],
                    quality_flags=["deterministic_profile"],
                )
            )
        return DatasetProfileSet(profiles=profiles)

    async def propose_merge_plan(self, planner_input: MergePlannerInput) -> MergePlanProposal:
        return self.merge_planner.fallback.plan(planner_input)

    async def plan_canonical_dataset_build(self, build_input: CanonicalBuildInput) -> CanonicalBuildPlanProposal:
        return self.canonical_builder.fallback.build_plan(build_input)

    async def propose_test_plan(self, question: str) -> TestPlanProposal:
        return TestPlanProposal(
            title="Base macro-market test plan",
            objective=f"Evaluate whether the research thesis is supported: {question}",
            analyses=[
                AnalysisSpecProposal(
                    analysis_type=AnalysisType.CORRELATION_SUMMARY,
                    title="Correlation and summary diagnostics",
                    objective="Inspect relationships and basic stability.",
                ),
                AnalysisSpecProposal(
                    analysis_type=AnalysisType.LINEAR_REGRESSION,
                    title="Linear regression",
                    objective="Estimate directional sensitivity of forward returns.",
                    dependent_variable="forward_return",
                    independent_variables=["real_yield_change", "core_cpi_change"],
                ),
                AnalysisSpecProposal(
                    analysis_type=AnalysisType.ROLLING_REGRESSION,
                    title="Rolling regression",
                    objective="Check whether sensitivity is regime-stable.",
                    dependent_variable="forward_return",
                    independent_variables=["real_yield_change"],
                    parameters={"window": 24},
                ),
            ],
        )

    async def summarize_results(self, question: str) -> ResultSummaryProposal:
        return ResultSummaryProposal(
            summary=f"Deterministic summary placeholder for {question}",
            key_findings=["Analysis execution completed with placeholder metrics."],
            warnings=["Replace deterministic stage adapter with live model- and data-backed execution for production."],
        )

    async def propose_next_steps(self, question: str) -> NextStepProposal:
        return NextStepProposal(
            summary=f"Next steps for {question}",
            next_steps=[
                "Fork branch to test alternative lag assumptions.",
                "Run regime split on falling vs rising real-yield environments.",
                "Expand evidence retrieval to filings or research notes.",
            ],
        )


class ResponsesWorkflowModelAdapter:
    def __init__(self, gateway: OpenAIResponsesGateway, fallback: WorkflowModelAdapter | None = None) -> None:
        self.gateway = gateway
        self.fallback = fallback or DeterministicWorkflowModelAdapter()
        self.planner = ResearchPlanner(gateway=gateway)
        self.hypothesis_engine = HypothesisEngine(gateway=gateway)
        self.evidence_retriever = EvidenceRetriever(gateway=gateway)
        self.merge_planner = MergePlanner(gateway=gateway)
        self.canonical_builder = CanonicalDatasetBuilder(gateway=gateway)

    async def _generate(self, prompt_name: str, system_prompt: str, user_prompt: str, schema):
        if self.gateway.client is None:
            return None
        request = PromptRequest(
            prompt_name=prompt_name,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        return await self.gateway.generate_structured(request=request, schema=schema)

    async def parse_research_question(self, raw_prompt: str) -> ResearchQuestionPlan:
        planner_output = await self.planner.plan(raw_prompt)
        return planner_output_to_research_question_plan(planner_output)

    async def generate_hypotheses(self, canonical_question: str) -> HypothesisProposalSet:
        research_question = ResearchQuestionPlan(
            canonical_question=canonical_question,
            market_universe=["macro", "equities"],
            benchmark="SPY",
            horizon="1-6 months",
            frequency="monthly",
            unit_of_analysis="time series",
        )
        return await self.hypothesis_engine.generate(research_question)

    async def retrieve_evidence(self, canonical_question: str) -> EvidenceSummarySet:
        return await self.evidence_retriever.retrieve(
            EvidenceInput(research_question=canonical_question)
        )

    async def discover_datasets(self, canonical_question: str) -> DatasetDiscoverySet:
        result = await self._generate(
            "discover_datasets",
            "Discover structured dataset candidates for a quant research investigation.",
            canonical_question,
            DatasetDiscoverySet,
        )
        return result or await self.fallback.discover_datasets(canonical_question)

    async def profile_datasets(self, dataset_names: list[str]) -> DatasetProfileSet:
        result = await self._generate(
            "profile_datasets",
            "Propose structured dataset profile summaries for the provided datasets.",
            ", ".join(dataset_names),
            DatasetProfileSet,
        )
        return result or await self.fallback.profile_datasets(dataset_names)

    async def propose_merge_plan(self, planner_input: MergePlannerInput) -> MergePlanProposal:
        return await self.merge_planner.plan(planner_input)

    async def plan_canonical_dataset_build(self, build_input: CanonicalBuildInput) -> CanonicalBuildPlanProposal:
        return await self.canonical_builder.build_plan(build_input)

    async def propose_test_plan(self, question: str) -> TestPlanProposal:
        result = await self._generate(
            "propose_test_plan",
            "Generate a structured test plan for the canonical research question.",
            question,
            TestPlanProposal,
        )
        return result or await self.fallback.propose_test_plan(question)

    async def summarize_results(self, question: str) -> ResultSummaryProposal:
        result = await self._generate(
            "summarize_results",
            "Generate a structured summary of the analysis results.",
            question,
            ResultSummaryProposal,
        )
        return result or await self.fallback.summarize_results(question)

    async def propose_next_steps(self, question: str) -> NextStepProposal:
        result = await self._generate(
            "propose_next_steps",
            "Generate structured next-step proposals for a quant investigation.",
            question,
            NextStepProposal,
        )
        return result or await self.fallback.propose_next_steps(question)

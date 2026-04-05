# Eureka Hypothesis Engine Notes

The hypothesis engine produces 3 to 6 card-ready hypotheses for a structured research question.

Each hypothesis includes:
- title
- plain-English hypothesis statement
- mechanism
- required variables
- preferred proxies
- recommended test type
- expected directionality
- likely caveats
- confidence level
- novelty/usefulness note

Integration points:
- Direct engine: `llm/hypothesis_engine.py`
- Workflow generation stage: `orchestration/engine.py`
- User rewrite flow: `WorkflowEngine.edit_hypothesis(...)`
- Branch-from-card flow: `WorkflowEngine.fork_from_hypothesis(...)`

# Eureka Evidence Retrieval Strategy

## Strategy
- Prefer fresh evidence first when the topic is time-sensitive.
- Blend:
  - papers
  - public research
  - credible macro or market commentary
- Rank by:
  - direct relevance to the active research question or hypothesis
  - recency
  - explicit methodology or data usage
  - whether the source adds support, contradiction, or useful adjacency

## Card design
Each card is compact and UI-ready:
- title
- source
- date
- short claim summary
- methodology summary
- data used
- relevance to current hypothesis
- support stance
- citation or provenance reference

## Notes
- The current implementation is a typed retrieval-and-summarization layer with deterministic fallback.
- It is designed so a live source search layer can later feed retrieved snippets into the same card schema without changing downstream workflow or UI contracts.

# Eureka Research Planner Examples

These are deterministic fallback planner outputs for five sample quant prompts.

## 1. Cooling inflation and falling real yields
Prompt: `Will cooling inflation and falling real yields rotate leadership into semiconductors and growth stocks?`

- Normalized question: `Investigate whether cooling inflation and falling real yields rotate leadership into semiconductors and growth stocks`
- Target variable: `relative performance of growth-oriented equities`
- Explanatory variables: `inflation`, `yields`
- Likely horizon: `1-6 months`
- Likely data frequency: `monthly`
- Recommended methodologies: `correlation_summary`, `linear_regression`, `rolling_regression`
- Candidate data sources: `FRED`, `Yahoo Finance`
- Recommended path: lag-aware monthly regression plus rolling diagnostics

## 2. Labor cooling and small caps
Prompt: `Does a cooling labor market help small caps outperform mega-cap tech?`

- Normalized question: `Investigate whether does a cooling labor market help small caps outperform mega-cap tech`
- Target variable: `forward sector or asset returns`
- Explanatory variables: `labor market`
- Likely horizon: `1-6 months`
- Likely data frequency: `monthly`
- Recommended methodologies: `correlation_summary`, `linear_regression`, `rolling_regression`
- Candidate data sources: `FRED`, `Yahoo Finance`
- Recommended path: monthly labor proxy versus relative-return specification

## 3. Recession risk and defensives
Prompt: `How does rising recession risk affect defensives versus cyclicals?`

- Normalized question: `Investigate whether how does rising recession risk affect defensives versus cyclicals`
- Target variable: `forward sector or asset returns`
- Explanatory variables: `recession risk`
- Likely horizon: `1-6 months`
- Likely data frequency: `monthly`
- Recommended methodologies: `correlation_summary`, `linear_regression`, `rolling_regression`, `regime_split`
- Candidate data sources: `FRED`, `Yahoo Finance`
- Recommended path: recession-proxy regime split with baseline regressions

## 4. CPI release event question
Prompt: `Do soft CPI releases create a short-term rally in long-duration tech stocks?`

- Normalized question: `Investigate whether do soft CPI releases create a short-term rally in long-duration tech stocks`
- Target variable: `forward sector or asset returns`
- Explanatory variables: `inflation`
- Likely horizon: `1-10 trading days`
- Likely data frequency: `daily`
- Recommended methodologies: `correlation_summary`, `linear_regression`, `rolling_regression`, `event_study`
- Candidate data sources: `FRED`, `Yahoo Finance`
- Recommended path: CPI release event study with short forward-return windows

## 5. Yield curve and bank stocks
Prompt: `Does a steeper yield curve improve bank stock performance over the next quarter?`

- Normalized question: `Investigate whether does a steeper yield curve improve bank stock performance over the next quarter`
- Target variable: `forward sector or asset returns`
- Explanatory variables: `yields`
- Likely horizon: `1-6 months`
- Likely data frequency: `monthly`
- Recommended methodologies: `correlation_summary`, `linear_regression`, `rolling_regression`
- Candidate data sources: `FRED`, `Yahoo Finance`
- Recommended path: monthly slope proxy versus bank-sector forward returns

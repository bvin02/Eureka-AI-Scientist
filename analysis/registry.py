from domain.models import AnalysisMethodDescriptor


def supported_analyses() -> list[AnalysisMethodDescriptor]:
    return [
        AnalysisMethodDescriptor(
            key="correlation_summary",
            label="Correlation / Summary",
            description="Descriptive statistics, correlations, and simple exploratory summaries.",
        ),
        AnalysisMethodDescriptor(
            key="linear_regression",
            label="Linear Regression",
            description="Cross-sectional or time-series regression with explicit dependent, independent, and lag assumptions.",
        ),
        AnalysisMethodDescriptor(
            key="rolling_regression",
            label="Rolling Regression",
            description="Windowed regressions for stability and regime diagnostics across time.",
        ),
        AnalysisMethodDescriptor(
            key="event_study",
            label="Event Study",
            description="Return behavior around macro releases, filings, or custom event dates.",
        ),
        AnalysisMethodDescriptor(
            key="simple_backtest",
            label="Simple Backtest",
            description="Transparent rule-based portfolio simulation with visible timing and transaction assumptions.",
        ),
        AnalysisMethodDescriptor(
            key="regime_split",
            label="Regime Split",
            description="Segmented analysis conditioned on macro or market regimes.",
        ),
    ]

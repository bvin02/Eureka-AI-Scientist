from __future__ import annotations

import asyncio

from data.adapters.registry import AdapterRegistry
from data.models import DatasetDiscoveryNeed


async def example_usage() -> dict[str, object]:
    registry = AdapterRegistry()
    need = DatasetDiscoveryNeed(
        research_need="Investigate whether cooling inflation and falling real yields rotate leadership into semiconductors.",
        target_assets=["semis", "growth"],
        explanatory_variables=["inflation", "real yields"],
        preferred_frequency="monthly",
    )

    fred = registry.get("fred")
    yahoo = registry.get("yahoo_finance")
    edgar = registry.get("sec_edgar")
    return {
        "fred_discovery": await fred.discover(need),
        "yahoo_discovery": await yahoo.discover(need),
        "edgar_discovery": await edgar.discover(need),
    }


if __name__ == "__main__":
    print(asyncio.run(example_usage()))

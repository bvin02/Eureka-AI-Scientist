import argparse
import asyncio

from infra.logging import configure_logging, get_logger
from infra.settings import get_settings
from orchestration.state_machine import WorkflowDefinition


async def run(once: bool = False) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("eureka.worker")
    workflow = WorkflowDefinition()

    logger.info("worker online with %s stages", len(workflow.ordered_stages()))
    logger.info("approval stages: %s", ", ".join(stage.value for stage in workflow.approval_stages()))

    if once:
        return

    while True:
        logger.info("worker heartbeat")
        await asyncio.sleep(settings.worker_poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Eureka workflow worker.")
    parser.add_argument("--once", action="store_true", help="Initialize and exit after one startup cycle.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(run(once=args.once))


if __name__ == "__main__":
    main()
